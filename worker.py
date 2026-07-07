"""
worker.py — 计算节点（Worker    ）
===============================

┌─────────────────────────────────────────────────────────────────┐
│                      Worker 节点架构                              │
│                                                                 │
│  Worker 是纯客户端，不启动 HTTP 服务器，不接收任何请求。            │
│  它只主动调用 Master 暴露的 RESTful API。                          │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    WorkerService                           │  │
│  │                                                           │  │
│  │  启动时:                                                   │  │
│  │    - 获取本机 IP                                           │  │
│  │    - 调用 Master 注册接口，获取 workerId                    │  │
│  │                                                           │  │
│  │  运行时（主循环，每 500ms 一轮）：                            │  │
│  │    - 每 3 秒调用 Master 心跳接口                            │  │
│  │    - 每 1 秒调用 Master 领取任务接口                         │  │
│  │                                                           │  │
│  │  领取到任务后（新线程异步执行）：                              │  │
│  │    - 调用 Master 下载程序接口                               │  │
│  │    - 本地动态加载用户类并执行 compute()                      │  │
│  │    - 调用 Master 提交结果接口                               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  通信模型：拉取式（Pull）                                         │
│    Worker 主动向 Master "拉取"任务，Master 不会主动 "推送"任务      │
│    这种设计使 Worker 可以部署在任意机器上，只需能访问 Master 即可    │
└─────────────────────────────────────────────────────────────────┘

启动方式：
    uv run python worker.py
    uv run python worker.py --port 8081 --name worker-1
    uv run python worker.py --port 8082 --name worker-2 --master http://localhost:8080

说明：
    Worker 不监听任何端口，--port 参数仅在注册时上报给 Master 用于标识。
    多个 Worker 可以部署在同一台机器或不同机器上。
"""

import os
import sys
import uuid
import socket
import threading
import argparse
import requests
import time

from utils import load_program


class WorkerService:
    """
    Worker 服务类，管理 Worker 的完整生命周期。

    Worker 是纯客户端角色，不启动任何 HTTP 服务器。
    所有通信都是 Worker 主动调用 Master 的 RESTful API。

    职责：
    1. 启动时调用 Master 注册接口，获取唯一的 workerId
    2. 定时调用心跳接口（每 3 秒），报告自身健康状态
    3. 定时调用领取任务接口（每 1 秒），获取待处理的分片
    4. 异步执行任务（新线程），调用 compute() 计算
    5. 调用提交结果接口，将结果返回给 Master
    6. 任务完成后清理本地程序缓存

    线程模型：
    - 主线程：心跳 + 任务轮询（循环调用 Master 接口）
    - 工作线程：执行 compute()（每个任务一个线程，daemon 模式）
    """

    def __init__(self, master_url: str, port: int, name: str):
        """
        初始化 Worker。

        参数:
            master_url: Master 节点地址（如 http://localhost:8080）
            port:       本机端口号
            name:       节点名称（如 worker-1）
        """
        # Master 地址，所有 HTTP 请求发送到此地址
        self.master_url = master_url

        # 本机端口（注册时上报给 Master）
        self.port = port

        # 节点名称（用于日志和 Master 显示）
        self.name = name

        # ---- 运行时状态 ----
        # Master 分配的节点 ID，注册成功后获得
        self.worker_id = None

        # 是否正在执行任务（防止同时领取多个任务）
        self.is_busy = False

        # 当前正在处理的任务 ID（心跳上报时使用）
        self.current_task_id = None

        # 当前正在处理的分片 ID（心跳上报时使用）
        self.current_slice_id = None

        # ---- 本地存储 ----
        # 程序文件本地缓存目录
        # 从 Master 下载的用户程序保存在此目录下
        self.program_dir = os.path.join(os.path.dirname(__file__), "worker-programs")
        os.makedirs(self.program_dir, exist_ok=True)

    def register(self):
        """
        向 Master 注册。

        Worker 启动时调用，向 Master 发送注册请求，获取 workerId。
        如果 Master 不可用（网络不通或 Master 未启动），会每隔 3 秒重试，
        直到注册成功为止。

        注册信息包含：
        - nodeName: 节点名称
        - ipAddress: 本机 IP 地址（自动获取）
        - port: 本机端口
        """
        # 自动获取本机 IP 地址
        ip = socket.gethostbyname(socket.gethostname())

        # 循环重试，直到注册成功
        while True:
            try:
                resp = requests.post(
                    f"{self.master_url}/api/workers/register",
                    json={
                        "nodeName": self.name,
                        "ipAddress": ip
                    },
                    timeout=5  # 5 秒超时
                )
                data = resp.json()

                if data.get("code") == 200:
                    # 注册成功，保存 Master 分配的 workerId
                    self.worker_id = data["data"]
                    print(f"[{self.name}] 注册成功，workerId={self.worker_id}")
                    return
            except Exception as e:
                # 注册失败（Master 不可达），等待重试
                print(f"[{self.name}] 注册失败: {e}，3秒后重试...")
                time.sleep(3)

    def send_heartbeat(self):
        """
        发送心跳。

        每 3 秒调用一次，向 Master 报告自身状态。
        心跳内容包括：
        - status: ONLINE（空闲）或 BUSY（执行中）
        - currentTaskId: 当前任务 ID（可选）
        - currentSliceId: 当前分片 ID（可选）

        如果发送失败（Master 不可达），不做特殊处理，
        下次循环会继续尝试。Master 端有容错调度器会检测心跳超时。
        """
        # 未注册时不发送心跳
        if self.worker_id is None:
            self.register()
            return

        try:
            resp = requests.post(
                f"{self.master_url}/api/workers/heartbeat",
                json={
                    "workerId": self.worker_id,
                    # 根据是否在执行任务报告不同状态
                    "status": "BUSY" if self.is_busy else "ONLINE",
                    "currentTaskId": self.current_task_id,
                    "currentSliceId": self.current_slice_id
                },
                timeout=5
            )

            if resp.json().get("code") != 200:
                print(f"[{self.name}] 心跳失败")
        except Exception:
            # 心跳发送失败，静默处理
            # 容错调度器会在 30 秒后检测到心跳超时
            print(f"[{self.name}] 心跳发送异常")

    def request_task(self):
        """
        请求下一个待处理的分片。

        每 1 秒调用一次。如果当前正在执行任务（is_busy=True），则跳过。
        领取成功后，在新线程中异步执行任务，不阻塞主循环。

        领取流程：
        1. GET /api/workers/{id}/next-task
        2. Master 查找 PENDING 分片，CAS 更新为 ASSIGNED
        3. 返回分片信息（taskId, sliceId, inputData 等）
        4. Worker 启动新线程执行 execute_task()
        """
        # 正在执行任务或未注册时，不请求新任务
        if self.is_busy or self.worker_id is None:
            return

        try:
            resp = requests.get(
                f"{self.master_url}/api/workers/next-task",
                params={"workerId": self.worker_id},
                timeout=5
            )
            data = resp.json()

            # 检查是否领取到任务
            if data.get("code") == 200 and data.get("data"):
                slice_info = data["data"]
                print(f"[{self.name}] 收到任务: taskId={slice_info['taskId']}, "
                      f"sliceId={slice_info['sliceId']}")

                # 在新线程中异步执行任务
                # daemon=True 表示主进程退出时此线程也会退出
                threading.Thread(
                    target=self.execute_task,
                    args=(slice_info,),
                    daemon=True
                ).start()
        except Exception:
            # 请求失败（网络问题），下次循环继续
            pass

    def execute_task(self, slice_info: dict):
        """
        执行任务分片（在工作线程中运行）。

        完整流程：
        1. 从 Master 下载用户程序文件到本地
        2. 用 importlib 动态加载用户类（继承 Computable 的类）
    3. 调用 compute(slice_data) 执行计算
    4. 将结果提交给 Master


        参数:
            slice_info: 分片信息字典，包含以下字段：
                - taskId:     主任务 ID
                - sliceId:    分片 ID
                - sliceIndex: 分片序号
                - className:  用户程序类名
                - programPath: 程序文件路径
                - inputData:  分片输入数据
        """
        # 标记为忙碌状态（防止主循环再领取新任务）
        self.is_busy = True
        self.current_task_id = slice_info["taskId"]
        self.current_slice_id = slice_info["sliceId"]

        try:
            # ---- 步骤 1：下载程序文件 ----
            # 从 Master 下载用户上传的 .py 文件到本地
            local_path = self.download_program(
                slice_info["taskId"],
                slice_info["programPath"]
            )
            if not local_path:
                # 下载失败，提交 FAILED 状态
                self.submit_result(slice_info, None, "FAILED")
                return

            # ---- 步骤 2：动态加载用户类 ----
            # 使用 importlib 从 .py 文件中加载继承 Computable 的类
            program = load_program(local_path, slice_info["className"])

            # ---- 步骤 3：执行计算 ----
            # 调用用户实现的 compute() 方法
            result = program.compute(slice_info["inputData"])

            # ---- 步骤 4：提交结果 ----
            self.submit_result(slice_info, result, "COMPLETED")
            print(f"[{self.name}] 任务完成: sliceId={slice_info['sliceId']}")

        except Exception as e:
            # 计算过程出错，提交 FAILED 状态
            print(f"[{self.name}] 任务执行失败: {e}")
            self.submit_result(slice_info, None, "FAILED")

        finally:
            # ---- 恢复状态 ----
            # 无论成功与否，都恢复为空闲状态
            self.is_busy = False
            self.current_task_id = None
            self.current_slice_id = None

    def download_program(self, task_id: int, program_path: str) -> str:
        """
        从 Master 下载程序文件到本地。

        Worker 每次执行任务时都需要下载程序文件，
        因为不同任务的用户程序可能不同。

        参数:
            task_id:      任务 ID
            program_path: Master 上的文件路径（保留兼容，实际使用固定文件名）

        返回:
            本地文件路径，失败返回 None
        """
        try:
            # 从 Master 下载程序文件
            resp = requests.get(
                f"{self.master_url}/api/tasks/{task_id}/program",
                timeout=30  # 文件下载给较长超时
            )

            if resp.status_code != 200:
                return None

            # 使用任务 ID 创建子目录，避免不同任务的文件冲突
            task_dir = os.path.join(self.program_dir, str(task_id))
            os.makedirs(task_dir, exist_ok=True)

            # 使用固定文件名保存，与 Master 端保持一致
            local_path = os.path.join(task_dir, "user_program.py")
            with open(local_path, "wb") as f:
                f.write(resp.content)

            return local_path
        except Exception as e:
            print(f"[{self.name}] 下载程序失败: {e}")
            return None

    def submit_result(self, slice_info: dict, result: str, status: str):
        """
        向 Master 提交分片的计算结果。

        参数:
            slice_info: 分片信息（包含 taskId 和 sliceId）
            result:     计算结果（字符串），失败时为 None
            status:     结果状态（"COMPLETED" 或 "FAILED"）
        """
        try:
            requests.post(
                f"{self.master_url}/api/tasks/{slice_info['taskId']}"
                f"/slices/{slice_info['sliceId']}/result",
                json={
                    "workerId": self.worker_id,
                    "result": result,
                    "status": status
                },
                timeout=10
            )
        except Exception as e:
            # 提交失败，Master 端的容错调度器会检测到超时并重置分片
            print(f"[{self.name}] 提交结果失败: {e}")

    def run(self):
        """
        启动 Worker 主循环。

        主循环逻辑（每 500ms 一轮）：
        - 每 3 轮（1.5 秒）发送一次心跳
        - 每 2 轮（1 秒）请求一次任务

        使用简单的 sleep 循环代替定时器框架，保持代码精简。
        """
        # 注册到 Master
        self.register()

        print(f"[{self.name}] Worker 已启动，开始轮询任务...")

        # 心跳计数器（用于控制心跳频率）
        heartbeat_counter = 0

        while True:
            try:
                # 每 3 轮发送一次心跳（约 1.5 秒）
                if heartbeat_counter % 3 == 0:
                    self.send_heartbeat()

                # 每 2 轮请求一次任务（约 1 秒）
                self.request_task()

                heartbeat_counter += 1
                # 500ms 循环间隔，兼顾响应速度和 CPU 占用
                time.sleep(0.5)

            except KeyboardInterrupt:
                # 用户按 Ctrl+C，优雅退出
                print(f"\n[{self.name}] Worker 已停止")
                break
            except Exception as e:
                # 捕获异常继续运行，不因单次错误退出
                print(f"[{self.name}] 异常: {e}")
                time.sleep(1)


# ============================================================
# 启动入口
# ============================================================
if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Worker 计算节点")
    parser.add_argument("--master", default="http://localhost:8080",
                        help="Master 节点地址（默认 http://localhost:8080）")
    parser.add_argument("--port", type=int, default=8081,
                        help="本机端口（默认 8081）")
    parser.add_argument("--name", default="worker-1",
                        help="节点名称（默认 worker-1）")
    args = parser.parse_args()

    # 创建并启动 Worker
    worker = WorkerService(args.master, args.port, args.name)
    worker.run()
