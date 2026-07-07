"""
scheduler.py — 容错调度器
==========================

┌─────────────────────────────────────────────────────────────────┐
│                    调度器巡检流程                                  │
│                                                                 │
│  每 15 秒执行一次巡检                                             │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ① 检查 Worker 心跳                                        │  │
│  │   timeout = NOW() - 30秒                                  │  │
│  │   将 last_heartbeat < timeout 的节点标记为 OFFLINE           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ② 检查分片超时                                             │  │
│  │   timeout = NOW() - 60秒                                  │  │
│  │   retry_count < 3  → 重置为 PENDING，retry_count++         │  │
│  │   retry_count >= 3 → 标记为 FAILED                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ③ 检查任务失败传播                                         │  │
│  │   若任务的所有分片都已终态（COMPLETED/FAILED）                │  │
│  │   且存在 FAILED 分片                                       │  │
│  │   则标记主任务为 FAILED                                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ④ 检查已完成任务 → 执行 reduce                             │  │
│  │   扫描 status=IN_PROGRESS 的主任务                         │  │
│  │   若 completed_slices == total_slices                      │  │
│  │   则调用 reduce() 汇聚结果                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘

运行方式：
    本模块不独立运行，由 master.py 导入后作为守护线程启动。
    Master 启动时自动创建 Scheduler 线程，Master 停止时 Scheduler 随之停止。
"""

import os
import datetime
import time
from models import MainTask, TaskSlice, ComputeNode, init_db
from utils import load_program


class FaultToleranceScheduler:
    """
    容错调度器。

    定时巡检数据库，处理超时和故障：
    1. 标记离线 Worker（心跳超时 30 秒）
    2. 重置超时分片（分配超时 60 秒，最大重试 3 次）
    3. 传播任务失败状态（所有分片终态且存在失败 → 主任务失败）
    4. 检查已完成任务并执行 reduce（所有分片完成 → 汇聚结果）

    支持两种运行模式：
    - 作为守护线程在 Master 进程内运行（推荐）

    线程安全说明：
    - 每次 inspect() 创建独立的数据库 Session，不会与其他线程共享连接
    - SQLite 自身支持多读单写，SQLAlchemy 会处理写锁等待
    - 所有状态更新使用批量 UPDATE 语句，减少锁持有时间
    """

    # ============================================================
    # 超时参数配置
    # ============================================================

    # Worker 心跳超时阈值（秒）
    # 若一个 Worker 在 30 秒内没有发送心跳，则判定为离线
    HEARTBEAT_TIMEOUT = 30

    # 分片分配超时阈值（秒）
    # 若一个分片被 Worker 领取后 60 秒仍未完成，则判定为超时（可能 Worker 崩溃）
    SLICE_TIMEOUT = 60

    # 分片最大重试次数
    # 超时分片会被重置为 PENDING 重新分配，但最多重试 3 次
    # 超过 3 次仍失败的分片将被标记为 FAILED，不再重试
    MAX_RETRY = 3

    # 巡检间隔（秒）
    # 调度器每 15 秒执行一次完整巡检
    INSPECT_INTERVAL = 15

    def __init__(self):
        """
        初始化调度器。

        创建独立的数据库引擎和 Session 工厂。
        即使作为线程运行，也使用独立的引擎，避免与 Master 共享连接池。
        """
        # 初始化数据库连接（使用与 Master 相同的 SQLite 文件）
        self.engine, self.Session = init_db()

        # 线程控制标志
        # 当 _running=False 时，run() 循环会退出
        self._running = False

    def inspect(self):
        """
        执行一次完整的巡检。

        依次执行三个检查步骤，使用同一个数据库事务。
        如果任何步骤出现异常，整个事务回滚，不影响数据一致性。

        步骤：
            1. check_offline_workers() — 检测离线 Worker
            2. check_timeout_slices()  — 重置超时分片
            3. check_task_failure()    — 传播任务失败状态
            4. check_and_reduce_tasks()— 检查已完成任务并执行 reduce
        """
        # 每次巡检创建独立的 Session，巡检结束后关闭
        # 这样即使在多线程环境下也不会出现连接泄漏
        session = self.Session()
        try:
            # 步骤 1：检查 Worker 心跳
            self.check_offline_workers(session)

            # 步骤 2：检查分片超时
            self.check_timeout_slices(session)

            # 步骤 3：检查任务失败传播
            self.check_task_failure(session)

            # 步骤 4：检查已完成任务并执行 reduce
            self.check_and_reduce_tasks(session)

            # 所有检查通过后统一提交
            session.commit()

        except Exception as e:
            # 任何异常都回滚事务，保证数据一致性
            session.rollback()
            print(f"[Scheduler] 巡检异常: {e}")
        finally:
            # 无论成功与否都关闭 Session，释放连接
            session.close()

    def check_offline_workers(self, session):
        """
        检查离线的 Worker。

        扫描 compute_node 表，将超过 HEARTBEAT_TIMEOUT 秒没有发送心跳的
        节点标记为 OFFLINE。

        判定算法：
            elapsed = NOW() - last_heartbeat
            如果 elapsed > HEARTBEAT_TIMEOUT，则判定为离线

        SQL 逻辑：
            UPDATE compute_node
            SET status = 'OFFLINE', version = version + 1
            WHERE status != 'OFFLINE'
              AND last_heartbeat < NOW() - 30秒

        参数:
            session: SQLAlchemy 数据库会话
        """
        # 计算超时阈值时间点
        timeout = datetime.datetime.now() - datetime.timedelta(
            seconds=self.HEARTBEAT_TIMEOUT
        )

        # 批量更新：将超时节点标记为 OFFLINE
        # 使用批量 UPDATE 而非逐条查询+更新，减少数据库锁持有时间
        affected = session.query(ComputeNode).filter(
            # 只处理非 OFFLINE 状态的节点（避免重复标记）
            ComputeNode.status != "OFFLINE",
            # 心跳时间早于超时阈值
            ComputeNode.last_heartbeat < timeout
        ).update({
            ComputeNode.status: "OFFLINE",
            # 版本号递增，用于乐观锁
            ComputeNode.version: ComputeNode.version + 1
        })

        # 有节点被标记为离线时输出日志
        if affected > 0:
            print(f"[Scheduler] 发现 {affected} 个离线 Worker")

    def check_timeout_slices(self, session):
        """
        检查超时的分片（僵尸任务检测）。

        扫描 task_slice 表，找出状态为 ASSIGNED 且分配时间超过
        SLICE_TIMEOUT 秒的分片，根据重试次数决定处理方式：
        - retry_count < MAX_RETRY：重置为 PENDING，允许其他 Worker 重新领取
        - retry_count >= MAX_RETRY：标记为 FAILED，不再重试

        判定算法：
            elapsed = NOW() - assigned_at
            如果 elapsed > SLICE_TIMEOUT 且 status = ASSIGNED，则判定为超时

        SQL 逻辑：
            -- 重置未超限的超时分片
            UPDATE task_slice
            SET status = 'PENDING', worker_id = NULL,
                retry_count = retry_count + 1, version = version + 1
            WHERE status = 'ASSIGNED'
              AND assigned_at < NOW() - 60秒
              AND retry_count < 3

            -- 标记超限的超时分片为 FAILED
            UPDATE task_slice
            SET status = 'FAILED', version = version + 1
            WHERE status = 'ASSIGNED'
              AND assigned_at < NOW() - 60秒
              AND retry_count >= 3

        参数:
            session: SQLAlchemy 数据库会话
        """
        # 计算超时阈值时间点
        timeout = datetime.datetime.now() - datetime.timedelta(
            seconds=self.SLICE_TIMEOUT
        )

        # ---- 第一步：重置超时但未超过重试次数的分片 ----
        # 这些分片的 Worker 可能已经崩溃，但还有重试机会
        reset_count = session.query(TaskSlice).filter(
            TaskSlice.status == "ASSIGNED",          # 状态为已分配
            TaskSlice.assigned_at < timeout,          # 分配时间超过阈值
            TaskSlice.retry_count < self.MAX_RETRY    # 重试次数未超限
        ).update({
            TaskSlice.status: "PENDING",              # 重置为待分配
            TaskSlice.worker_id: None,                # 清除 Worker 绑定
            TaskSlice.retry_count: TaskSlice.retry_count + 1,  # 重试次数 +1
            TaskSlice.version: TaskSlice.version + 1  # 版本号递增
        })

        if reset_count > 0:
            print(f"[Scheduler] 重置 {reset_count} 个超时分片")

        # ---- 第二步：标记超过最大重试次数的分片为 FAILED ----
        # 这些分片已经重试了 MAX_RETRY 次仍然失败，放弃处理
        failed_count = session.query(TaskSlice).filter(
            TaskSlice.status == "ASSIGNED",           # 状态为已分配
            TaskSlice.assigned_at < timeout,           # 分配时间超过阈值
            TaskSlice.retry_count >= self.MAX_RETRY    # 重试次数已达上限
        ).update({
            TaskSlice.status: "FAILED",               # 标记为失败
            TaskSlice.version: TaskSlice.version + 1  # 版本号递增
        })

        if failed_count > 0:
            print(f"[Scheduler] 标记 {failed_count} 个分片为 FAILED")

    def check_task_failure(self, session):
        """
        检查并标记失败的主任务（失败传播）。

        当一个任务的所有分片都已到达终态（COMPLETED 或 FAILED），
        且其中存在 FAILED 分片时，将主任务标记为 FAILED。

        这是一个"向上聚合"的逻辑：分片的失败状态传播到主任务级别。

        判定条件：
            1. 该任务存在 FAILED 状态的分片
            2. 该任务不存在 PENDING 或 ASSIGNED 状态的分片（即全部终态）

        参数:
            session: SQLAlchemy 数据库会话
        """
        # 第一步：找出所有包含 FAILED 分片的任务 ID
        # 使用 DISTINCT 去重，因为一个任务可能有多个 FAILED 分片
        failed_task_ids = session.query(TaskSlice.main_task_id).filter(
            TaskSlice.status == "FAILED"
        ).distinct().all()

        # 第二步：逐个检查这些任务是否所有分片都已到达终态
        for (task_id,) in failed_task_ids:
            # 统计该任务中尚未到达终态的分片数量
            # PENDING 和 ASSIGNED 都是非终态，说明还有分片在处理中或等待处理
            pending_count = session.query(TaskSlice).filter(
                TaskSlice.main_task_id == task_id,
                TaskSlice.status.in_(["PENDING", "ASSIGNED"])
            ).count()

            # 如果没有未完成的分片，说明所有分片都已终态
            if pending_count == 0:
                # 查询主任务记录
                main_task = session.query(MainTask).get(task_id)

                # 只处理非 FAILED 状态的主任务（避免重复标记）
                if main_task and main_task.status != "FAILED":
                    main_task.status = "FAILED"
                    main_task.version += 1
                    print(f"[Scheduler] 主任务 {task_id} 标记为 FAILED")

    def check_and_reduce_tasks(self, session):
        """
        检查已完成的任务并执行 reduce 汇聚。

        扫描所有 status='IN_PROGRESS' 的主任务，检查其已完成分片数
        是否等于总分片数。如果相等，说明所有分片都已完成，触发 reduce()。

        这个逻辑从 master.py 的 submit_slice_result 中移到调度器线程，
        由调度器统一扫描和触发，职责更清晰。

        参数:
            session: SQLAlchemy 数据库会话
        """
        # 查找所有进行中的任务
        in_progress_tasks = session.query(MainTask).filter(
            MainTask.status == "IN_PROGRESS"
        ).all()

        for main_task in in_progress_tasks:
            # 统计该任务已完成的分片数
            completed_count = session.query(TaskSlice).filter(
                TaskSlice.main_task_id == main_task.id,
                TaskSlice.status == "COMPLETED"
            ).count()

            # 检查是否所有分片都已完成
            if completed_count < main_task.total_slices:
                continue

            # 所有分片完成，执行 reduce
            try:
                program = load_program(main_task.program_path, main_task.class_name)

                # 收集所有分片的计算结果
                slices = session.query(TaskSlice).filter(
                    TaskSlice.main_task_id == main_task.id
                ).all()
                results = [s.result for s in slices if s.result is not None]

                # 调用用户的 reduce() 方法
                final_result = program.reduce(results)

                # 保存结果到文件
                result_path = os.path.join("uploads", f"result_{main_task.id}.txt")
                with open(result_path, "w", encoding="utf-8") as f:
                    f.write(final_result)

                # 更新主任务为已完成
                session.query(MainTask).filter(
                    MainTask.id == main_task.id
                ).update({
                    MainTask.final_result: final_result,
                    MainTask.result_file_path: result_path,
                    MainTask.status: "COMPLETED",
                    MainTask.completed_slices: main_task.total_slices,
                    MainTask.version: MainTask.version + 1
                })
                session.commit()

                print(f"[Scheduler] 任务 {main_task.id} 已完成，结果: {final_result}")

            except Exception as e:
                # reduce 执行失败，标记任务为 FAILED
                session.query(MainTask).filter(
                    MainTask.id == main_task.id
                ).update({
                    MainTask.final_result: f"REDUCE_FAILED: {e}",
                    MainTask.status: "FAILED"
                })
                session.commit()
                print(f"[Scheduler] 任务 {main_task.id} reduce 失败: {e}")

    def run(self):
        """
        启动调度器主循环（作为线程运行）。

        每 INSPECT_INTERVAL 秒执行一次巡检，直到 _running 被设为 False。
        由 Master 在 __main__ 中通过 threading.Thread(target=scheduler.run) 启动。
        """
        print(f"[Scheduler] 容错调度器已启动，每 {self.INSPECT_INTERVAL} 秒巡检一次")

        # 设置运行标志
        self._running = True

        while self._running:
            try:
                # 执行一次巡检
                self.inspect()
                # 等待下一次巡检
                # 使用短间隔循环而非长 sleep，以便能及时响应 stop() 信号
                for _ in range(self.INSPECT_INTERVAL * 2):
                    if not self._running:
                        break
                    time.sleep(0.5)
            except Exception as e:
                # 捕获异常继续运行，不因单次巡检失败而退出
                print(f"[Scheduler] 异常: {e}")
                time.sleep(self.INSPECT_INTERVAL)

        print("[Scheduler] 调度器已退出")

    def stop(self):
        """
        停止调度器。

        将 _running 标志设为 False，主循环会在下一次检查时退出。
        Master 关闭时守护线程会自动结束，此方法供需要显式停止时使用。
        """
        self._running = False
        print("[Scheduler] 收到停止信号")
