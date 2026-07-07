"""
master.py — 主控节点（Master）
===============================

┌─────────────────────────────────────────────────────────────────┐
│                      Master 节点架构                              │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Flask Web Server                       │  │
│  │                                                           │  │
│  │  POST /api/tasks              ← 用户提交任务               │  │
│  │  GET  /api/tasks              ← 获取任务列表               │  │
│  │  GET  /api/tasks/<id>         ← 查询任务状态               │  │
│  │  GET  /api/tasks/<id>/result  ← 获取结果                  │  │
│  │  GET  /api/tasks/<id>/program ← Worker下载程序             │  │
│  │                                                           │  │
│  │  POST /api/workers/register   ← Worker注册                │  │
│  │  POST /api/workers/<id>/hb    ← Worker心跳                │  │
│  │  GET  /api/workers/<id>/next  ← Worker请求任务             │  │
│  │  POST /api/tasks/<id>/slices/<id>/result ← 提交分片结果    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                          │                                      │
│                          ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    TaskService                             │  │
│  │  - submit_task(): 接收任务 → 自动检测类 → split() → 创建分片│  │
│  │  - get_next_slice(): 查找PENDING分片 → CAS更新为ASSIGNED   │  │
│  │  - submit_slice_result(): 更新分片结果                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                          │                                      │
│                          ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    SQLite 数据库                            │  │
│  │  main_task │ task_slice │ compute_node                     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              容错调度器（守护线程）                          │  │
│  │  - 每 15 秒巡检一次                                        │  │
│  │  - 检测离线 Worker                                         │  │
│  │  - 重置超时分片                                            │  │
│  │  - 传播任务失败状态                                        │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘

启动方式：
    uv run python master.py
    uv run python master.py --port 8080

说明：
    Master 启动时会自动创建容错调度器的守护线程，
    Master 停止时调度器线程随之停止，无需单独管理。
"""

import os
import sys
import uuid
import datetime
import argparse
import threading
from flask import Flask, request, jsonify, send_file
from models import MainTask, TaskSlice, ComputeNode, init_db
from scheduler import FaultToleranceScheduler
from utils import load_program, detect_computable_class


# ============================================================
# 初始化 Flask 应用和数据库
# ============================================================

# 创建 Flask 应用实例
# template_folder 指定 HTML 模板目录（用于 Web UI）
app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

# 初始化数据库连接
# 返回数据库引擎和 Session 工厂
engine, Session = init_db()

# 文件上传目录
# 用户上传的程序文件和数据文件保存在此目录下
# 每个任务使用 UUID 子目录隔离，避免文件名冲突
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# 容错调度器实例（在 __main__ 中启动为守护线程）
scheduler = None


# ============================================================
# Web UI 路由
# ============================================================

@app.route("/")
def index():
    """返回 Web UI 首页（单页面应用）"""
    from flask import render_template
    return render_template("index.html")


# ============================================================
# API 接口：任务相关
# ============================================================

@app.route("/api/tasks", methods=["POST"])
def submit_task():
    """
    提交新任务。

    接收 multipart/form-data：
    - taskName:    任务名称（必填）
    - programFile: .py 程序文件（必填，系统自动检测 Computable 子类）
    - dataFile:    数据文件（可选）
    - inputData:   输入参数（可选，与 dataFile 二选一）

    处理流程：
    1. 保存上传的程序文件和数据文件
    2. 创建主任务记录（status=SPLITTING）
    3. 自动检测程序中的 Computable 子类名
    4. 调用 split() 拆分数据
    5. 为每个分片创建 task_slice 记录
    6. 更新主任务状态为 IN_PROGRESS
    """
    session = Session()
    main_task = None
    try:
        # ---- 步骤 1：获取表单参数 ----
        task_name = request.form.get("taskName", "")
        input_data = request.form.get("inputData", "")

        # ---- 步骤 2：保存程序文件 ----
        program_file = request.files.get("programFile")
        if not program_file:
            return jsonify({"code": 400, "message": "缺少程序文件", "data": None}), 400

        # 保存原始文件名（仅用于记录，实际保存使用固定文件名）
        original_name = program_file.filename

        # 使用 UUID 创建独立的任务目录，避免不同任务的文件冲突
        task_dir = os.path.join(UPLOAD_DIR, str(uuid.uuid4()))
        os.makedirs(task_dir, exist_ok=True)

        # 将上传的程序文件统一重命名为固定名称，避免文件名安全问题
        program_path = os.path.join(task_dir, "user_program.py")
        program_file.save(program_path)

        # ---- 步骤 3：保存数据文件（可选） ----
        data_file_path = None
        data_file = request.files.get("dataFile")
        if data_file and data_file.filename:
            df_path = os.path.join(task_dir, data_file.filename)
            data_file.save(df_path)
            data_file_path = df_path

        # ---- 步骤 4：自动检测用户程序类名 ----
        # 从上传的 .py 文件中扫描 Computable 子类
        # 用户无需手动填写类名，系统自动识别
        try:
            detected_class_name = detect_computable_class(program_path)
        except RuntimeError as e:
            return jsonify({"code": 400, "message": str(e), "data": None}), 400

        # ---- 步骤 5：创建主任务记录 ----
        main_task = MainTask(
            task_name=task_name,
            class_name=detected_class_name,  # 自动检测到的类名
            program_path=program_path,
            program_file_name=original_name,
            input_data=input_data,
            data_file_path=data_file_path,
            status="SPLITTING"
        )
        session.add(main_task)
        session.commit()

        # ---- 步骤 6：加载用户程序并执行 split() ----
        program = load_program(program_path, detected_class_name)

        # 确定最终的输入数据
        # 优先使用数据文件内容，其次使用输入参数
        final_input = input_data
        if data_file_path:
            with open(data_file_path, "r", encoding="utf-8") as f:
                final_input = f.read()

        # 调用用户的 split() 方法拆分数据
        slices = program.split(final_input)

        # ---- 步骤 7：创建分片记录 ----
        for i, slice_data in enumerate(slices):
            ts = TaskSlice(
                main_task_id=main_task.id,
                slice_index=i,
                input_data=slice_data,
                status="PENDING"
            )
            session.add(ts)

        # ---- 步骤 8：更新主任务状态 ----
        main_task.total_slices = len(slices)
        main_task.status = "IN_PROGRESS"
        session.commit()

        return jsonify({"code": 200, "message": "任务已提交", "data": main_task.id})

    except Exception as e:
        # 任何异常都回滚事务
        session.rollback()
        # 如果主任务已创建，标记为 FAILED
        if main_task and main_task.id:
            try:
                main_task.status = "FAILED"
                session.commit()
            except:
                pass
        return jsonify({"code": 500, "message": f"任务提交失败: {e}", "data": None}), 500
    finally:
        # 无论成功与否都关闭 Session
        session.close()


@app.route("/api/tasks", methods=["GET"])
def get_all_tasks():
    """
    获取所有任务列表。

    返回任务的基本信息和进度，按创建时间倒序排列。
    进度百分比 = completedSlices / totalSlices × 100%
    """
    session = Session()
    try:
        # 查询所有任务，按 ID 倒序（最新的在前面）
        tasks = session.query(MainTask).order_by(MainTask.id.desc()).all()

        result = []
        for t in tasks:
            # 计算进度百分比
            progress = round(
                t.completed_slices / t.total_slices * 100, 2
            ) if t.total_slices > 0 else 0

            result.append({
                "taskId": t.id,
                "taskName": t.task_name,
                "taskType": t.class_name,
                "status": t.status,
                "totalSlices": t.total_slices,
                "completedSlices": t.completed_slices,
                "progress": progress,
                "createdAt": str(t.created_at),
                "updatedAt": str(t.updated_at)
            })
        return jsonify(result)
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>", methods=["GET"])
def get_task_status(task_id):
    """
    查询单个任务的状态和进度。

    返回任务的详细信息，包括状态、进度、创建时间等。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        progress = round(
            t.completed_slices / t.total_slices * 100, 2
        ) if t.total_slices > 0 else 0

        return jsonify({
            "taskId": t.id,
            "taskName": t.task_name,
            "taskType": t.class_name,
            "status": t.status,
            "totalSlices": t.total_slices,
            "completedSlices": t.completed_slices,
            "progress": progress,
            "createdAt": str(t.created_at),
            "updatedAt": str(t.updated_at)
        })
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>/result", methods=["GET"])
def get_task_result(task_id):
    """
    获取任务的最终计算结果。

    只有状态为 COMPLETED 的任务才有结果。
    其他状态返回错误提示。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        # 只有已完成的任务才有结果
        if t.status != "COMPLETED":
            return jsonify({
                "code": 400,
                "message": f"任务尚未完成，当前状态: {t.status}",
                "data": None
            })

        return jsonify({"code": 200, "message": "success", "data": t.final_result})
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>/result/download", methods=["GET"])
def download_result(task_id):
    """
    下载任务结果文件。

    只有状态为 COMPLETED 的任务才能下载结果。
    结果以文本文件形式返回。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        if t.status != "COMPLETED":
            return jsonify({"code": 400, "message": "任务尚未完成"}), 400

        # 如果有结果文件路径，直接返回文件
        if t.result_file_path and os.path.exists(t.result_file_path):
            return send_file(
                t.result_file_path,
                as_attachment=True,
                download_name=f"result_{task_id}.txt"
            )

        # 否则将 final_result 写入临时文件返回
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        tmp.write(t.final_result or "")
        tmp.close()
        return send_file(
            tmp.name,
            as_attachment=True,
            download_name=f"result_{task_id}.txt"
        )
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>/program", methods=["GET"])
def download_program(task_id):
    """
    Worker 下载程序文件。

    Worker 执行任务前需要先下载用户程序文件。
    此接口将程序文件以附件形式返回。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t or not t.program_path or not os.path.exists(t.program_path):
            return jsonify({"code": 404, "message": "程序文件不存在"}), 404

        # send_file 以文件流形式返回，as_attachment 触发浏览器下载
        return send_file(
            t.program_path,
            as_attachment=True,
            download_name=t.program_file_name or "program.py"
        )
    finally:
        session.close()


# ============================================================
# API 接口：任务控制（暂停/恢复/删除）
# ============================================================

@app.route("/api/tasks/<int:task_id>/pause", methods=["POST"])
def pause_task(task_id):
    """
    暂停任务。

    暂停后，Worker 不会再获取到该任务的分片（get_next_task 会过滤 PAUSED 任务）。
    已经在执行中的分片会继续完成，不会被中断。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        # 只有 IN_PROGRESS 状态的任务可以暂停
        if t.status not in ("IN_PROGRESS",):
            return jsonify({
                "code": 400,
                "message": f"当前状态 {t.status} 无法暂停"
            }), 400

        # 更新状态并递增版本号
        t.status = "PAUSED"
        t.version += 1
        session.commit()

        return jsonify({"code": 200, "message": "任务已暂停"})
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>/resume", methods=["POST"])
def resume_task(task_id):
    """
    恢复任务。

    恢复后，Worker 可以继续获取该任务的分片。
    之前已经完成的分片不受影响。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        # 只有 PAUSED 状态的任务可以恢复
        if t.status != "PAUSED":
            return jsonify({
                "code": 400,
                "message": f"当前状态 {t.status} 无法恢复"
            }), 400

        t.status = "IN_PROGRESS"
        t.version += 1
        session.commit()

        return jsonify({"code": 200, "message": "任务已恢复"})
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>", methods=["DELETE"])
def delete_task(task_id):
    """
    删除任务。

    删除任务及其所有分片记录和上传的文件。
    只允许删除已完成、失败或暂停的任务（不能删除正在执行的任务）。
    """
    session = Session()
    try:
        t = session.query(MainTask).get(task_id)
        if not t:
            return jsonify({"code": 404, "message": "任务不存在"}), 404

        # 不允许删除正在执行的任务
        if t.status in ("SPLITTING", "IN_PROGRESS"):
            return jsonify({"code": 400, "message": "请先暂停任务再删除"}), 400

        # 删除该任务的所有分片记录
        session.query(TaskSlice).filter(TaskSlice.main_task_id == task_id).delete()

        # 删除上传的文件目录
        if t.program_path and os.path.exists(os.path.dirname(t.program_path)):
            import shutil
            shutil.rmtree(os.path.dirname(t.program_path), ignore_errors=True)

        # 删除结果文件
        if t.result_file_path and os.path.exists(t.result_file_path):
            os.remove(t.result_file_path)

        # 删除主任务记录
        session.delete(t)
        session.commit()

        return jsonify({"code": 200, "message": "任务已删除"})
    finally:
        session.close()


# ============================================================
# API 接口：Worker 相关
# ============================================================

@app.route("/api/workers/register", methods=["POST"])
def register_worker():
    """
    Worker 注册。

    Worker 启动时调用此接口，将自己的信息注册到 Master。
    Master 创建 compute_node 记录并返回 workerId。
    """
    session = Session()
    try:
        data = request.get_json()

        # 创建计算节点记录
        node = ComputeNode(
            node_name=data["nodeName"],
            ip_address=data["ipAddress"],
            status="ONLINE",
            last_heartbeat=datetime.datetime.now()
        )
        session.add(node)
        session.commit()

        # 返回 Master 分配的节点 ID
        return jsonify({"code": 200, "message": "注册成功", "data": node.id})
    finally:
        session.close()


@app.route("/api/workers/heartbeat", methods=["POST"])
def worker_heartbeat():
    """
    处理 Worker 心跳。

    Worker 定时发送心跳，Master 更新节点的状态和最后心跳时间。
    容错调度器根据 last_heartbeat 判断节点是否存活。
    """
    session = Session()
    try:
        data = request.get_json()
        worker_id = data.get("workerId")

        if not worker_id:
            return jsonify({"code": 400, "message": "缺少 workerId"}), 400

        node = session.query(ComputeNode).get(worker_id)
        if not node:
            return jsonify({"code": 404, "message": "Worker不存在"}), 404

        # 更新节点状态
        node.status = data.get("status", "ONLINE")
        node.current_task_id = data.get("currentTaskId")
        node.current_slice_id = data.get("currentSliceId")
        node.last_heartbeat = datetime.datetime.now()
        node.version += 1

        session.commit()
        return jsonify({"code": 200, "message": "OK"})
    finally:
        session.close()


@app.route("/api/workers/next-task", methods=["GET"])
def get_next_task():
    """
    Worker 请求下一个待处理的分片。

    这是任务调度的核心接口。使用 CAS 乐观锁防止并发冲突：
    1. 查找一个 PENDING 状态的分片（排除已暂停任务的分片）
    2. CAS 更新：status 从 PENDING 改为 ASSIGNED
    3. 如果 CAS 失败（其他 Worker 抢到了），返回无可用任务

    CAS 机制说明：
    - SELECT 获取当前分片的 version 值
    - UPDATE 时 WHERE 条件包含 version，只有 version 未变才能成功
    - 如果其他 Worker 先一步更新了 version，UPDATE 影响行数为 0
    """
    session = Session()
    try:
        # 从查询参数获取 workerId
        worker_id = request.args.get("workerId", type=int)
        if not worker_id:
            return jsonify({"code": 400, "message": "缺少 workerId 参数"}), 400

        # ---- 步骤 1：查找一个 PENDING 分片 ----
        # JOIN main_task 确保只分配进行中任务的分片（排除 PAUSED 任务）
        slice_obj = session.query(TaskSlice).join(
            MainTask, TaskSlice.main_task_id == MainTask.id
        ).filter(
            TaskSlice.status == "PENDING",          # 只看待分配的分片
            MainTask.status == "IN_PROGRESS"         # 只分配进行中的任务
        ).first()

        if not slice_obj:
            # 没有可用的分片
            return jsonify({"code": 200, "message": "暂无可用任务", "data": None})

        # ---- 步骤 2：CAS 更新（PENDING → ASSIGNED） ----
        # 记录当前版本号
        old_version = slice_obj.version

        # 原子更新：只有 status 仍为 PENDING 且 version 未变时才成功
        affected = session.query(TaskSlice).filter(
            TaskSlice.id == slice_obj.id,
            TaskSlice.status == "PENDING",
            TaskSlice.version == old_version
        ).update({
            TaskSlice.status: "ASSIGNED",                # 状态改为已分配
            TaskSlice.worker_id: worker_id,              # 记录领取的 Worker
            TaskSlice.assigned_at: datetime.datetime.now(),  # 记录分配时间
            TaskSlice.version: old_version + 1           # 版本号递增
        })

        if affected == 0:
            # CAS 失败：其他 Worker 抢到了这个分片
            session.rollback()
            return jsonify({"code": 200, "message": "暂无可用任务", "data": None})

        session.commit()

        # ---- 步骤 3：获取主任务信息并返回 ----
        main_task = session.query(MainTask).get(slice_obj.main_task_id)

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "taskId": slice_obj.main_task_id,
                "sliceId": slice_obj.id,
                "sliceIndex": slice_obj.slice_index,
                "className": main_task.class_name,
                "programPath": main_task.program_path,
                "inputData": slice_obj.input_data
            }
        })
    finally:
        session.close()


@app.route("/api/tasks/<int:task_id>/slices/<int:slice_id>/result", methods=["POST"])
def submit_slice_result(task_id, slice_id):
    """
    Worker 提交分片结果。

    流程：
    1. 校验分片状态和 Worker 归属
    2. CAS 更新分片结果（防止重复提交）
    3. 如果分片完成，检查是否所有分片都完成
    4. 如果全部完成，触发 reduce() 汇聚操作
    """
    session = Session()
    try:
        data = request.get_json()
        worker_id = data["workerId"]
        result = data["result"]
        status = data["status"]

        # ---- 校验 ----
        slice_obj = session.query(TaskSlice).get(slice_id)
        if not slice_obj:
            return jsonify({"code": 404, "message": "分片不存在"}), 404

        # 检查分片是否分配给此 Worker
        if slice_obj.worker_id != worker_id:
            return jsonify({"code": 400, "message": "该分片未分配给此Worker"}), 400

        # 检查分片状态是否正确
        if slice_obj.status != "ASSIGNED":
            return jsonify({
                "code": 400,
                "message": f"分片状态不正确: {slice_obj.status}"
            }), 400

        # ---- CAS 更新分片结果 ----
        old_version = slice_obj.version
        affected = session.query(TaskSlice).filter(
            TaskSlice.id == slice_id,
            TaskSlice.version == old_version
        ).update({
            TaskSlice.result: result,
            TaskSlice.status: status,
            TaskSlice.completed_at: datetime.datetime.now(),
            TaskSlice.version: old_version + 1
        })

        if affected == 0:
            # CAS 失败，可能已超时被重置
            session.rollback()
            return jsonify({"code": 409, "message": "分片结果提交冲突"}), 409

        # ---- 更新主任务的已完成分片数 ----
        if status == "COMPLETED":
            session.query(MainTask).filter(
                MainTask.id == task_id
            ).update({
                MainTask.completed_slices: MainTask.completed_slices + 1,
                MainTask.version: MainTask.version + 1
            })

        session.commit()

        return jsonify({"code": 200, "message": "结果已提交"})
    finally:
        session.close()


# ============================================================
# API 接口：Worker 列表
# ============================================================

@app.route("/api/workers", methods=["GET"])
def get_workers():
    """
    获取所有在线 Worker 列表。

    返回非 OFFLINE 状态的节点信息，用于 Web UI 展示。
    """
    session = Session()
    try:
        nodes = session.query(ComputeNode).filter(
            ComputeNode.status != "OFFLINE"
        ).all()

        return jsonify([{
            "id": n.id,
            "nodeName": n.node_name,
            "ipAddress": n.ip_address,
            "status": n.status,
            "currentTaskId": n.current_task_id,
            "currentSliceId": n.current_slice_id,
            "lastHeartbeat": str(n.last_heartbeat),
            "registeredAt": str(n.registered_at),
            "version": n.version
        } for n in nodes])
    finally:
        session.close()


# ============================================================
# 启动入口
# ============================================================

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Master 主控节点")
    parser.add_argument("--port", type=int, default=8080,
                        help="监听端口（默认 8080）")
    args = parser.parse_args()

    print(f"Master 启动中... 端口: {args.port}")
    print(f"Web UI: http://localhost:{args.port}")

    # ---- 启动容错调度器守护线程 ----
    # 创建调度器实例
    scheduler = FaultToleranceScheduler()

    # 创建守护线程（daemon=True）
    # 守护线程的特点：当主线程（Flask）退出时，守护线程自动结束
    # 这样 Master 停止时，调度器也随之停止，无需单独管理
    scheduler_thread = threading.Thread(
        target=scheduler.run,    # 线程执行的函数
        name="FaultToleranceScheduler",  # 线程名称（便于调试）
        daemon=True              # 设为守护线程
    )
    scheduler_thread.start()
    print("[Master] 容错调度器已在后台启动")

    # ---- 启动 Flask Web 服务器 ----
    # debug=False：生产模式，不自动重载（避免重启时调度器线程重复创建）
    # host="0.0.0.0"：监听所有网络接口
    app.run(host="0.0.0.0", port=args.port, debug=False)
