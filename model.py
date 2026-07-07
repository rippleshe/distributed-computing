"""
models.py — 数据库模型定义
============================

┌─────────────────────┐       ┌─────────────────────┐       ┌─────────────────────┐
│     main_task       │       │    task_slice        │       │   compute_node      │
│  (主任务表)          │       │  (分任务表)          │       │  (计算节点表)        │
├─────────────────────┤       ├─────────────────────┤       ├─────────────────────┤
│ id (PK)             │◄──┐   │ id (PK)             │       │ id (PK)             │
│ task_name           │   │   │ main_task_id (FK) ──┘   │   │ node_name           │
│ class_name          │   │   │ slice_index             │   │ ip_address          │
│ program_path        │   │   │ input_data              │   │ status              │
│ status              │   │   │ status                  │   │ current_task_id     │
│ total_slices        │   │   │ worker_id ──────────────│──►│ current_slice_id    │
│ completed_slices    │   │   │ result                  │   │ last_heartbeat      │
│ final_result        │   │   │ retry_count             │   │ version             │
│ version             │   │   │ version                 │   └─────────────────────┘
└─────────────────────┘   │   └─────────────────────────┘
                          │
                    一对多关系

技术说明：
    本文件使用 SQLAlchemy ORM 定义三张表的结构。
    SQLAlchemy 是 Python 最流行的 ORM 框架，类似 Java 的 JPA/Hibernate。
    ORM 允许我们用 Python 类来操作数据库表，而不需要直接写 SQL 语句。
"""

import datetime
from sqlalchemy import (
    create_engine,   # 数据库引擎工厂
    Column,          # 列定义
    Integer,         # 整数类型
    BigInteger,      # 大整数类型
    String,          # 字符串类型（VARCHAR）
    Text,            # 长文本类型（TEXT）
    DateTime         # 日期时间类型
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ============================================================
# ORM 基类
# ============================================================
# Base 是所有模型类的基类，类似 Java 的 @Entity 注解
# 所有继承 Base 的类都会被 SQLAlchemy 映射为数据库表
Base = declarative_base()


# ============================================================
# 1. 主任务表 —— 存储用户提交的全局任务信息
# ============================================================
class MainTask(Base):
    """
    主任务表。每提交一个任务创建一条记录。

    职责：
    - 记录任务的元信息（名称、程序文件路径、输入参数）
    - 跟踪任务的执行状态和进度
    - 存储最终计算结果

    状态流转：
        SPLITTING → IN_PROGRESS → COMPLETED
                    ↓
                  PAUSED ←→ IN_PROGRESS
                    ↓
                  FAILED

    字段说明：
    - status: 任务当前状态，使用字符串枚举
    - total_slices: split() 产生的分片总数
    - completed_slices: 已完成的分片数（由 check_and_reduce 更新）
    - version: 乐观锁版本号，用于 CAS 并发控制
    """
    __tablename__ = "main_task"

    # ---- 主键 ----
    # 自增整数主键，由数据库自动生成
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ---- 任务基本信息 ----
    # 任务名称，用户自定义（如"计算1到100的和"）
    task_name = Column(String(200), nullable=False)

    # 用户程序类名（如"SumCalculator"）
    # 系统从上传的 .py 文件中自动检测，无需用户手动输入
    class_name = Column(String(200), nullable=False)

    # 程序文件在 Master 服务器上的绝对路径
    # 格式：uploads/<uuid>/<filename>.py
    program_path = Column(String(500))

    # 程序原始文件名（用户上传时的文件名）
    # Worker 下载程序时使用此名称作为本地文件名
    program_file_name = Column(String(200))

    # ---- 输入数据 ----
    # 用户提交的输入参数（字符串形式）
    # 例如 "1,100" 表示计算 1 到 100 的和
    input_data = Column(Text)

    # 数据文件路径（可选）
    # 当用户上传数据文件时，此字段存储文件路径
    # 与 input_data 二选一
    data_file_path = Column(String(500))

    # ---- 执行状态 ----
    # 任务状态，取值范围：SPLITTING / IN_PROGRESS / PAUSED / COMPLETED / FAILED
    status = Column(String(20), nullable=False, default="SPLITTING")

    # ---- 进度统计 ----
    # 总分片数（split() 执行后写入）
    total_slices = Column(Integer, default=0)

    # 已完成分片数（每有分片完成时更新）
    # 进度百分比 = completed_slices / total_slices × 100%
    completed_slices = Column(Integer, default=0)

    # ---- 结果 ----
    # 最终汇聚结果（reduce() 的返回值）
    final_result = Column(Text)

    # 结果文件路径（结果同时保存到文件，便于下载）
    result_file_path = Column(String(500))

    # ---- 时间戳 ----
    # 任务创建时间（插入时自动设置）
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now)

    # 最后更新时间（插入和更新时自动设置）
    updated_at = Column(DateTime, nullable=False, default=datetime.datetime.now,
                        onupdate=datetime.datetime.now)

    # ---- 并发控制 ----
    # 乐观锁版本号
    # 每次更新时递增，CAS 操作通过检查版本号防止并发冲突
    version = Column(Integer, default=0)


# ============================================================
# 2. 分任务表 —— 记录每个数据块的处理进度（核心表）
# ============================================================
class TaskSlice(Base):
    """
    分任务表。主任务经 split() 分片后，每个分片对应一条记录。

    这是系统最核心的表，所有调度操作都围绕此表进行：
    - Worker 领取任务：查询 status='PENDING' 的记录，CAS 更新为 'ASSIGNED'
    - Worker 提交结果：CAS 更新 status 和 result
    - 容错调度器：检测超时的 ASSIGNED 记录，重置为 PENDING 或标记 FAILED

    状态流转：
        PENDING → ASSIGNED → COMPLETED
            ↑       ↓
            └─── PENDING（超时重置，retry_count < 3）
                    ↓
                  FAILED（retry_count >= 3）

    索引设计：
    - main_task_id 索引：加速按任务查询其所有分片
    - status 索引：加速查找 PENDING 分片（调度核心查询，高频）
    - worker_id 索引：加速按 Worker 查询其负责的分片
    """
    __tablename__ = "task_slice"

    # ---- 主键 ----
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ---- 隶属关系 ----
    # 所属主任务 ID（逻辑外键，未定义物理外键约束以提高性能）
    # INDEX：高频查询条件（按任务查分片）
    main_task_id = Column(Integer, nullable=False, index=True)

    # 分片在任务中的序号（从 0 开始）
    # 用于标识分片的顺序，reduce 时可按序号排列
    slice_index = Column(Integer, nullable=False)

    # ---- 输入输出 ----
    # 该分片的输入数据（由 split() 方法生成）
    # 例如 SumCalculator 的分片："1,25"
    input_data = Column(Text, nullable=False)

    # ---- 执行状态 ----
    # 分片状态，取值范围：PENDING / ASSIGNED / COMPLETED / FAILED
    # INDEX：调度器和 Worker 高频查询此字段
    status = Column(String(20), nullable=False, default="PENDING", index=True)

    # 分配给哪个 Worker（领取时写入，重置时清空）
    # INDEX：用于查询某个 Worker 负责的所有分片
    worker_id = Column(Integer, index=True)

    # ---- 计算结果 ----
    # 该分片的计算结果（compute() 方法的返回值）
    # 当 status='COMPLETED' 时此字段有值
    result = Column(Text)

    # ---- 时间戳 ----
    # 被 Worker 领取的时间（用于超时判定）
    assigned_at = Column(DateTime)

    # 计算完成的时间
    completed_at = Column(DateTime)

    # ---- 容错控制 ----
    # 已重试次数
    # 每次超时重置时 +1，达到 MAX_RETRY(3) 后标记为 FAILED
    retry_count = Column(Integer, default=0)

    # ---- 并发控制 ----
    # 乐观锁版本号
    # Worker 领取和提交结果时使用 CAS 检查此字段
    version = Column(Integer, default=0)


# ============================================================
# 3. 计算节点表 —— 维护在线 Worker 的健康状态
# ============================================================
class ComputeNode(Base):
    """
    计算节点表。Worker 启动时注册，通过心跳维持在线状态。

    生命周期：
    1. Worker 启动 → 注册 → status='ONLINE'
    2. Worker 领取任务 → 心跳上报 status='BUSY'
    3. Worker 空闲 → 心跳上报 status='ONLINE'
    4. Worker 崩溃 → 心跳停止 → 调度器标记 status='OFFLINE'

    字段说明：
    - current_task_id / current_slice_id：由心跳上报更新，用于监控 Worker 工作状态
    - last_heartbeat：容错判定的核心依据，调度器据此判断节点是否存活
    """
    __tablename__ = "compute_node"

    # ---- 主键 ----
    # 此 ID 即为 workerId，Worker 注册后获得，后续所有请求携带此 ID
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ---- 节点信息 ----
    # 节点名称（如 "worker-1"），由启动参数指定
    node_name = Column(String(100), nullable=False)

    # 节点 IP 地址（注册时自动获取本机 IP）
    ip_address = Column(String(50), nullable=False)

    # ---- 状态 ----
    # 节点状态，取值范围：ONLINE / BUSY / OFFLINE
    status = Column(String(20), nullable=False, default="ONLINE")

    # ---- 当前工作 ----
    # 当前正在处理的主任务 ID（心跳上报时更新，空闲时为 None）
    current_task_id = Column(Integer)

    # 当前正在处理的分片 ID（心跳上报时更新，空闲时为 None）
    current_slice_id = Column(Integer)

    # ---- 健康监测 ----
    # 最后一次心跳时间（容错判定的核心依据）
    # 调度器检查：NOW() - last_heartbeat > 30秒 → 标记为 OFFLINE
    last_heartbeat = Column(DateTime, nullable=False,
                           default=datetime.datetime.now)

    # Worker 注册时间（用于统计和调试）
    registered_at = Column(DateTime, nullable=False,
                           default=datetime.datetime.now)

    # ---- 并发控制 ----
    # 乐观锁版本号
    version = Column(Integer, default=0)


# ============================================================
# 数据库初始化函数
# ============================================================
def init_db(db_url: str = "sqlite:///data/distributed.db"):
    """
    初始化数据库连接，创建所有表（如果不存在）。



    参数:
        db_url: 数据库连接字符串
                默认使用 SQLite 文件数据库，路径为 data/distributed.db

    返回:
        (engine, Session) 元组
        - engine: SQLAlchemy 数据库引擎，管理底层连接
        - Session: 会话工厂，调用 Session() 创建数据库会话

    使用方式：
        engine, Session = init_db()
        session = Session()       # 创建会话
        # ... 执行查询 ...
        session.close()           # 关闭会话
    """
    import os
    # 确保 data 目录存在（SQLite 需要目录已存在才能创建数据库文件）
    os.makedirs("data", exist_ok=True)

    # 创建数据库引擎
    # echo=False 表示不打印 SQL 语句（调试时可设为 True）
    engine = create_engine(db_url, echo=False)

    # 自动创建所有继承 Base 的模型对应的表（如果不存在）
    # 这等价于执行 CREATE TABLE IF NOT EXISTS ...
    Base.metadata.create_all(engine)

    # 创建 Session 工厂
    # bind=engine 将 Session 绑定到指定的数据库引擎
    Session = sessionmaker(bind=engine)

    return engine, Session
