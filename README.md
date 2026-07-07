# 简易分布式计算系统

仿照 Hadoop MapReduce 原理实现的简化版分布式计算系统。用户只需实现 `split()` / `compute()` / `reduce()` 三个方法，框架自动完成任务分发、并行计算和结果汇聚。

## 技术栈

- Python 3.10+
- Flask（Master HTTP 服务）
- SQLAlchemy + SQLite（数据持久化）
- requests（Worker HTTP 客户端）

## 系统架构

采用 Master-Worker 架构，通过 HTTP 通信：

- **Master**：启动 HTTP 服务器，暴露 RESTful API，负责接收任务、拆分数据、分发分片、汇聚结果，内置容错调度器守护线程
- **Worker**：纯客户端角色，不接收任何请求，只主动调用 Master 的接口领取任务和提交结果，可部署在任意机器上
- **Scheduler**：作为 Master 内置的守护线程运行，每 15 秒巡检一次，处理节点离线、任务超时、结果汇聚

## 快速开始

### 安装依赖

```bash
uv sync
```

### 一键启动

```bash
uv run python start.py
```

自动启动 Master（端口 8080）+ 10 个 Worker + 容错调度器。

### 分别启动

```bash
# 终端 1：启动 Master
uv run python master.py --port 8080

# 终端 2~N：启动 Worker
uv run python worker.py --port 8081 --name worker-1
uv run python worker.py --port 8082 --name worker-2
```

启动后访问 http://localhost:8080 进入 Web UI。

## 编写用户程序

继承 `Computable` 基类，实现三个方法：

```python
from computable import Computable

class SumCalculator(Computable):
    def split(self, input_data):
        """将输入数据切分为多个分片（Master 端执行）"""
        start, end = map(int, input_data.split(","))
        return [f"{start},{start+24}", f"{start+25},{end}", ...]

    def compute(self, slice_data):
        """处理单个分片（Worker 端并行执行）"""
        start, end = map(int, slice_data.split(","))
        return str(sum(range(start, end + 1)))

    def reduce(self, results):
        """汇聚所有分片结果（Master 端执行）"""
        return str(sum(map(int, results)))
```

将 `.py` 文件通过 Web UI 上传，系统自动检测 `Computable` 子类并执行。

## API 接口

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/tasks` | 提交任务（multipart/form-data） |
| GET | `/api/tasks` | 获取任务列表 |
| GET | `/api/tasks/<id>` | 查询任务状态 |
| GET | `/api/tasks/<id>/result` | 获取任务结果 |
| POST | `/api/tasks/<id>/pause` | 暂停任务 |
| POST | `/api/tasks/<id>/resume` | 恢复任务 |
| DELETE | `/api/tasks/<id>` | 删除任务 |

### Worker 通信

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/workers/register` | Worker 注册 |
| POST | `/api/workers/<id>/heartbeat` | 心跳上报 |
| GET | `/api/workers/<id>/next-task` | 领取分片 |
| GET | `/api/tasks/<id>/program` | 下载程序文件 |
| POST | `/api/tasks/<id>/slices/<id>/result` | 提交分片结果 |

## 数据库表

| 表名 | 说明 |
|------|------|
| `main_task` | 主任务表，存储全局任务信息 |
| `task_slice` | 分任务表，记录每个分片的处理进度（核心表） |
| `compute_node` | 计算节点表，维护 Worker 健康状态 |

三张表均包含 `version` 字段用于 CAS 乐观锁并发控制。

## 容错机制

调度器每 15 秒巡检一次：

1. **检测离线 Worker**：心跳超过 30 秒未更新 → 标记 OFFLINE
2. **重置超时分片**：分片被领取超过 60 秒未完成 → 重置为 PENDING（最多重试 3 次）
3. **传播任务失败**：所有分片终态且存在失败 → 主任务标记 FAILED
4. **执行 reduce**：所有分片完成 → 调用用户 reduce() 汇聚结果

## 项目结构

```
├── master.py              主控节点（Flask HTTP 服务 + 容错调度器线程）
├── worker.py              计算节点（纯客户端，调用 Master 接口）
├── scheduler.py           容错调度器（被 Master 导入为守护线程）
├── computable.py          用户程序接口定义（Computable 基类）
├── models.py              数据库模型（SQLAlchemy ORM）
├── start.py               一键启动脚本
├── pyproject.toml         项目配置
├── templates/
│   └── index.html         Web UI
├── examples/
│   ├── sum_calculator.py  示例：累加计算
│   ├── word_count.py      示例：词频统计
│   └── sample_text.txt    示例数据
└── 设计说明文档.md          详细设计文档
```

## License

MIT
