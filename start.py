"""
start.py — 一键启动脚本
=========================

┌─────────────────────────────────────────────────────────────────┐
│                    启动流程                                       │
│                                                                 │
│  ① 初始化数据库（创建表）                                         │
│  ② 启动 Master（端口 8080）                                      │
│     └── 自动启动容错调度器守护线程                                 │
│  ③ 启动 10 个 Worker（端口 8081~8090）                            │
│                                                                 │
│  所有进程在同一个终端运行，Ctrl+C 停止所有                         │
└─────────────────────────────────────────────────────────────────┘

使用方式：
    uv run python start.py

说明：
    容错调度器已集成到 Master 进程内（守护线程），
    Master 启动时自动运行，Master 停止时自动结束，
    无需单独管理调度器进程。
"""

import subprocess
import sys
import time
import os


def main():
    """
    主启动函数。

    启动顺序：
    1. 确保必要的目录存在（data/ 和 uploads/）
    2. 启动 Master（含调度器守护线程）
    3. 等待 Master 就绪
    4. 依次启动 10 个 Worker
    5. 等待所有进程（Ctrl+C 退出）
    """
    print("=" * 50)
    print("  分布式计算系统 - Python 版")
    print("=" * 50)
    print()

    # ---- 确保必要目录存在 ----
    # data/ 目录：SQLite 数据库文件存放位置
    os.makedirs("data", exist_ok=True)
    # uploads/ 目录：用户上传的程序文件和数据文件存放位置
    os.makedirs("uploads", exist_ok=True)

    # 记录所有启动的进程，便于统一管理
    processes = []

    try:
        # ============================================================
        # 步骤 1：启动 Master
        # ============================================================
        # Master 启动时会自动创建容错调度器的守护线程
        # 无需单独启动 scheduler.py
        print("[1/2] 启动 Master (端口 8080，含容错调度器)...")
        master = subprocess.Popen(
            [sys.executable, "master.py", "--port", "8080"],
            cwd=os.path.dirname(__file__) or "."
        )
        processes.append(("Master", master))

        # 等待 Master 启动完成（Flask 需要几秒初始化）
        time.sleep(3)

        # ============================================================
        # 步骤 2：启动 Worker
        # ============================================================
        print("[2/2] 启动 10 个 Worker...")
        for i in range(1, 11):
            port = 8080 + i
            worker = subprocess.Popen(
                [sys.executable, "worker.py",
                 "--port", str(port),
                 "--name", f"worker-{i}",
                 "--master", "http://localhost:8080"],
                cwd=os.path.dirname(__file__) or "."
            )
            processes.append((f"Worker-{i}", worker))

        # ============================================================
        # 启动完成，显示信息
        # ============================================================
        print()
        print("=" * 50)
        print("  系统已启动！")
        print()
        print("  Master:     http://localhost:8080")
        print("  Scheduler:  Master 内置守护线程")
        print("  Worker:     8081 ~ 8090 (10个)")
        print()
        print("  Ctrl+C 停止所有进程")
        print("=" * 50)

        # 等待所有进程结束
        # 正常情况下进程不会自行结束，除非被 Ctrl+C 终止
        for name, proc in processes:
            proc.wait()

    except KeyboardInterrupt:
        # 用户按 Ctrl+C，优雅停止所有进程
        print("\n正在停止所有进程...")
        for name, proc in processes:
            proc.terminate()
        for name, proc in processes:
            proc.wait(timeout=5)
        print("已停止")


if __name__ == "__main__":
    main()
