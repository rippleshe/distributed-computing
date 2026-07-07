"""
示例1：累加计算（SumCalculator）
==================================

┌─────────────────────────────────────────────────────────────────┐
│                    SumCalculator 数据流                           │
│                                                                 │
│  输入: "1,100"                                                  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ split(): 将 1~100 按每 25 个数切分为一个分片               │  │
│  │                                                          │  │
│  │  返回: ["1,25", "26,50", "51,75", "76,100"]              │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ compute(): 4个 Worker 并行计算每个分片的和                 │  │
│  │                                                          │  │
│  │  Worker-1: 1+2+...+25   = "325"                          │  │
│  │  Worker-2: 26+27+...+50 = "950"                          │  │
│  │  Worker-3: 51+52+...+75 = "1575"                         │  │
│  │  Worker-4: 76+77+...+100= "2200"                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ reduce(): 将所有分片的和相加                               │  │
│  │                                                          │  │
│  │  325 + 950 + 1575 + 2200 = "5050"                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  输出: "5050"                                                   │
└─────────────────────────────────────────────────────────────────┘

使用方式：
    1. 通过 Web UI 上传此文件
    2. 类名填写: SumCalculator
    3. 输入参数填写: 1,100
    4. 提交任务
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from typing import List
from computable import Computable


class SumCalculator(Computable):
    """
    累加计算器。

    计算 start 到 end 的所有整数之和。
    分片策略：按每 25 个数为一个分片。
    """

    SLICE_SIZE = 25  # 每个分片包含的数字个数

    def split(self, input_data: str) -> List[str]:
        """
        将计算范围切分成多个分片。

        输入格式: "start,end"（如 "1,100"）
        """
        start, end = map(int, input_data.split(","))
        slices = []

        while start <= end:
            slice_end = min(start + self.SLICE_SIZE - 1, end)
            slices.append(f"{start},{slice_end}")
            start = slice_end + 1

        return slices

    def compute(self, slice_data: str) -> str:
        """
        计算单个分片的和。
        """
        start, end = map(int, slice_data.split(","))
        return str(sum(range(start, end + 1)))

    def reduce(self, results: List[str]) -> str:
        """
        将所有分片的和相加。
        """
        return str(sum(map(int, results)))
