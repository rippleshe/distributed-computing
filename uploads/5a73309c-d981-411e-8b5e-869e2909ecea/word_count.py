"""
示例2：词频统计（WordCount）
==============================

┌─────────────────────────────────────────────────────────────────┐
│                    WordCount 数据流                               │
│                                                                 │
│  输入: 文本文件内容                                               │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ split(): 按每 10 行切分为一个分片                          │  │
│  │                                                          │  │
│  │  50行文本 → 5个分片（每片10行）                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ compute(): 5个 Worker 并行统计每个分片的词频               │  │
│  │                                                          │  │
│  │  每个 Worker 输出: "word1:count1\nword2:count2\n..."      │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ reduce(): 合并所有分片的词频统计                           │  │
│  │                                                          │  │
│  │  对相同单词的计数求和，按词频降序排列                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  输出: "hello:22\nworld:21\njava:20\n..."                       │
└─────────────────────────────────────────────────────────────────┘

使用方式：
    1. 通过 Web UI 上传此文件
    2. 类名填写: WordCount
    3. 上传数据文件（文本文件）
    4. 提交任务
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from typing import List
from collections import Counter
from computable import Computable


class WordCount(Computable):
    """
    词频统计器。

    统计文本中每个单词出现的次数。
    分片策略：按每 10 行为一个分片。
    """

    SLICE_LINES = 10  # 每个分片包含的行数

    def split(self, input_data: str) -> List[str]:
        """
        按行数将文本切分成多个分片。
        """
        lines = input_data.split("\n")
        slices = []

        for i in range(0, len(lines), self.SLICE_LINES):
            chunk = "\n".join(lines[i:i + self.SLICE_LINES])
            slices.append(chunk)

        return slices

    def compute(self, slice_data: str) -> str:
        """
        统计单个分片的词频。

        返回格式: "word1:count1\nword2:count2"
        """
        counter = Counter()
        words = slice_data.lower().split()

        for word in words:
            # 去除标点符号，只保留字母和数字
            clean = "".join(c for c in word if c.isalnum())
            if clean:
                counter[clean] += 1

        # 按词频降序排列
        sorted_words = counter.most_common()
        return "\n".join(f"{word}:{count}" for word, count in sorted_words)

    def reduce(self, results: List[str]) -> str:
        """
        合并所有分片的词频统计。

        对相同单词的计数求和，按词频降序排列。
        """
        total = Counter()

        for result in results:
            for line in result.split("\n"):
                if ":" in line:
                    word, count = line.rsplit(":", 1)
                    total[word] += int(count)

        # 按词频降序排列
        sorted_words = total.most_common()
        return "\n".join(f"{word}:{count}" for word, count in sorted_words)
