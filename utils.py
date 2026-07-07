"""
utils.py — 工具函数模块
========================

抽取公共的动态加载逻辑，避免在 master.py、worker.py、scheduler.py 中重复代码。

包含：
- load_program():      动态加载用户程序，创建 Computable 实例
- detect_computable_class(): 从 .py 文件中自动检测 Computable 子类名
"""

import importlib.util
from computable import Computable


def load_program(program_path: str, class_name: str) -> Computable:
    """
    动态加载用户程序。

    Python 的 importlib 模块提供了动态加载 .py 文件的能力，
    类似 Java 的 URLClassLoader。

    参数:
        program_path: .py 文件路径
        class_name:   类名（如 "SumCalculator"）

    返回:
        Computable 实例

    异常:
        RuntimeError: 找不到指定的类或类不是 Computable 的子类
    """
    spec = importlib.util.spec_from_file_location("user_program", program_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if class_name:
        clazz = getattr(module, class_name, None)
        if clazz is not None:
            instance = clazz()
            if isinstance(instance, Computable):
                return instance

    computable_classes = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (isinstance(attr, type)
                and issubclass(attr, Computable)
                and attr is not Computable):
            computable_classes.append(attr)

    if len(computable_classes) == 1:
        return computable_classes[0]()
    elif len(computable_classes) > 1:
        names = [c.__name__ for c in computable_classes]
        raise RuntimeError(f"发现多个 Computable 类: {names}，请指定类名")
    else:
        raise RuntimeError("未找到 Computable 子类，请检查程序文件")


def detect_computable_class(program_path: str) -> str:
    """
    从用户上传的 .py 文件中自动检测 Computable 子类的类名。

    参数:
        program_path: .py 文件路径

    返回:
        检测到的类名（如 "SumCalculator"）

    异常:
        RuntimeError: 找不到 Computable 子类，或存在多个 Computable 子类
    """
    spec = importlib.util.spec_from_file_location("user_program", program_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    computable_classes = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (isinstance(attr, type)
                and issubclass(attr, Computable)
                and attr is not Computable):
            computable_classes.append(attr)

    if len(computable_classes) == 1:
        return computable_classes[0].__name__
    elif len(computable_classes) > 1:
        names = [c.__name__ for c in computable_classes]
        raise RuntimeError(
            f"发现多个 Computable 子类: {names}，请在文件中只保留一个 Computable 子类"
        )
    else:
        raise RuntimeError(
            "未找到 Computable 子类，请确保程序中有一个继承 Computable 的类"
        )