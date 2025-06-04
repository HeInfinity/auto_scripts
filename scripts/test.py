import asyncio
import aiomysql
import time
from datetime import datetime, timedelta
import sys
import os
import warnings
import functools
from typing import TypeVar, Callable, Any, Coroutine

# 忽略 VALUES 函数的警告
warnings.filterwarnings('ignore', message='.*VALUES function.*')

# 动态查找项目根目录并将 auto_scripts/modules 目录添加到 sys.path
project_root_name = 'Python'
current_dir = os.path.dirname(os.path.abspath(__file__))
while True:
    if os.path.basename(current_dir) == project_root_name:
        break
    new_dir = os.path.dirname(current_dir)
    if new_dir == current_dir:
        raise RuntimeError(f"无法找到包含目录 '{project_root_name}' 的项目根目录")
    current_dir = new_dir
modules_dir = os.path.join(current_dir, 'auto_scripts', 'modules')
sys.path.append(modules_dir)

from db_conn import DBManager