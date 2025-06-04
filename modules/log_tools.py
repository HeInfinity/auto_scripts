import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logger(script_name, parent_logger=None):
    """
    设置日志记录器
    :param script_name: 脚本名称, 用于确定日志文件名
    :param parent_logger: 父日志记录器, 如果有的话就使用父日志记录器
    :return: 配置好的日志记录器
    """
    # 如果已经有父日志记录器, 直接返回
    if parent_logger:
        return parent_logger

    # 获取logger
    logger = logging.getLogger(script_name)
    
    # 如果logger已经有处理器, 说明已经被配置过, 直接返回
    if logger.handlers:
        return logger
        
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 创建logs目录(如果不存在)
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # 设置日志文件路径
    log_file = os.path.join(logs_dir, f"{os.path.splitext(os.path.basename(script_name))[0]}.log")
    
    # 创建滚动文件处理器
    # maxBytes=1MB, backupCount=1 表示:
    # - 每个日志文件最大1MB
    # - 保留1个备份文件
    # 当日志文件达到1MB时, 会自动创建新的日志文件, 旧的文件会被重命名为 xxx.log.1, xxx.log.2, xxx.log.3
    # 超过1个文件时, 最旧的文件会被删除
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1024*1024,  # 1MB
        backupCount=1,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # 将格式化器添加到处理器
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 将处理器添加到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 阻止日志传递到父logger
    logger.propagate = False
    
    return logger 