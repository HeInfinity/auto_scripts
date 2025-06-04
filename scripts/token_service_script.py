import os
import time
import logging
import subprocess
import chardet
import pytz
import sys
from concurrent.futures import ThreadPoolExecutor

# 动态获取当前脚本所在目录，并根据相对路径设置sys.path
def load_config():
    """动态查找项目根目录并将 config 目录添加到 sys.path"""
    project_root_name = 'Python'
    current_dir = os.path.dirname(os.path.abspath(__file__))

    while True:
        if os.path.basename(current_dir) == project_root_name:
            break
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            raise RuntimeError(f"无法找到包含目录 '{project_root_name}' 的项目根目录")
        current_dir = new_dir

    config_dir = os.path.join(current_dir, 'config')
    sys.path.append(config_dir)

# 调用函数加载 config 目录
load_config()
from directory import SCRIPT_DIR

# 定义脚本目录和日志文件路径
log_file = os.path.join(SCRIPT_DIR, 'log', 'token_service_script.log')

# 设置时区为UTC+8
timezone = pytz.timezone('Asia/Shanghai')

# 多个Python脚本路径, 并发执行
scripts_to_run = [
    os.path.join(SCRIPT_DIR, 'projects', 'moxueyuan', 'Get_MXY_token.py'),
    # 可以在这里添加更多的脚本路径
]

# 多个Python脚本路径, 顺序执行
new_emloyee_script = os.path.join(SCRIPT_DIR, 'projects', 'moxueyuan', 'MXY_new_employee.py')

# 获取根日志记录器
logger = logging.getLogger()

# 如果已经有处理器，先清空处理器列表，避免重复输出
if logger.hasHandlers():
    logger.handlers.clear()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, 'a', 'utf-8'),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)

def run_script(script_path):
    """运行单个脚本的函数，并记录日志"""
    script_name = os.path.basename(script_path)
    retries = 3
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"正在执行脚本: {script_name} - 尝试次数: {attempt}")
            
            # 使用subprocess运行脚本并捕获输出，不指定编码，让系统选择默认编码
            result = subprocess.run(
                ['python', script_path],
                stdout=subprocess.PIPE,  # 捕获标准输出
                stderr=subprocess.PIPE,  # 捕获标准错误输出
            )
            
            # 自动检测输出编码
            detected_encoding = chardet.detect(result.stdout)['encoding']
            
            # 将输出根据检测到的编码解码为字符串
            stdout_text = result.stdout.decode(detected_encoding)
            stderr_text = result.stderr.decode(detected_encoding)
            
            # 去除多余的换行符和空格，并逐行写入日志
            if stdout_text:
                for line in stdout_text.splitlines():
                    logging.info(line.strip())
            if stderr_text:
                for line in stderr_text.splitlines():
                    logging.error(line.strip())

            if result.returncode == 0:
                logging.info(f"{script_name} 已成功执行")
                return True
            else:
                logging.error(f"{script_name} 执行失败，返回代码: {result.returncode}")

        except Exception as e:
            logging.error(f"脚本执行失败: {script_name} - 错误信息: {e} - 尝试次数: {attempt}")
    
    logging.error(f"{script_name} 未成功执行")
    return False

def job():
    """开始运行脚本任务"""
    logging.info("开始执行脚本...")
    with ThreadPoolExecutor(max_workers=len(scripts_to_run)) as executor:
        executor.map(run_script, scripts_to_run)
    
    # 执行完第一个脚本后，等待10秒再执行下一个脚本
    num = 10  # 延迟时间，单位为秒
    time.sleep(num)  # 等待 num 秒
    logging.info(f"{num}秒延迟后，开始执行脚本: {os.path.basename(new_emloyee_script)}")
    run_script(new_emloyee_script)  # 执行脚本
    logging.info("结束执行脚本...")
    
if __name__ == '__main__':
    job()