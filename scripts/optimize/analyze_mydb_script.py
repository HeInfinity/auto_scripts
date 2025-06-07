import os
import subprocess
import pytz
import sys
import chardet
from datetime import datetime
from typing import Dict, List, Optional

# 动态获取当前脚本所在目录, 并根据相对路径设置sys.path
def load_sys_path():
    """动态查找项目根目录并将 config 目录添加到 sys.path"""
    project_root_name = 'Python'
    current_dir = os.path.dirname(os.path.abspath(__file__))

    while True:
        if os.path.basename(current_dir) == project_root_name:
            project_root = current_dir
            break
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            raise RuntimeError(f"无法找到包含目录 '{project_root_name}' 的项目根目录")
        current_dir = new_dir
        
    # 需要添加的路径列表
    paths_to_add = [
        os.path.join(project_root, 'auto_scripts'),
        os.path.join(project_root, 'auto_scripts', 'jobs'),
        os.path.join(project_root, 'auto_scripts', 'modules')
    ]
    
    # 添加路径并确保唯一性
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.append(path)
    return project_root

# 调用函数加载配置
load_sys_path()
from modules.log_tools import setup_logger
from modules.email_sender import EmailManager

# 获取logger
logger = setup_logger(__file__)

# 设置时区为UTC+8
timezone = pytz.timezone('Asia/Shanghai')

def parse_output(output: bytes, encoding: Optional[str] = None) -> List[str]:
    """
    解析命令输出并返回行列表
    :param output: 命令输出的字节
    :param encoding: 指定编码, 如果为None则自动检测
    :return: 输出行列表
    """
    if not encoding:
        encoding = chardet.detect(output)['encoding'] or 'utf-8'
    text = output.decode(encoding, errors='replace')
    return [line.strip() for line in text.splitlines() if line.strip()]

def collect_log_info(start_time: datetime, log_content: List[str]) -> Dict[str, List[str]]:
    """
    收集并分类日志信息
    :param start_time: 开始时间
    :param log_content: 日志内容
    :return: 分类后的日志信息
    """
    total_times = []
    errors = []
    query_times = []

    for line in log_content:
        # 解析日志时间
        try:
            log_time_str = line.split()[0] + ' ' + line.split()[1]
            log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
            log_time = timezone.localize(log_time)

            if log_time >= start_time:
                # 清理日志行，移除时间戳和日志级别
                cleaned_line = ' '.join(line.split()[4:]) if len(line.split()) > 4 else line
                
                if "总计耗时" in line:
                    total_times.append(cleaned_line)
                elif "错误信息" in line:
                    errors.append(cleaned_line)
                elif "ANALYZE操作, 耗时" in line:
                    query_times.append(cleaned_line)
        except (IndexError, ValueError):
            continue

    return {
        'total_times': total_times,
        'errors': errors,
        'query_times': query_times
    }

def format_email_body(log_info: Dict[str, List[str]], success: bool) -> str:
    """
    格式化邮件正文
    :param log_info: 日志信息
    :param success: 执行是否成功
    :return: 格式化后的邮件正文
    """
    # 根据是否有错误生成不同的正文内容
    if log_info['errors']:
        execution_result = "脚本执行成功, 但出现错误"
    else:
        execution_result = f"脚本执行{'成功' if success else '失败'}"

    # 获取当前时间
    current_time = datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')

    # 按顺序排列各类日志, 并在类别之间加入一个空行
    log_sections = []
    if log_info['total_times']:
        # 保持原始日志格式
        formatted_times = [f"{current_time} INFO {line}" for line in log_info['total_times']]
        log_sections.append('\n'.join(formatted_times))
    if log_info['errors']:
        # 保持原始日志格式
        formatted_errors = [f"{current_time} ERROR {line}" for line in log_info['errors']]
        log_sections.append('\n'.join(formatted_errors))
    if log_info['query_times']:
        # 保持原始日志格式
        formatted_query = [f"{current_time} INFO {line}" for line in log_info['query_times']]
        log_sections.append('\n'.join(formatted_query))

    log_lines = '\n\n'.join(log_sections)

    return (
        f"{execution_result}。\n"
        f"以下是本次执行的日志内容：\n"
        f"{log_lines}"
    )

def job():
    """
    主要任务函数
    """
    start_time = datetime.now(timezone)
    logger.info("开始执行脚本...")
    
    try:
        # 执行 Analyze_Database.py 并将输出写入日志
        project_root = load_sys_path()
        sub_script = os.path.join(project_root, 'auto_scripts', 'jobs', 'optimize', 'analyze_mydb.py')
        process = subprocess.Popen(
            ['python', sub_script],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        stdout, stderr = process.communicate()

        # 处理标准输出
        stdout_lines = parse_output(stdout)
        for line in stdout_lines:
            # 直接写入日志文件，避免重复时间戳
            logger.handlers[0].stream.write(line.strip() + '\n')
            logger.handlers[0].stream.flush()

        # 处理标准错误
        stderr_lines = parse_output(stderr)
        for line in stderr_lines:
            # 直接写入日志文件，避免重复时间戳
            logger.handlers[0].stream.write(line.strip() + '\n')
            logger.handlers[0].stream.flush()

        logger.info("脚本执行完毕...")

        # 读取日志文件内容
        with open(logger.handlers[0].baseFilename, 'r', encoding='utf-8', errors='replace') as f:
            log_content = f.readlines()

        # 收集日志信息
        log_info = collect_log_info(start_time, log_content)
        
        # 创建邮件管理器和发送器
        email_manager = EmailManager(logger=logger)
        email_sender = email_manager.get_sender('wework_hjq')
        
        # 发送邮件
        success = email_sender.send_email(
            subject='数据库Analyze优化执行结果',
            body=format_email_body(log_info, True),
            receiver_group='default'
        )
        
        if success:
            logger.info('执行结果邮件已发送!')
        else:
            logger.error('邮件发送失败')
            
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='replace').decode('utf-8')
        logger.error(f"发生错误信息: {error_msg}")
        
        # 发送错误通知
        try:
            email_manager = EmailManager(logger=logger)
            email_sender = email_manager.get_sender('wework_hjq')
            email_sender.send_error_notification(
                error_message=error_msg,
                subject_prefix='数据库Analyze优化执行失败',
                receiver_group='default'
            )
        except Exception as email_error:
            logger.error(f"发送错误通知邮件失败: {str(email_error)}")

if __name__ == '__main__':
    job()