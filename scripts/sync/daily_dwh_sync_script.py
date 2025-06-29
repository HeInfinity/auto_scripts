import os
import sys
import asyncio
from datetime import datetime
from typing import Optional, Dict, List

# 动态获取当前脚本所在目录，并根据相对路径设置sys.path
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
        os.path.join(project_root, 'auto_scripts', 'jobs'),
        os.path.join(project_root, 'auto_scripts', 'modules')
    ]
    
    # 添加路径并确保唯一性
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.append(path)
            
    return project_root

# 调用函数加载配置
project_root = load_sys_path()
from directory import SCRIPT_DIR
from log_tools import setup_logger
from email_sender import EmailManager

class DWHSyncScript:
    def __init__(self):
        """初始化数据仓库同步脚本"""
        # 获取logger, 支持继承父logger
        parent_logger = os.environ.get('PARENT_LOGGER')
        self.logger = setup_logger(__file__, parent_logger)
        
        # 初始化邮件管理器和发送器
        self.email_manager = EmailManager(logger=self.logger)
        self.email_sender = self.email_manager.get_sender('wework_hjq')
        
    async def execute_sync_job(self) -> None:
        """执行数据仓库同步任务"""
        start_time = datetime.now()
        self.logger.info("开始执行脚本...")
        
        try:
            # 构建daily_dwh_sync.py的完整路径
            sync_script_path = os.path.join(project_root, 'auto_scripts', 'jobs', 'sync', 'daily_dwh_sync.py')
            self.logger.info(f"执行脚本路径: {sync_script_path}")
            
            if not os.path.exists(sync_script_path):
                raise FileNotFoundError(f"找不到脚本文件: {sync_script_path}")
            
            # 使用异步子进程执行daily_dwh_sync.py
            process = await asyncio.create_subprocess_exec(
                'python',
                sync_script_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=dict(os.environ, PARENT_LOGGER=__file__)
            )
            
            # 异步读取输出
            stdout, stderr = await process.communicate()
            
            # 解码输出(使用utf-8编码)
            stdout_text = stdout.decode('utf-8', errors='replace')
            stderr_text = stderr.decode('utf-8', errors='replace')
            
            # 记录日志
            self._log_output(stdout_text, stderr_text)
            
            self.logger.info("脚本执行完毕...")
            await self._send_execution_email(True, start_time)
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"发生错误信息: {error_msg}")
            await self._send_execution_email(False, start_time)
            
    def _log_output(self, stdout_text: str, stderr_text: str) -> None:
        """记录脚本输出到日志"""
        if stdout_text:
            for line in stdout_text.splitlines():
                if line.strip():  # 只记录非空行
                    # 检查是否已经是完整的日志格式（以[开头且包含] [）
                    if line.startswith('[') and '] [' in line:
                        # 直接写入日志文件，避免双重时间戳
                        self.logger.handlers[0].stream.write(line.strip() + '\n')
                        self.logger.handlers[0].stream.flush()
                    else:
                        # 不是完整格式，使用logger记录
                        self.logger.info(line.strip())
        if stderr_text:
            for line in stderr_text.splitlines():
                if line.strip():  # 只记录非空行
                    # 检查是否已经是完整的日志格式
                    if line.startswith('[') and '] [' in line:
                        # 直接写入日志文件，避免双重时间戳
                        self.logger.handlers[0].stream.write(line.strip() + '\n')
                        self.logger.handlers[0].stream.flush()
                    else:
                        # 不是完整格式，使用logger记录
                        self.logger.error(line.strip())
                
    def _parse_log_content(self, log_content: List[str], start_time: datetime) -> Dict[str, List[str]]:
        """解析日志内容"""
        parsed_logs = {
            'query_times': [],      # SQL查询时间
            'errors': [],           # 错误信息
            'data_range': [],       # 数据范围
            'total_times': [],      # 总执行时间
            'days_interval': [],    # 同步时间范围
            'get_data_times': []    # 获取数据时间
        }
        
        for line in log_content:
            # 解析日志时间，只处理本次执行期间的日志
            try:
                line_stripped = line.strip()
                if not line_stripped:
                    continue
                
                # 提取日志时间进行过滤
                log_time = None
                if line_stripped.startswith('[') and '] [' in line_stripped:
                    # 处理[时间] [应用名] [级别]格式
                    time_end = line_stripped.find('] [')
                    if time_end > 0:
                        log_time_str = line_stripped[1:time_end]
                        log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                else:
                    # 处理时间 级别格式
                    parts = line_stripped.split()
                    if len(parts) >= 3:
                        try:
                            log_time_str = parts[0] + ' ' + parts[1]
                            log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            continue
                
                # 只处理本次执行开始时间之后的日志
                if log_time and log_time >= start_time:
                    # 根据关键词匹配，收集相关日志
                    if "代码总执行时间" in line_stripped:
                        parsed_logs['total_times'].append(line_stripped)
                    elif "错误信息" in line_stripped:
                        parsed_logs['errors'].append(line_stripped)
                    elif "同步时间范围" in line_stripped:
                        parsed_logs['days_interval'].append(line_stripped)
                    elif "需要插入的记录数" in line_stripped or "需要更新的记录数" in line_stripped:
                        parsed_logs['data_range'].append(line_stripped)
                    elif "从个人数据库 SQL 查询花费时间" in line_stripped:
                        parsed_logs['get_data_times'].append(line_stripped)
                    elif "结束，耗时" in line_stripped:
                        parsed_logs['query_times'].append(line_stripped)
                        
            except Exception:
                continue  # 跳过无法解析的行
                
        return parsed_logs
        
    async def _send_execution_email(self, success: bool, start_time: datetime) -> None:
        """发送执行结果邮件"""
        try:
            # 等待日志完全写入文件
            await asyncio.sleep(2)
            
            # 构建日志文件路径
            script_name = os.path.splitext(os.path.basename(__file__))[0]
            log_file = os.path.join(project_root, 'auto_scripts', 'logs', f"{script_name}.log")
            
            if not os.path.exists(log_file):
                # 尝试备用路径
                log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs',
                                      f"{script_name}.log")
            
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                    log_content = f.readlines()
            else:
                self.logger.error(f"无法找到日志文件: {log_file}")
                log_content = []
                
            # 解析日志内容
            parsed_logs = self._parse_log_content(log_content, start_time)
            
            # 组装日志内容，确保不同类型的日志之间有空行
            email_content = []
            
            # 添加总执行时间
            if parsed_logs['total_times']:
                email_content.append('\n'.join(parsed_logs['total_times']))
            
            # 添加错误信息(如果有)
            if parsed_logs['errors']:
                if email_content:
                    email_content.append("")  # 添加空行
                email_content.append('\n'.join(parsed_logs['errors']))
            
            # 添加同步时间范围
            if parsed_logs['days_interval']:
                if email_content:
                    email_content.append("")  # 添加空行
                email_content.append('\n'.join(parsed_logs['days_interval']))
            
            # 添加数据范围
            if parsed_logs['data_range']:
                if email_content:
                    email_content.append("")  # 添加空行
                email_content.append('\n'.join(parsed_logs['data_range']))
            
            # 添加获取数据时间
            if parsed_logs['get_data_times']:
                if email_content:
                    email_content.append("")  # 添加空行
                email_content.append('\n'.join(parsed_logs['get_data_times']))
            
            # 添加查询时间
            if parsed_logs['query_times']:
                if email_content:
                    email_content.append("")  # 添加空行
                email_content.append('\n'.join(parsed_logs['query_times']))
            
            # 连接所有部分
            log_lines = '\n'.join(email_content)
            
            # 如果没有解析到任何有用的日志内容，添加基本执行信息
            if not log_lines.strip():
                self.logger.warning("未解析到有用的日志内容，使用基本执行信息")
                log_lines = f"脚本执行时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n执行状态: {'成功' if success else '失败'}"
            
            # 生成邮件内容
            execution_result = "脚本执行成功, 但出现错误" if parsed_logs['errors'] else f"脚本执行{'成功' if success else '失败'}"
            body = (
                f"{execution_result}。\n"
                f"以下是本次执行的日志内容：\n\n"
                f"{log_lines}"
            )
            
            # 发送邮件
            self.email_sender.send_email(
                subject='数仓同步执行结果',
                body=body,
                receiver_group='default'  # 指定接收组为default
            )
            self.logger.info('执行结果邮件已发送!')
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f'邮件发送失败: {error_msg}')

async def main():
    """主函数"""
    sync_script = DWHSyncScript()
    await sync_script.execute_sync_job()

if __name__ == '__main__':
    asyncio.run(main())