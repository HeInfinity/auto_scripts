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

class DatabaseSyncScript:
    def __init__(self):
        """初始化数据库同步脚本"""
        # 获取logger, 支持继承父logger
        parent_logger = os.environ.get('PARENT_LOGGER')
        self.logger = setup_logger(__file__, parent_logger)
        
        # 初始化邮件管理器和发送器
        self.email_manager = EmailManager(logger=self.logger)
        self.email_sender = self.email_manager.get_sender('wework_hjq')
        
    async def execute_sync_job(self) -> None:
        """执行数据库同步任务"""
        start_time = datetime.now()
        self.logger.info("开始执行脚本...")
        
        try:
            # 构建daily_database_sync.py的完整路径
            sync_script_path = os.path.join(project_root, 'auto_scripts', 'jobs', 'sync', 'daily_database_sync.py')
            self.logger.info(f"执行脚本路径: {sync_script_path}")
            
            if not os.path.exists(sync_script_path):
                raise FileNotFoundError(f"找不到脚本文件: {sync_script_path}")
            
            # 使用异步子进程执行daily_database_sync.py
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
                # 只输出原始内容, 不加前缀
                self.logger.handlers[0].stream.write(line.strip() + '\n')
                self.logger.handlers[0].stream.flush()
        if stderr_text:
            for line in stderr_text.splitlines():
                self.logger.handlers[0].stream.write(line.strip() + '\n')
                self.logger.handlers[0].stream.flush()
                
    def _parse_log_content(self, log_content: List[str], start_time: datetime) -> Dict[str, List[str]]:
        """解析日志内容"""
        parsed_logs = {
            'large_table_logs': [],    # large_table的日志
            'small_table_logs': [],    # small_table的日志
            'full_refresh_logs': [],   # full_refresh的日志
            'total_times': [],         # 总执行时间
            'fail': [],               # 同步失败记录
            'errors': [],             # 错误信息
            'sync_dates': []          # 同步日期
        }
        
        # 用于临时存储日志行
        current_log_line = None
        
        for line in log_content:
            # 解析日志时间
            try:
                log_time_str = line.split()[0] + ' ' + line.split()[1]
                log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                
                # 只处理start_time之后的日志
                if log_time >= start_time:
                    # 处理总时长
                    if "脚本运行的总时长" in line:
                        parsed_logs['total_times'].append(line.strip())
                        continue
                        
                    # 处理错误和失败
                    if "同步数据没有成功" in line:
                        parsed_logs['fail'].append(line.strip())
                        continue
                    if "错误信息" in line:
                        parsed_logs['errors'].append(line.strip())
                        continue
                        
                    # 处理同步日期
                    if "需要同步的日期" in line:
                        parsed_logs['sync_dates'].append(line.strip())
                        continue
                        
                    # 处理各类表的日志
                    if "数据同步完成" in line and "总耗时" in line and "插入的行数" in line:
                        # large_table的日志格式
                        parsed_logs['large_table_logs'].append(line.strip())
                    elif "全量刷新完成" in line and "总耗时" in line and "其中" in line:
                        # full_refresh的日志格式
                        parsed_logs['full_refresh_logs'].append(line.strip())
                    elif "数据同步完成" in line and "同步的行数" in line:
                        # small_table成功的日志格式
                        parsed_logs['small_table_logs'].append(line.strip())
                    elif "查询用时" in line and "总查询到的行数" in line:
                        # small_table的其他日志格式
                        parsed_logs['small_table_logs'].append(line.strip())
                        
            except (ValueError, IndexError):
                continue  # 跳过无法解析的行
                
        return parsed_logs
        
    async def _send_execution_email(self, success: bool, start_time: datetime) -> None:
        """发送执行结果邮件"""
        try:
            # 读取日志文件
            log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs',
                                  f"{os.path.splitext(os.path.basename(__file__))[0]}.log")
            
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                log_content = f.readlines()
                
            # 解析日志内容
            parsed_logs = self._parse_log_content(log_content, start_time)
            
            # 组装日志内容，确保不同类型的日志之间有空行
            email_content = []
            
            # 添加总执行时间
            if parsed_logs['total_times']:
                email_content.append('\n'.join(parsed_logs['total_times']))
            
            # 添加large_table日志(在后面添加两个换行符确保有空行)
            if parsed_logs['large_table_logs']:
                email_content.append('\n'.join(parsed_logs['large_table_logs']) + "\n\n")
            
            # 添加small_table日志(在后面添加两个换行符确保有空行)
            if parsed_logs['small_table_logs']:
                email_content.append('\n'.join(parsed_logs['small_table_logs']) + "\n\n")
            
            # 添加full_refresh日志
            if parsed_logs['full_refresh_logs']:
                email_content.append('\n'.join(parsed_logs['full_refresh_logs']))
            
            # 添加其他日志(如果有的话添加空行)
            other_logs = []
            if parsed_logs['fail']:
                other_logs.append('\n'.join(parsed_logs['fail']))
            if parsed_logs['errors']:
                other_logs.append('\n'.join(parsed_logs['errors']))
            if parsed_logs['sync_dates']:
                other_logs.append('\n'.join(parsed_logs['sync_dates']))
            
            if other_logs and (parsed_logs['large_table_logs'] or 
                              parsed_logs['small_table_logs'] or 
                              parsed_logs['full_refresh_logs']):
                email_content.append("\n")  # 添加一个空行分隔主要日志和其他日志
            
            email_content.extend(other_logs)
            
            # 连接所有部分
            log_lines = '\n'.join(email_content)
            
            # 生成邮件内容
            execution_result = "脚本执行成功, 但出现错误" if parsed_logs['errors'] else f"脚本执行{'成功' if success else '失败'}"
            body = (
                f"{execution_result}。\n"
                f"以下是本次执行的日志内容：\n\n"
                f"{log_lines}"
            )
            
            # 发送邮件
            self.email_sender.send_email(
                subject='数据导入执行结果',
                body=body,
                receiver_group='default'  # 指定接收组为default
            )
            self.logger.info('执行结果邮件已发送!')
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f'邮件发送失败: {error_msg}')

async def main():
    """主函数"""
    sync_script = DatabaseSyncScript()
    await sync_script.execute_sync_job()

if __name__ == '__main__':
    asyncio.run(main())