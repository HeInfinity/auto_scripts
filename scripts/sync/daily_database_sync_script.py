import os
import sys
import asyncio
import time
from datetime import datetime, timedelta
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
            
            # 等待日志缓冲刷新到磁盘
            self.logger.info("等待日志缓冲刷新...")
            await asyncio.sleep(2)  # 等待2秒确保日志写入磁盘
            
            # 强制刷新日志处理器
            for handler in self.logger.handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()
            
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
        
        # 计算时间窗口 - 放宽时间限制，避免过度过滤
        time_threshold = start_time - timedelta(minutes=5)  # 允许5分钟的时间差
        
        self.logger.info(f"开始解析日志，时间阈值: {time_threshold}, 总日志行数: {len(log_content)}")
        
        parsed_count = 0
        for line_num, line in enumerate(log_content, 1):
            line = line.strip()
            if not line:
                continue
                
            # 解析日志时间
            try:
                # 解析新的日志格式: [YYYY-MM-DD HH:MM:SS] [app_name] [LEVEL] message
                if line.startswith('[') and '] [' in line:
                    # 提取时间部分 [YYYY-MM-DD HH:MM:SS]
                    time_end = line.find('] [')
                    if time_end > 0:
                        log_time_str = line[1:time_end]  # 去掉开头的 [
                        try:
                            log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            # 如果标准格式失败，尝试其他格式
                            try:
                                log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                # 如果时间解析失败，仍然处理这行日志（不过滤）
                                log_time = time_threshold
                    else:
                        log_time = time_threshold
                else:
                    # 兼容旧格式或其他格式的日志
                    time_parts = line.split()
                    if len(time_parts) >= 2:
                        log_time_str = time_parts[0] + ' ' + time_parts[1]
                        try:
                            log_time = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            log_time = time_threshold
                    else:
                        log_time = time_threshold
                    
                # 使用放宽的时间限制
                if log_time >= time_threshold:
                    parsed_count += 1
                    
                    # 处理总时长
                    if "脚本运行的总时长" in line:
                        parsed_logs['total_times'].append(line)
                        continue
                        
                    # 处理错误和失败
                    if "同步数据没有成功" in line:
                        parsed_logs['fail'].append(line)
                        continue
                    if "错误信息" in line:
                        parsed_logs['errors'].append(line)
                        continue
                        
                    # 处理同步日期
                    if "需要同步的日期" in line:
                        parsed_logs['sync_dates'].append(line)
                        continue
                        
                    # 处理各类表的日志
                    if "数据同步完成" in line and "总耗时" in line and "插入的行数" in line:
                        # large_table的日志格式
                        parsed_logs['large_table_logs'].append(line)
                    elif "全量刷新完成" in line and "总耗时" in line and "其中" in line:
                        # full_refresh的日志格式
                        parsed_logs['full_refresh_logs'].append(line)
                    elif "数据同步完成" in line and "同步的行数" in line:
                        # small_table成功的日志格式
                        parsed_logs['small_table_logs'].append(line)
                    elif "查询用时" in line and "总查询到的行数" in line:
                        # small_table的其他日志格式
                        parsed_logs['small_table_logs'].append(line)
                        
            except Exception as e:
                # 记录解析异常但继续处理
                self.logger.warning(f"解析日志行 {line_num} 时出现异常: {e}, 行内容: {line[:100]}")
                continue
                
        self.logger.info(f"日志解析完成，共解析 {parsed_count} 行有效日志")
        
        # 记录各类日志的数量
        for log_type, logs in parsed_logs.items():
            if logs:
                self.logger.info(f"{log_type}: {len(logs)} 条")
                
        return parsed_logs
        
    async def _send_execution_email(self, success: bool, start_time: datetime) -> None:
        """发送执行结果邮件"""
        try:
            # 构建日志文件路径
            log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs',
                                  f"{os.path.splitext(os.path.basename(__file__))[0]}.log")
            
            self.logger.info(f"准备读取日志文件: {log_file}")
            
            # 检查日志文件是否存在
            if not os.path.exists(log_file):
                self.logger.error(f"日志文件不存在: {log_file}")
                # 发送错误邮件
                self.email_sender.send_email(
                    subject='数据导入执行结果 - 日志文件缺失',
                    body=f"脚本执行完成，但无法找到日志文件: {log_file}",
                    receiver_group='default'
                )
                return
                
            # 读取日志文件
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                log_content = f.readlines()
                
            self.logger.info(f"成功读取日志文件，共 {len(log_content)} 行")
                
            # 解析日志内容
            parsed_logs = self._parse_log_content(log_content, start_time)
            
            # 检查是否有有效的日志内容
            total_logs = sum(len(logs) for logs in parsed_logs.values())
            if total_logs == 0:
                self.logger.warning("未解析到任何有效日志，将发送包含所有日志的邮件")
                # 如果没有解析到有效日志，发送最近的日志内容
                recent_logs = log_content[-50:] if len(log_content) > 50 else log_content
                body = (
                    f"脚本执行{'成功' if success else '失败'}，但日志解析异常。\n"
                    f"以下是最近的日志内容：\n\n"
                    f"{''.join(recent_logs)}"
                )
            else:
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
            # 尝试发送错误通知邮件
            try:
                self.email_sender.send_email(
                    subject='数据导入执行结果 - 邮件发送异常',
                    body=f"脚本执行完成，但邮件发送过程中出现异常: {error_msg}",
                    receiver_group='default'
                )
            except Exception as e2:
                self.logger.error(f'错误通知邮件也发送失败: {e2}')

async def main():
    """主函数"""
    sync_script = DatabaseSyncScript()
    await sync_script.execute_sync_job()

if __name__ == '__main__':
    asyncio.run(main())