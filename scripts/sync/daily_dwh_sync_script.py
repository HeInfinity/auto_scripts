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
        
        # 保存子脚本输出
        self.sub_script_output = []
        
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
            
            # 保存子脚本输出供后续邮件发送使用
            self.sub_script_output = []
            if stdout_text:
                self.sub_script_output.extend(stdout_text.splitlines())
            if stderr_text:
                self.sub_script_output.extend(stderr_text.splitlines())
            
            # 记录输出到wrapper脚本自己的日志（但不重复记录格式）
            self.logger.info(f"子脚本执行完毕，共获得 {len(self.sub_script_output)} 行输出")
            
            # 检查进程退出状态
            if process.returncode != 0:
                self.logger.error(f"子脚本执行失败，退出码: {process.returncode}")
                await self._send_execution_email(False, start_time)
            else:
                self.logger.info("子脚本执行成功")
                await self._send_execution_email(True, start_time)
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"发生错误信息: {error_msg}")
            await self._send_execution_email(False, start_time)
            
    def _parse_output_content(self, output_lines: List[str]) -> Dict[str, List[str]]:
        """直接从子脚本输出中解析日志内容"""
        parsed_logs = {
            'query_times': [],      # SQL查询时间
            'errors': [],           # 错误信息
            'data_range': [],       # 数据范围
            'total_times': [],      # 总执行时间
            'days_interval': [],    # 同步时间范围
            'get_data_times': []    # 获取数据时间
        }
        
        self.logger.info(f"开始解析子脚本输出，总输出行数: {len(output_lines)}")
        
        parsed_count = 0
        for line_num, line in enumerate(output_lines, 1):
            line = line.strip()
            if not line:
                continue
                
            parsed_count += 1
            
            # 根据关键词匹配，收集相关日志
            if "代码总执行时间" in line:
                parsed_logs['total_times'].append(line)
            elif "错误信息" in line or "ERROR" in line:
                parsed_logs['errors'].append(line)
            elif "同步时间范围" in line:
                parsed_logs['days_interval'].append(line)
            elif "需要插入的记录数" in line or "需要更新的记录数" in line:
                parsed_logs['data_range'].append(line)
            elif "从个人数据库 SQL 查询花费时间" in line:
                parsed_logs['get_data_times'].append(line)
            elif "结束，耗时" in line:
                parsed_logs['query_times'].append(line)
                
        self.logger.info(f"输出解析完成，共解析 {parsed_count} 行有效输出")
        
        # 记录各类日志的数量
        for log_type, logs in parsed_logs.items():
            if logs:
                self.logger.info(f"{log_type}: {len(logs)} 条")
                
        return parsed_logs
        
    async def _send_execution_email(self, success: bool, start_time: datetime) -> None:
        """发送执行结果邮件"""
        try:
            self.logger.info("开始准备发送执行结果邮件")
            
            # 检查是否有子脚本输出
            if not self.sub_script_output:
                self.logger.warning("没有子脚本输出，将发送简单的执行结果邮件")
                # 发送简单的执行结果邮件
                self.email_sender.send_email(
                    subject='数仓同步执行结果 - 无输出',
                    body=f"脚本执行{'成功' if success else '失败'}，但没有获取到子脚本的输出内容。",
                    receiver_group='default'
                )
                return
                
            self.logger.info(f"开始解析 {len(self.sub_script_output)} 行子脚本输出")
                
            # 直接从子脚本输出解析日志内容
            parsed_logs = self._parse_output_content(self.sub_script_output)
            
            # 检查是否有有效的日志内容
            total_logs = sum(len(logs) for logs in parsed_logs.values())
            if total_logs == 0:
                self.logger.warning("未解析到任何有效日志，将发送包含原始输出的邮件")
                # 如果没有解析到有效日志，发送最近的输出内容
                recent_output = self.sub_script_output[-50:] if len(self.sub_script_output) > 50 else self.sub_script_output
                body = (
                    f"脚本执行{'成功' if success else '失败'}，但日志解析异常。\n"
                    f"以下是子脚本的输出内容：\n\n"
                    f"{''.join(line + '\n' for line in recent_output)}"
                )
            else:
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