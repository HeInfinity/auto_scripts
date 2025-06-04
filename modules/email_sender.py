import os
import sys
import yaml
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional, Union
from datetime import datetime
from dotenv import load_dotenv

# 动态查找项目根目录并将 config 目录添加到 sys.path
def find_project_root(root_name='Python'):
    """
    查找项目根目录
    :param root_name: 项目根目录名称
    :return: 项目根目录的完整路径
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.basename(current_dir) == root_name:
            return current_dir
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            raise RuntimeError(f"无法找到包含目录 '{root_name}' 的项目根目录")
        current_dir = new_dir

def load_all_email_configs(config_subpath='auto_scripts/config/email.yaml') -> Dict:
    """
    加载所有邮件配置, 并从.env文件中读取密码
    :param config_subpath: 配置文件相对于项目根目录的路径
    :return: 配置字典
    """
    project_root = find_project_root()
    
    # 加载.env文件
    env_path = os.path.join(project_root, 'auto_scripts', '.env')
    if not os.path.exists(env_path):
        raise FileNotFoundError(f"未找到.env文件: {env_path}")
    load_dotenv(env_path)
    
    # 加载email.yaml
    config_path = os.path.join(project_root, config_subpath)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as f:
        configs = yaml.safe_load(f)
    
    # 从环境变量中读取密码并更新配置
    for env_name, config in configs.items():
        password_env_key = f"EMAIL_{env_name.upper()}"
        password = os.getenv(password_env_key)
        if password is None:
            raise ValueError(f"在.env文件中未找到邮箱 {env_name} 的密码配置 ({password_env_key})")
        config['smtp']['password'] = password
    
    return configs

class EmailManager:
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化邮件管理器
        :param logger: 日志记录器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.configs = load_all_email_configs()
        
    def get_sender(self, env: str = 'default') -> 'EmailSender':
        """
        获取指定环境的邮件发送器
        :param env: 环境名称
        :return: EmailSender实例
        """
        if env not in self.configs:
            raise ValueError(f"配置文件中不存在名为 '{env}' 的配置")
            
        return EmailSender(config=self.configs[env], logger=self.logger)

class EmailSender:
    def __init__(self, config: Dict, logger: Optional[logging.Logger] = None):
        """
        初始化邮件发送器
        :param config: 邮件配置
        :param logger: 日志记录器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = config
        
        # 获取SMTP配置
        if 'smtp' not in self.config:
            raise ValueError("配置中未找到SMTP配置")
        self.smtp_config = self.config['smtp']
        
        # 获取发件人配置
        if 'sender' not in self.config:
            raise ValueError("配置中未找到发件人配置")
        self.sender_config = self.config['sender'][0]  # 使用第一个发件人配置
        
        # 获取收件人配置
        if 'receivers' not in self.config:
            raise ValueError("配置中未找到收件人配置")
        self.receiver_groups = self.config['receivers']

    def _create_smtp_server(self) -> smtplib.SMTP:
        """
        创建SMTP服务器连接
        :return: SMTP服务器对象
        """
        if self.smtp_config.get('use_ssl', False):
            server = smtplib.SMTP_SSL(
                self.smtp_config['server'],
                self.smtp_config['port'],
                timeout=self.smtp_config.get('timeout', 10)
            )
        else:
            server = smtplib.SMTP(
                self.smtp_config['server'],
                self.smtp_config['port'],
                timeout=self.smtp_config.get('timeout', 10)
            )

        if self.smtp_config.get('use_tls', False):
            server.starttls()

        server.login(
            self.sender_config['email'],  # 使用发件人邮箱作为登录用户名
            self.smtp_config['password']
        )
        
        return server

    def get_receivers(self, group: str = 'default') -> List[Dict]:
        """
        获取指定组的收件人配置
        :param group: 收件人组名
        :return: 收件人配置列表
        """
        if group not in self.receiver_groups:
            raise ValueError(f"未找到收件人组配置: {group}")
        return self.receiver_groups[group]

    def send_email(
        self,
        subject: str,
        body: str,
        receiver_group: str = 'default',
        is_html: bool = False
    ) -> bool:
        """
        发送邮件
        :param subject: 邮件主题
        :param body: 邮件正文
        :param receiver_group: 收件人组名称
        :param is_html: 是否为HTML格式
        :return: 是否发送成功
        """
        try:
            # 获取收件人列表
            receivers = self.get_receivers(receiver_group)
            receiver_list = [r['email'] for r in receivers]

            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = f"{self.sender_config['name']} <{self.sender_config['email']}>"
            msg['To'] = ', '.join(receiver_list)
            msg['Subject'] = subject

            # 添加正文
            content_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(body, content_type, 'utf-8'))

            # 发送邮件
            with self._create_smtp_server() as server:
                server.send_message(msg)

            self.logger.info(f'邮件发送成功: {subject}')
            return True

        except Exception as e:
            self.logger.error(f'邮件发送失败: {str(e)}')
            return False

    def send_error_notification(
        self,
        error_message: str,
        subject_prefix: str = "错误通知",
        additional_info: Optional[Dict] = None,
        receiver_group: str = 'default'
    ) -> bool:
        """
        发送错误通知邮件
        :param error_message: 错误信息
        :param subject_prefix: 主题前缀
        :param additional_info: 额外信息
        :param receiver_group: 收件人组名
        :return: 是否发送成功
        """
        subject = f"{subject_prefix} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        body = f"错误时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        body += f"错误信息: {error_message}\n"
        
        if additional_info:
            body += "\n额外信息:\n"
            for key, value in additional_info.items():
                body += f"{key}: {value}\n"
                
        return self.send_email(
            subject=subject,
            body=body,
            receiver_group=receiver_group
        )

# 使用示例
if __name__ == '__main__':
    from log_tools import setup_logger
    
    # 获取logger
    logger = setup_logger(__file__)
    
    # 创建邮件管理器实例
    email_manager = EmailManager(logger=logger)
    
    # 获取指定环境的邮件发送器
    email_sender = email_manager.get_sender('wework_hjq')
    
    # 发送测试邮件
    success = email_sender.send_email(
        subject="测试邮件",
        body="这是一封测试邮件",
        receiver_group="default"
    )
    
    if success:
        logger.info("测试邮件发送成功")
    else:
        logger.error("测试邮件发送失败") 