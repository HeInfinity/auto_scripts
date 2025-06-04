import os
import sys
import yaml
import json
import requests
import asyncio
import datetime
import pytz
from typing import Dict, Tuple, Optional, Any
from abc import ABC, abstractmethod
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

class TokenManager:
    """Token管理基类"""
    
    def __init__(self, app_name: str, logger=None):
        """
        初始化TokenManager
        :param app_name: 应用名称(如'moxueyuan', 'wechat'等)
        :param logger: 日志记录器
        """
        self.app_name = app_name
        self.logger = logger
        self.api_config = self._load_api_config()
        self.post_body_config = self._load_post_body_config()
        
        # 验证应用配置是否存在
        if app_name not in self.api_config:
            raise ValueError(f"应用 {app_name} 在api.yaml中未配置")
        if 'get_token' not in self.api_config[app_name]['endpoints']:
            raise ValueError(f"应用 {app_name} 未配置get_token端点")
        
    def _load_api_config(self) -> Dict:
        """加载API配置"""
        project_root = self._find_project_root()
        config_path = os.path.join(project_root, 'auto_scripts/config/api.yaml')
        
        if not os.path.exists(config_path):
            error_msg = f"未找到配置文件: {config_path}"
            self._log(error_msg, 'error')
            raise FileNotFoundError(error_msg)
            
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)['api_configs']
            
    def _load_post_body_config(self) -> Dict:
        """加载POST请求体配置"""
        project_root = self._find_project_root()
        config_path = os.path.join(project_root, 'auto_scripts/config/api_post_body.yaml')
        
        if not os.path.exists(config_path):
            return {}
            
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)['post_body']
            
    def _find_project_root(self, root_name='Python') -> str:
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
            
    def _get_current_utc8_time(self) -> datetime.datetime:
        """获取当前UTC+8时间"""
        tz_utc_8 = pytz.timezone('Asia/Shanghai')
        return datetime.datetime.now(tz=tz_utc_8)
        
    def _log(self, message: str, level: str = 'info'):
        """记录日志"""
        current_time = self._get_current_utc8_time().strftime('%Y-%m-%d %H:%M:%S')
        if self.logger:
            log_method = getattr(self.logger, level)
            log_method(f"[{current_time}] [{self.app_name}] {message}")
        else:
            print(f"[{current_time}] [{self.app_name}] [{level.upper()}] {message}")
            
    def _build_url(self, endpoint_config: Dict) -> str:
        """构建完整的API URL"""
        app_config = self.api_config[self.app_name]
        base_url = app_config['base_url']
        path = endpoint_config['path']
        return f"{base_url}{path}"
        
    def _build_params(self, endpoint_config: Dict) -> Dict:
        """构建请求参数"""
        params = endpoint_config.get('params', {}).copy()
        # 从环境变量获取参数值
        for key in params.keys():
            env_key = f"{self.app_name}_{key}".upper()
            params[key] = os.getenv(env_key)
            if not params[key]:
                raise ValueError(f"环境变量 {env_key} 未设置")
        return params
        
    def _build_body(self, endpoint_config: Dict) -> Optional[Dict]:
        """构建POST请求体"""
        if 'body_template' not in endpoint_config:
            return None
            
        template_name = endpoint_config['body_template']
        if self.app_name not in self.post_body_config:
            return None
            
        body = self.post_body_config[self.app_name].get(template_name, {}).copy()
        # 从环境变量获取参数值
        for key in body.keys():
            env_key = f"{self.app_name}_{key}".upper()
            env_value = os.getenv(env_key)
            if env_value:
                body[key] = env_value
        return body
        
    def _parse_token_response(self, response_data: Dict) -> str:
        """
        解析响应数据获取token
        :param response_data: API响应数据
        :return: access_token字符串
        """
        # 获取当前应用的get_token配置
        endpoint_config = self.api_config[self.app_name]['endpoints']['get_token']
        
        # 如果配置中指定了response_token_path，则按照指定路径解析
        token_path = endpoint_config.get('response_token_path', 'access_token').split('.')
        
        # 按照路径逐层解析响应数据
        result = response_data
        for key in token_path:
            if not isinstance(result, dict) or key not in result:
                raise ValueError(f"无法从响应中获取{self.app_name}的access_token，路径{'.'.join(token_path)}无效")
            result = result[key]
            
        if not isinstance(result, str):
            raise ValueError(f"获取到的token不是字符串类型: {result}")
            
        return result
        
    async def get_token(self) -> Tuple[str, datetime.datetime, datetime.datetime]:
        """
        获取访问令牌
        :return: (access_token, requested_at, expires_at)的元组
        """
        self._log("开始获取access_token")
        
        endpoint_config = self.api_config[self.app_name]['endpoints']['get_token']
        url = self._build_url(endpoint_config)
        params = self._build_params(endpoint_config)
        body = self._build_body(endpoint_config)
        
        method = endpoint_config.get('method', 'GET').upper()
        
        try:
            if method == 'GET':
                response = requests.get(url, params=params)
            elif method == 'POST':
                response = requests.post(url, params=params, json=body)
            else:
                raise ValueError(f"不支持的请求方法: {method}")
                
            response.raise_for_status()
            data = response.json()
            
            # 解析响应数据
            token = self._parse_token_response(data)
            requested_at = self._get_current_utc8_time()
            expires_at = requested_at + datetime.timedelta(hours=2) - datetime.timedelta(minutes=10)
            
            self._log(f"成功获取token: {token}")
            return token, requested_at, expires_at
            
        except Exception as e:
            self._log(f"获取token失败: {str(e)}", 'error')
            raise

def create_token_manager(app_name: str, logger=None) -> TokenManager:
    """
    创建TokenManager实例
    :param app_name: 应用名称
    :param logger: 日志记录器
    :return: TokenManager实例
    """
    return TokenManager(app_name, logger)

# 如果直接运行此脚本，执行测试代码
if __name__ == "__main__":
    print("开始测试TokenManager")
    
    async def test():
        try:
            # 测试魔学院token管理器
            mxy_manager = create_token_manager('moxueyuan')
            token, requested_at, expires_at = await mxy_manager.get_token()
            
        except Exception as e:
            print(f"测试过程中发生错误: {str(e)}")
    
    # 运行测试
    asyncio.run(test())