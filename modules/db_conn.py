import os
import sys
import yaml
import asyncio
import aiomysql
import asyncpg
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, Any, Union
import logging
from log_tools import setup_logger

# 获取logger
logger = setup_logger(__file__)

# -------------------------------------------------------
# 找到项目根目录(Python文件夹), 并定位 config/database.yaml
# -------------------------------------------------------
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

def load_all_db_configs(config_subpath='auto_scripts/config/database.yaml'):
    """
    加载数据库配置文件, 并从.env文件中读取密码
    :param config_subpath: 配置文件相对于项目根目录的路径
    :return: 配置字典
    """
    project_root = find_project_root()
    
    # 加载.env文件
    env_path = os.path.join(project_root, 'auto_scripts', '.env')
    if not os.path.exists(env_path):
        raise FileNotFoundError(f"未找到.env文件: {env_path}")
    load_dotenv(env_path)
    
    # 加载database.yaml
    config_path = os.path.join(project_root, config_subpath)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        configs = yaml.safe_load(f)
    
    # 从环境变量中读取密码并更新配置
    for env_name, config in configs.items():
        password_env_key = f"DB_PASSWORD_{env_name.upper()}"
        password = os.getenv(password_env_key)
        if password is None:
            raise ValueError(f"在.env文件中未找到数据库 {env_name} 的密码配置 ({password_env_key})")
        config['password'] = password
    
    return configs

# -------------------------------------------------------
# 连接池管理器
# -------------------------------------------------------
class DBManager:
    def __init__(self, logger: Optional[logging.Logger] = None, max_retry=3, connect_timeout=20, max_concurrent=3):
        """
        初始化数据库管理器
        :param logger: 日志记录器, 如果为None则创建新的日志记录器
        :param max_retry: 最大重试次数
        :param connect_timeout: 连接超时时间(秒)
        :param max_concurrent: 每个数据库环境最大并发连接数
        """
        self.logger = logger or setup_logger(__file__)
        self.configs = load_all_db_configs()
        self.pools = {}  # 存放连接池
        self.max_retry = max_retry
        self.connect_timeout = connect_timeout
        self.max_concurrent = max_concurrent
        self.semaphores = {}  # 存放每个env的信号量

    async def get_connection(self, env: str, database: str = None, schema: str = None) -> Union[aiomysql.Connection, asyncpg.Pool]:
        """
        获取数据库连接
        :param env: 环境名称
        :param database: 要使用的数据库名，如果为None则使用配置中的默认值
        :param schema: 要使用的schema名（仅PostgreSQL），如果为None则使用配置中的默认值
        :return: MySQL返回connection，PostgreSQL返回pool
        """
        if env not in self.configs:
            raise ValueError(f"配置文件中不存在名为 '{env}' 的数据库配置")
        
        config = self.configs[env]
        db_type = config.get('type', 'mysql')
        
        # 确保连接池存在
        if env not in self.pools:
            await self._create_pool(env)
        
        # 确保信号量存在
        if env not in self.semaphores:
            self.semaphores[env] = asyncio.Semaphore(self.max_concurrent)
        
        semaphore = self.semaphores[env]
        await semaphore.acquire()
        
        try:
            pool = self.pools[env]
            
            if db_type == 'mysql':
                conn = await asyncio.wait_for(pool.acquire(), timeout=self.connect_timeout)
                if database:
                    async with conn.cursor() as cursor:
                        await cursor.execute(f"USE {database}")
                return conn
            
            elif db_type == 'postgresql':
                return pool  # 返回连接池，让使用者通过 async with 管理连接
            
            else:
                raise ValueError(f"不支持的数据库类型: {db_type}")
                
        except Exception as e:
            semaphore.release()
            self.logger.error(f"获取连接失败: {str(e)}")
            raise e

    async def _create_pool(self, env: str):
        """创建连接池"""
        config = self.configs[env]
        db_type = config.get('type', 'mysql')
        retry = 0

        while retry < self.max_retry:
            try:
                if db_type == 'mysql':
                    self.pools[env] = await aiomysql.create_pool(
                        host=config['host'],
                        port=config.get('port', 3306),
                        user=config['user'],
                        password=config['password'],
                        db=config.get('default_db', 'mysql'),
                        charset=config.get('charset', 'utf8mb4'),
                        autocommit=True,
                        minsize=1,
                        maxsize=5,
                        cursorclass=aiomysql.DictCursor
                    )
                elif db_type == 'postgresql':
                    self.pools[env] = await asyncpg.create_pool(
                        host=config['host'],
                        port=config.get('port', 5432),
                        user=config['user'],
                        password=config['password'],
                        database=config.get('default_db', 'postgres'),
                        min_size=1,
                        max_size=5,
                        command_timeout=60.0,
                        server_settings={
                            'application_name': 'auto_scripts',
                            'timezone': 'UTC',
                            'client_encoding': 'UTF8',
                            'search_path': config.get('default_schema', 'public')
                        }
                    )
                else:
                    raise ValueError(f"不支持的数据库类型: {db_type}")
                
                self.logger.info(f"成功创建连接池: {env} ({db_type})")
                return
            except Exception as e:
                retry += 1
                self.logger.warning(f"创建连接池 {env} ({db_type}) 失败, 第 {retry}/{self.max_retry} 次重试...")
                self.logger.error(f"错误详情: {str(e)}")
                await asyncio.sleep(1)

        raise ConnectionError(f"无法连接到数据库 {env} ({db_type})")

    async def release_connection(self, env: str, conn):
        """
        释放数据库连接
        :param env: 数据库环境名称
        :param conn: 数据库连接对象
        """
        if env not in self.configs:
            return
        
        config = self.configs[env]
        db_type = config.get('type', 'mysql')

        try:
            if db_type == 'mysql':
                if env in self.pools:
                    self.pools[env].release(conn)
                    self.logger.info(f"已释放MySQL连接: {env}")
            elif db_type == 'postgresql':
                # PostgreSQL的连接由async with自动管理，这里不需要手动释放
                pass
            else:
                raise ValueError(f"不支持的数据库类型: {db_type}")
        except Exception as e:
            self.logger.error(f"释放连接时出错: {str(e)}")
        finally:
            if env in self.semaphores:
                self.semaphores[env].release()

    async def close_all(self):
        """关闭所有连接池"""
        close_timeout = 30  # 设置关闭超时时间为30秒
        
        for env, pool in self.pools.items():
            try:
                config = self.configs[env]
                db_type = config.get('type', 'mysql')
                
                if db_type == 'mysql':
                    pool.close()
                    await asyncio.wait_for(pool.wait_closed(), timeout=close_timeout)
                elif db_type == 'postgresql':
                    await asyncio.wait_for(pool.close(), timeout=close_timeout)
                
                self.logger.info(f"数据库连接池已关闭: {env} ({db_type})")
            except asyncio.TimeoutError:
                self.logger.error(f"关闭连接池超时: {env}")
            except Exception as e:
                self.logger.error(f"关闭连接池时出错: {env}, {str(e)}")

# -------------------------------------------------------
# 测试(test)
# -------------------------------------------------------
async def main():
    logger = setup_logger(__file__)
    db_manager = DBManager(logger=logger)

    try:
        # 测试MySQL连接
        conn_mysql = await db_manager.get_connection('zcwDB_Alicloud')
        try:
            async with conn_mysql.cursor() as cursor:
                await cursor.execute("SELECT NOW()")
                result = await cursor.fetchone()
                logger.info(f"[MySQL] 查询结果: {result}")
        finally:
            await db_manager.release_connection('zcwDB_Alicloud', conn_mysql)

        # 测试PostgreSQL连接
        pool = await db_manager.get_connection('zcwDWH_Alicloud')
        async with pool.acquire() as conn:
            result = await conn.fetchrow("SELECT NOW()")
            logger.info(f"[PostgreSQL] 查询结果: {result}")

    finally:
        await asyncio.wait_for(db_manager.close_all(), timeout=30)

if __name__ == '__main__':
    asyncio.run(main())