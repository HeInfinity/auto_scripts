import os
import sys

# 添加项目根目录到Python路径
def load_sys_path():
    """动态查找项目根目录并将其添加到sys.path"""
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

# 调用函数加载配置
load_sys_path()

import asyncio
import datetime
import pytz
from typing import Tuple, Optional

# 导入自定义模块
from modules.db_conn import DBManager
from modules.log_tools import setup_logger
from modules.access_token import create_token_manager

# 获取logger
logger = setup_logger(__file__)

async def save_token_to_database(db_manager: DBManager, access_token: str, requested_at: datetime.datetime, expires_at: datetime.datetime):
    """保存令牌到数据库"""
    try:
        conn = await db_manager.get_connection('myDB_Alicloud')
        try:
            async with conn.cursor() as cursor:
                # 首先删除所有现有记录
                await cursor.execute("DELETE FROM moxueyuan_token")
                # 然后插入新的 token 记录
                sql = "INSERT INTO moxueyuan_token (token, start_time, end_time) VALUES (%s, %s, %s)"
                await cursor.execute(sql, (
                    access_token,
                    requested_at.strftime('%Y-%m-%d %H:%M:%S'),
                    expires_at.strftime('%Y-%m-%d %H:%M:%S')
                ))
                await conn.commit()
                logger.info(f"成功将token保存到数据库, token有效期至: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logger.error(f"保存token时发生错误: {e}")
            raise
        finally:
            await db_manager.release_connection('myDB_Alicloud', conn)
    except Exception as e:
        logger.error(f"数据库操作失败: {e}")
        raise

async def get_stored_token(db_manager: DBManager, now_utc8: datetime.datetime) -> Tuple[Optional[str], Optional[datetime.datetime], Optional[datetime.datetime]]:
    """从数据库获取存储的令牌"""
    try:
        conn = await db_manager.get_connection('myDB_Alicloud')
        try:
            async with conn.cursor() as cursor:
                # 获取数据库中的token和end_time
                await cursor.execute("SELECT token, start_time, end_time FROM moxueyuan_token")
                result = await cursor.fetchone()
                
                if result:
                    token = result['token']
                    start_time = result['start_time']
                    end_time = result['end_time']
                    
                    # 确保end_time为UTC+8时区感知对象
                    tz = pytz.timezone('Asia/Shanghai')
                    end_time = tz.localize(end_time)
                    start_time = tz.localize(start_time)
                    
                    logger.info(f"从数据库获取到token记录, 有效期至: {end_time}")
                    
                    if now_utc8 <= end_time:
                        logger.info("数据库中的token仍然有效")
                        return token, start_time, end_time
                    else:
                        logger.info("数据库中的token已过期")
                else:
                    logger.info("数据库中未找到token记录")
                    
                return None, None, None
        finally:
            await db_manager.release_connection('myDB_Alicloud', conn)
    except Exception as e:
        logger.error(f"获取stored token时发生错误: {e}")
        return None, None, None

async def main():
    """主函数"""
    # 创建TokenManager实例
    token_manager = create_token_manager('moxueyuan', logger)
    db_manager = DBManager(logger=logger)
    
    try:
        # 获取当前UTC+8时间
        now_utc8 = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
        logger.info(f"当前时间(UTC+8): {now_utc8.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 先尝试从数据库获取token
        token, start_time, end_time = await get_stored_token(db_manager, now_utc8)
        
        if not token:
            # 如果没有有效的token则获取新的
            logger.info("开始获取新的token")
            token, start_time, end_time = await token_manager.get_token()
            await save_token_to_database(db_manager, token, start_time, end_time)
            
        logger.info("token处理完成！")
        
    except Exception as e:
        logger.error(f"处理token时发生错误: {e}")
    finally:
        await db_manager.close_all()

if __name__ == "__main__":
    asyncio.run(main())