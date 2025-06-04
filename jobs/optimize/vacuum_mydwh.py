import os
import sys
import yaml
import asyncio
import time
from datetime import datetime
from typing import Dict, List

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

# 调用函数加载配置
load_sys_path()
from modules.db_conn import DBManager
from modules.log_tools import setup_logger

# 获取logger
logger = setup_logger(__file__)

def load_tables_from_yaml(config_subpath='auto_scripts/sql/config/daily_dwh_query.yaml') -> Dict[str, Dict]:
    """
    从yaml配置文件中加载需要优化的表信息
    :param config_subpath: 配置文件相对于项目根目录的路径
    :return: 表配置信息字典
    """
    # 获取项目根目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.basename(current_dir) == 'Python':
            break
        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            raise RuntimeError("无法找到包含目录 'Python' 的项目根目录")
        current_dir = new_dir
    
    # 加载配置文件
    config_path = os.path.join(current_dir, config_subpath)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        configs = yaml.safe_load(f)
    
    return configs.get('daily_dwh_query', {})

async def vacuum_tables():
    """
    对数据仓库表执行VACUUM ANALYZE操作
    VACUUM ANALYZE用于回收空间并更新统计信息, 这有助于查询优化器生成更好的执行计划
    """
    tables_config = load_tables_from_yaml()
    start_time = time.time()
    
    # 创建数据库管理器
    db_manager = DBManager(logger=logger)
    pool = None
    
    try:
        total_tables = len(tables_config)
        logger.info(f"准备对 {total_tables} 张表执行VACUUM ANALYZE操作")
        
        # 获取连接池
        pool = await db_manager.get_connection('myDWH_Tencent')
        
        # 遍历每个表, 逐个优化
        for index, (table_name, table_info) in enumerate(tables_config.items(), 1):
            table_start_time = time.time()
            try:
                # 从连接池获取一个连接, 并设置为原始模式（不自动管理事务）
                async with pool.acquire() as conn:
                    schema = table_info.get('schema', 'zcwhr_dwh')  # 从配置中获取schema
                    # 设置search_path
                    await conn.execute(f'SET search_path TO {schema}')
                    
                    logger.info(f"[{index}/{total_tables}] 开始对表 {schema}.{table_name} 执行VACUUM ANALYZE")
                    sql = f'VACUUM ANALYZE "{table_name}";'  # 由于已设置search_path, 这里不需要schema前缀
                    await conn.execute(sql)
                
                table_end_time = time.time()
                logger.info(f"[{index}/{total_tables}] 完成表 {schema}.{table_name} 的VACUUM ANALYZE操作, 耗时 {table_end_time - table_start_time:.2f} 秒")
                await asyncio.sleep(3)  # 等待3秒, 避免对数据库造成过大压力
                
            except Exception as e:
                logger.error(f"[{index}/{total_tables}] 对表 {table_name} 执行VACUUM ANALYZE时出错: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"执行VACUUM ANALYZE操作时发生错误: {str(e)}")
    finally:
        end_time = time.time()
        logger.info(f"所有表的VACUUM ANALYZE操作完成, 总计耗时 {end_time - start_time:.2f} 秒")
        await db_manager.close_all()

async def main():
    """主函数"""
    logger.info("开始执行数据仓库表VACUUM ANALYZE操作")
    await vacuum_tables()
    logger.info("数据仓库表VACUUM ANALYZE操作执行完成")

if __name__ == "__main__":
    asyncio.run(main()) 