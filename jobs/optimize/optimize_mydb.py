import os
import sys
import yaml
import asyncio
import time
from datetime import datetime
from typing import List

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

def load_tables_from_yaml(config_subpath='auto_scripts/sql/config/daily_database_query.yaml') -> List[str]:
    """
    从yaml配置文件中加载需要优化的表名列表
    :param config_subpath: 配置文件相对于项目根目录的路径
    :return: 表名列表
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
        # 如果配置文件不存在, 返回默认表列表
        return [
            'tags', 'orders', 'tagtypes', 'orderitems', 'categories',
            'orderreturns', 'delivercenters', 'deliveryreceiptitems',
            'regionals', 'deliveryreceipts', 'deliveryroutes', 'stores',
            'storetags', 'visitrecorditems', 'products', 'visitrecords',
            'subsidiaries', 'largecsfollowups', 'employeeroles', 'employees'
        ]
    
    with open(config_path, 'r', encoding='utf-8') as f:
        configs = yaml.safe_load(f)
    
    # 获取所有表名
    tables = list(configs['daily_database_query'].keys())
    return tables

async def optimize_tables():
    """对数据库表执行OPTIMIZE操作
    OPTIMIZE TABLE用于重建表和索引, 可以回收未使用的空间并整理数据文件
    """
    tables = load_tables_from_yaml()
    start_time = time.time()
    
    # 创建数据库管理器
    db_manager = DBManager(logger=logger)
    
    try:
        total_tables = len(tables)
        logger.info(f"准备对 {total_tables} 张表执行OPTIMIZE操作")
        
        # 遍历每个表, 逐个优化
        for index, table in enumerate(tables, 1):
            table_start_time = time.time()
            try:
                # 获取数据库连接
                conn = await db_manager.get_connection('myDB_Alicloud')
                try:
                    async with conn.cursor() as cursor:
                        logger.info(f"[{index}/{total_tables}] 开始对表 {table} 执行OPTIMIZE")
                        sql = f"OPTIMIZE TABLE `{table}`;"
                        await cursor.execute(sql)
                finally:
                    await db_manager.release_connection('myDB_Alicloud', conn)
                
                table_end_time = time.time()
                logger.info(f"[{index}/{total_tables}] 完成表 {table} 的OPTIMIZE操作, 耗时 {table_end_time - table_start_time:.2f} 秒")
                await asyncio.sleep(3)  # 等待3秒
                
            except Exception as e:
                logger.error(f"[{index}/{total_tables}] 对表 {table} 执行OPTIMIZE时出错: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"执行OPTIMIZE操作时发生错误: {str(e)}")
    finally:
        end_time = time.time()
        logger.info(f"所有表的OPTIMIZE操作完成, 总计耗时 {end_time - start_time:.2f} 秒")
        await db_manager.close_all()

async def main():
    """主函数"""
    logger.info("开始执行数据库表OPTIMIZE操作")
    await optimize_tables()
    logger.info("数据库表OPTIMIZE操作执行完成")

if __name__ == "__main__":
    asyncio.run(main())