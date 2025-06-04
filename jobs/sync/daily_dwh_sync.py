import asyncio
import aiomysql
import asyncpg
import pytz
from datetime import datetime, timedelta
import os
import sys
import yaml
from typing import Dict, Any, List, Optional
import warnings

# 忽略 VALUES 函数的警告
warnings.filterwarnings('ignore', message='.*VALUES function.*')

# 更新数据的时间变量
days_offset = 0  # 0表示今天, 1表示昨天, 以此类推; 表示日期末位置
days_interval = 1  # 1表示间隔1天, 2表示间隔2天, 以此类推; 表示日期始位置

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
        os.path.join(project_root, 'auto_scripts', 'modules')
    ]
    
    # 添加路径并确保唯一性
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.append(path)

# 调用函数加载配置
load_sys_path()
from directory import SCRIPT_DIR
from log_tools import setup_logger
from db_conn import DBManager

# 获取logger
logger = setup_logger(__file__)

# 初始化数据库管理器
db_manager = DBManager(logger=logger, max_retry=3, connect_timeout=20, max_concurrent=5)

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

def load_query_config():
    """加载查询配置文件"""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(project_root, 'sql', 'config', 'daily_dwh_query.yaml')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)['daily_dwh_query']

async def check_table_exists(conn: asyncpg.Connection, schema: str, table_name: str) -> bool:
    """
    检查表是否存在
    :param conn: 数据库连接
    :param schema: schema名称
    :param table_name: 表名
    :return: 表是否存在
    """
    # 检查schema是否存在
    schema_exists = await conn.fetchval("""
        SELECT EXISTS(
            SELECT 1 FROM pg_namespace WHERE nspname = $1
        )
    """, schema)
    
    if not schema_exists:
        raise ValueError(f"Schema '{schema}' 不存在")
        
    # 检查表是否存在
    exists = await conn.fetchval("""
        SELECT EXISTS(
            SELECT 1 
            FROM pg_catalog.pg_class c 
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = $1 
            AND c.relname = $2 
            AND c.relkind = 'r'
        )
    """, schema, table_name)
    
    return exists

async def insert_new_data(conn: asyncpg.Connection, table_name: str, data: List[Dict], schema: str) -> float:
    """
    将新数据插入到数仓的指定表中
    :param conn: 数据库连接
    :param table_name: 表名
    :param data: 要插入的数据
    :param schema: schema名称
    :return: 插入操作耗时（秒）
    """
    if not data:
        logger.info(f"无需插入新数据到 {table_name}")
        return 0

    logger.info(f"正在向 {table_name} 插入 {len(data)} 条新数据")
    start_time = datetime.now()

    try:
        # 准备插入语句
        columns = list(data[0].keys())
        values_str = ','.join([f'${i+1}' for i in range(len(columns))])
        insert_query = f"""
        INSERT INTO {schema}.{table_name} ({', '.join(columns)})
        VALUES ({values_str})
        """

        # 分批插入数据
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            values = [[row[col] for col in columns] for row in batch]
            await conn.executemany(insert_query, values)
            logger.info(f"已插入 {len(batch)} 条数据到 {table_name}")

        insert_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"表 {table_name} 插入数据花费时间: {insert_duration:.2f} 秒")
        return insert_duration
    except Exception as e:
        logger.error(f"插入数据时出错: {str(e)}")
        raise

async def update_existing_data(conn: asyncpg.Connection, table_name: str, data: List[Dict], 
                             unique_columns: List[str], schema: str) -> float:
    """
    更新数仓中指定表的现有数据
    :param conn: 数据库连接
    :param table_name: 表名
    :param data: 要更新的数据
    :param unique_columns: 唯一键列名列表
    :param schema: schema名称
    :return: 更新操作耗时（秒）
    """
    if not data:
        logger.info(f"无需更新 {table_name} 中的数据")
        return 0

    logger.info(f"正在更新 {table_name} 中的 {len(data)} 条数据")
    start_time = datetime.now()

    try:
        # 准备更新语句
        columns = list(data[0].keys())
        update_columns = [col for col in columns if col not in unique_columns]

        if not update_columns:
            logger.info(f"没有需要更新的列")
            return 0

        # 构建ON CONFLICT语句
        conflict_target = ', '.join(unique_columns)
        set_values = ', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])
        
        values_str = ','.join([f'${i+1}' for i in range(len(columns))])
        upsert_query = f"""
        INSERT INTO {schema}.{table_name} ({', '.join(columns)})
        VALUES ({values_str})
        ON CONFLICT ({conflict_target})
        DO UPDATE SET {set_values}
        """

        # 分批处理数据
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            values = [[row[col] for col in columns] for row in batch]
            await conn.executemany(upsert_query, values)
            logger.info(f"已处理 {len(batch)} 条数据")

        update_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"表 {table_name} 更新数据花费时间: {update_duration:.2f} 秒")
        return update_duration
    except Exception as e:
        logger.error(f"更新数据时出错: {str(e)}")
        raise

async def main():
    """
    主函数，负责从个人数据库读取数据，根据唯一标识分为插入和更新数据，然后同步到数仓
    """
    total_start_time = datetime.now()

    # Step 1: 加载配置信息
    table_info = []
    config = load_query_config()
    project_root = find_project_root()
    
    for table_name, table_config in config.items():
        table_info.append({
            'table_name': table_name,
            'id_column': table_config['compare_columns'],
            'unique_columns': table_config['unique_columns'],
            'sql_file': table_config['sql_file'],
            'conn': table_config['conn'],
            'db': table_config['db'],
            'schema': table_config['schema']
        })

    try:
        # Step 2: 创建数仓数据库连接
        dwh_conn = await db_manager.get_connection('myDWH_Tencent')
        logger.info("成功连接到数仓数据库")
        
        # Step 3: 循环处理每个表
        for table in table_info:
            table_start_time = datetime.now()
            
            table_name = table['table_name']
            id_column = table['id_column']
            unique_columns = table['unique_columns']
            sql_file = table['sql_file']
            schema = table['schema']
            
            logger.info(f"正在处理表 {table_name}")
            
            try:
                # Step 4: 检查表是否存在
                async with dwh_conn.acquire() as conn:
                    table_exists = await check_table_exists(conn, schema, table_name)
                    if not table_exists:
                        logger.error(f"表 {schema}.{table_name} 不存在，跳过处理")
                        continue
                        
                    logger.info(f"表 {schema}.{table_name} 存在，继续处理")
                    
                    # Step 5: 从数仓获取最大 id_column 值
                    await conn.execute(f'SET search_path TO {schema}')
                    result = await conn.fetchrow(f"SELECT MAX({id_column}) AS max_id FROM {schema}.{table_name}")
                    max_id = result['max_id'] if result['max_id'] else 0
                    logger.info(f"表 {table_name} 中 {id_column} 的最大值为: {max_id}")

                # Step 6: 设置时间范围
                tz = pytz.timezone("Asia/Shanghai")
                end_date = (datetime.now(tz) - timedelta(days=days_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
                start_date = end_date - timedelta(days=days_interval)
                logger.info(f"表 {table_name} 同步时间范围: {start_date} 至 {end_date}")

                # Step 7: 从SQL文件读取查询
                sql_file_path = os.path.join(project_root, 'auto_scripts', 'sql', sql_file)
                if not os.path.exists(sql_file_path):
                    logger.error(f"SQL文件不存在: {sql_file_path}")
                    continue
                    
                logger.info(f"正在读取SQL文件: {sql_file_path}")
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    sql = f.read()

                # Step 8: 替换时间占位符
                sql = sql.replace('@start_date', f"'{start_date.strftime('%Y-%m-%d %H:%M:%S')}'")
                sql = sql.replace('@end_date', f"'{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")
                logger.info(f"个人数据库正在执行 SQL 查询: {sql_file_path}")

                # Step 9: 在个人数据库执行SQL查询
                sql_start_time = datetime.now()
                try:
                    mydb_conn = await db_manager.get_connection('myDB_Alicloud')
                    async with mydb_conn.cursor() as cursor:
                        await cursor.execute(sql)
                        results = await cursor.fetchall()
                    sql_duration = (datetime.now() - sql_start_time).total_seconds()
                    logger.info(f"从个人数据库获取了 {len(results)} 条记录")
                    logger.info(f"从个人数据库 SQL 查询花费时间: {sql_duration:.2f} 秒")
                finally:
                    await db_manager.release_connection('myDB_Alicloud', mydb_conn)

                # Step 10: 将结果分类为插入和更新的数据
                insert_data = []
                update_data = []

                for row in results:
                    if row[id_column] > max_id:
                        insert_data.append(row)
                    else:
                        update_data.append(row)

                logger.info(f"表 {table_name} 需要插入的记录数: {len(insert_data)}, 需要更新的记录数: {len(update_data)}")

                # Step 11: 更新和插入数据
                async with dwh_conn.acquire() as conn:
                    # 更新现有数据
                    update_duration = await update_existing_data(conn, table_name, update_data, unique_columns, schema)
                    
                    # 插入新数据
                    insert_duration = await insert_new_data(conn, table_name, insert_data, schema)

                    total_db_duration = update_duration + insert_duration
                    logger.info(f"表 {table_name} 更新和插入总时间: {total_db_duration:.2f} 秒")

                # Step 12: 计算表的执行时间
                table_duration = (datetime.now() - table_start_time).total_seconds()
                logger.info(f"表 {table_name} 总计花费时间: {table_duration:.2f} 秒")

            except Exception as e:
                logger.error(f"处理表 {schema}.{table_name} 时出错: {str(e)}")
                continue

    finally:
        # Step 13: 关闭数据库连接
        await db_manager.close_all()
        total_duration = (datetime.now() - total_start_time).total_seconds()
        logger.info(f"数据库连接已关闭")
        logger.info(f"代码总执行时间: {total_duration:.2f} 秒")

# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())