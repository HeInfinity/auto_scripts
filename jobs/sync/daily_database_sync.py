import asyncio
import aiomysql
import time
from datetime import datetime, timedelta
import sys
import os
import warnings
import functools
import logging
from typing import TypeVar, Callable, Any, Coroutine
import yaml

# 忽略 VALUES 函数的警告
warnings.filterwarnings('ignore', message='.*VALUES function.*')

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
# 导入数据库连接管理器和日志工具
from db_conn import DBManager
from log_tools import setup_logger

# 获取logger
logger = setup_logger(__file__)

# 更新数据的时间变量, 都表示日期间隔: 使用今日(CURRENT_DATE)减去日期间隔. 更新的数据都是昨天及之前的数据
# CURRENT_DATE返回的是今日零时
# @end_date = CURRENT_DATE - days_offset, 默认值0
days_offset = 0 # 表示日期末位置; 0表示今天(间隔0天:今日零时), 1表示昨天(间隔1天:昨天零时);
# @start_date = CURRENT_DATE - days_interval, 默认值1
days_interval = 1 # 表示日期始位置; 所以相比days_offset要多往前进1天.
# @filter_date = CURRENT_DATE - days_updated, 默认值45
days_updated = 45 # 与days_interval的作用相同, 表示日期始位置, 只是在queries_large_table中筛选特定日期区间以减少数据查询量

# 初始化数据库管理器
db_manager = DBManager(logger=logger, max_retry=3, connect_timeout=20, max_concurrent=5)

# 获取公司数据库连接(zcwDB_Alicloud)
async def get_company_connection():
    """
    获取公司数据库连接(zcwDB_Alicloud)
    """
    return await db_manager.get_connection('zcwDB_Alicloud')

# 获取个人数据库连接(myDB_Alicloud)
async def get_personal_connection():
    """
    获取个人数据库连接(myDB_Alicloud)
    """
    return await db_manager.get_connection('myDB_Alicloud')

T = TypeVar('T')

def async_timeout(timeout: int) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    异步函数超时装饰器
    :param timeout: 超时时间(秒)
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Function {func.__name__} timed out after {timeout} seconds")
        return wrapper
    return decorator

# sync_large_table_step1: 从公司数据库获取在updateAt中所有 createdAt 日期的数据
@async_timeout(45)  # 设置45秒超时
async def fetch_dates_with_updates(conn, query, table_name):
    try:
        async with conn.cursor() as cursor:
            # 使用 days_interval 和 days_offset 设置时间
            await cursor.execute(f"SET @start_date = CURRENT_DATE - INTERVAL {days_interval} DAY;")
            await cursor.execute(f"SET @end_date = CURRENT_DATE - INTERVAL {days_offset} DAY;")
            await cursor.execute(f"SET @filter_date = CURRENT_DATE - INTERVAL {days_updated} DAY;")
            
            # 执行查询
            await cursor.execute(query)
            dates = await cursor.fetchall()
            
            # 返回创建日期列表
            return [row['createdAt'].strftime('%Y-%m-%d') for row in dates]
    except TimeoutError:
        logger.error(f"{table_name} 查询超时(60秒), 将重试连接")
        raise
    except Exception as e:
        logger.error(f"{table_name} 查询出错: {str(e)}")
        raise
    finally:
        await db_manager.release_connection('zcwDB_Alicloud', conn)

# sync_large_table_step2: 删除个人数据库中对应createdAt的数据
async def delete_existing_data(conn, table_name, dates):
    try:
        total_start_time = time.time()
        async with conn.cursor() as cursor:
            total_deleted_rows = 0
            for date in dates:
                date_condition = f"`createdAt` BETWEEN '{date} 00:00:00' AND '{date} 23:59:59'"
                delete_query = f"DELETE FROM {table_name} WHERE {date_condition}"
                logger.info(f"开始删除 {table_name} 中日期 {date} 的数据")
                start_time = time.time()
                await cursor.execute(delete_query)
                deleted_rows = cursor.rowcount
                elapsed_time = time.time() - start_time
                logger.info(f"删除 {table_name} 中日期 {date} 的数据完成, 共删除 {deleted_rows} 行, 耗时 {elapsed_time:.2f} 秒")
                total_deleted_rows += deleted_rows
        total_elapsed_time = time.time() - total_start_time
        logger.info(f"{table_name} 删除了 {total_deleted_rows} 行数据, 删除总计耗时 {total_elapsed_time:.2f} 秒")
        return total_deleted_rows
    except Exception as e:
        logger.error(f"删除 {table_name} 中的数据时发生错误信息: {e}")
        return 0
    finally:
        await db_manager.release_connection('myDB_Alicloud', conn)

# sync_large_table_step3: 公司数据库异步获取每个createdAt的数据
async def fetch_and_collect_data_with_retry_by_date(table_name, date, data_query_template, all_data_by_date, semaphore):
    max_retries = 5
    retry_count = 0
    while retry_count <= max_retries:
        conn = None
        async with semaphore:
            try:
                logger.info(f"开始获取 {table_name} 中日期 {date} 的数据")
                if table_name == "deliveryreceiptitems":
                    date_conditions = f"(`createdAt` BETWEEN '{date} 00:00:00' AND '{date} 23:59:59' AND sort_time BETWEEN @filter_date AND @end_date)"
                elif table_name == "orders":
                    date_conditions = f"(`createdAt` BETWEEN '{date} 00:00:00' AND '{date} 23:59:59' AND `status` IN ('CONFIRMED', 'DELIVERED', 'DONE', 'RECEIVED'))"
                else:
                    date_conditions = f"(`createdAt` BETWEEN '{date} 00:00:00' AND '{date} 23:59:59')"
                data_query = data_query_template.format(date_conditions=date_conditions)
                conn = await get_company_connection()
                start_time = time.time()
                async with conn.cursor() as cursor:
                    await cursor.execute(f"SET @start_date = CURRENT_DATE - INTERVAL {days_interval} DAY;")
                    await cursor.execute(f"SET @end_date = CURRENT_DATE - INTERVAL {days_offset} DAY;")
                    await cursor.execute(f"SET @filter_date = CURRENT_DATE - INTERVAL {days_updated} DAY;")
                    await cursor.execute(data_query)
                    data = await cursor.fetchall()
                    row_count = len(data)
                query_time = time.time() - start_time
                if row_count > 0:
                    logger.info(f"{table_name} 中日期 {date} 获取成功, 行数: {row_count}, 耗时: {query_time:.2f}秒")
                    all_data_by_date[date] = data
                else:
                    logger.info(f"{table_name} 中日期 {date} 没有数据")
                return  # 成功后直接返回
            except aiomysql.MySQLError as e:
                logger.error(f"查询 {table_name} 日期 {date} 时失败, 错误信息: {e}")
                retry_count += 1
                if retry_count >= 3:
                    logger.error(f"{table_name} 日期 {date} 错误次数达到 {retry_count} 次, 等待 5 秒后重试")
                    await asyncio.sleep(5)
                else:
                    logger.info(f"等待 3 秒后重试 {table_name} 日期 {date}")
                    await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"{table_name} 日期 {date} 发生未知错误信息: {e}")
                import traceback
                traceback.print_exc()
                break
            finally:
                if conn:
                    await db_manager.release_connection('zcwDB_Alicloud', conn)
    # 失败时不抛异常, 直接返回

# sync_large_table_step4: 从公司数据库插入所有createdAt的数据到个人数据库
async def insert_data_by_date(table_name, data_by_date):
    total_inserted = 0
    total_start_time = time.time()
    logger.info(f"准备插入 {table_name}, 共 {len(data_by_date)} 个日期批次")
    
    for date, data in data_by_date.items():
        data_length = len(data)
        if data_length == 0:
            continue
            
        # 确定是否需要分批插入(根据数据量)
        batch_size = 10000 if data_length > 10000 else data_length
        batches_count = (data_length + batch_size - 1) // batch_size  # 向上取整
        
        logger.info(f"{table_name} 日期 {date} 数据量: {data_length}行, 分 {batches_count} 批插入")
        
        date_inserted = 0
        for i in range(0, data_length, batch_size):
            batch_data = data[i:i + batch_size]
            batch_length = len(batch_data)
            retries = 0
            max_retries = 3
            
            while retries <= max_retries:
                conn = None
                try:
                    batch_start_time = time.time()
                    logger.info(f"插入 {table_name} 批次 {i//batch_size + 1}/{batches_count}, 行数: {batch_length}")
                    
                    conn = await get_personal_connection()
                    async with conn.cursor() as cursor:
                        insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(batch_data[0].keys())})
                        VALUES ({', '.join(['%s'] * len(batch_data[0]))})
                        """
                        await cursor.executemany(insert_query, [tuple(row.values()) for row in batch_data])
                    await conn.commit()
                    
                    batch_time = time.time() - batch_start_time
                    date_inserted += batch_length
                    total_inserted += batch_length
                    
                    logger.info(f"{table_name} 批次 {i//batch_size + 1}/{batches_count} 插入成功, 耗时: {batch_time:.2f}秒, 进度: {date_inserted}/{data_length}")
                    break  # 当前批次成功, 退出重试循环
                    
                except aiomysql.MySQLError as e:
                    logger.error(f"插入 {table_name} 批次 {i//batch_size + 1}/{batches_count} 失败, MySQL错误: {e}")
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"{table_name} 中日期 {date} 的批次 {i//batch_size + 1} 跳过")
                        break
                    else:
                        logger.info(f"重试插入 {table_name} 批次 {i//batch_size + 1}, 暂停 5 秒后重试")
                        await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"插入 {table_name} 批次 {i//batch_size + 1}/{batches_count} 时发生未知错误: {e}")
                    import traceback
                    traceback.print_exc()
                    break
                finally:
                    if conn:
                        await db_manager.release_connection('myDB_Alicloud', conn)
    
    total_time = time.time() - total_start_time
    logger.info(f"{table_name} 所有数据插入完成, 总插入行数: {total_inserted}, 总耗时: {total_time:.2f}秒")
    return total_inserted

# sync_large_table: 处理查询和数据同步任务带重试机制
async def sync_large_table(table_name, date_query, data_query_template, semaphore):
    retries = 0
    max_retries = 3
    
    async with semaphore:
        while retries <= max_retries:
            try:
                total_start_time = time.time()  # 记录整个同步过程的开始时间
                logger.info(f"开始处理表: {table_name}, 第 {retries + 1} 次尝试")

                # Step 1: 从公司数据库获取在updateAt中所有 createdAt 日期的数据
                start_dates_query_time = time.time()
                conn1 = await get_company_connection()
                try:
                    dates = await fetch_dates_with_updates(conn1, date_query, table_name)
                    dates_query_time = time.time() - start_dates_query_time
                except TimeoutError:
                    logger.error(f"{table_name} Step 1 超时, 等待10秒后进行第 {retries + 2} 次重试")
                    retries += 1
                    if retries <= max_retries:
                        await asyncio.sleep(10)
                        continue
                    else:
                        logger.error(f"{table_name} 已达到最大重试次数 {max_retries}, 处理失败")
                        return False

                if not dates:
                    logger.info(f"{table_name} 没有需要同步的数据")
                    return True

                logger.info(f"{table_name} 需要同步的日期有: {', '.join(dates)}")

                # Step 2: 删除个人数据库中对应createdAt的数据
                start_delete_time = time.time()
                conn2 = await get_personal_connection()
                await delete_existing_data(conn2, table_name, dates)
                delete_time = time.time() - start_delete_time

                # Step 3: 从公司数据库异步获取每个createdAt的数据
                start_data_query_time = time.time()
                all_data_by_date = {}
                tasks = []

                date_semaphore = asyncio.Semaphore(1)  # 允许同时处理多个日期, 但限制并发数
                for date in dates:
                    task = asyncio.create_task(fetch_and_collect_data_with_retry_by_date(
                        table_name, date, data_query_template, all_data_by_date, date_semaphore))
                    tasks.append(task)

                await asyncio.gather(*tasks)

                total_row_count = sum(len(data) for data in all_data_by_date.values())
                data_query_time = time.time() - start_data_query_time

                # Step 4: 从公司数据库插入所有createdAt的数据到个人数据库
                start_insert_time = time.time()
                inserted_rows = await insert_data_by_date(table_name, all_data_by_date)
                insert_time = time.time() - start_insert_time
                total_sync_time = time.time() - total_start_time

                # 计算每个步骤的耗时百分比
                total_time_str = f"总耗时: {total_sync_time:.2f} 秒"
                steps_time_str = (
                    f"其中日期查询: {dates_query_time:.2f} 秒 ({(dates_query_time/total_sync_time*100):.1f}%), "
                    f"数据删除: {delete_time:.2f} 秒 ({(delete_time/total_sync_time*100):.1f}%), "
                    f"数据查询: {data_query_time:.2f} 秒 ({(data_query_time/total_sync_time*100):.1f}%), "
                    f"数据插入: {insert_time:.2f} 秒 ({(insert_time/total_sync_time*100):.1f}%)"
                )
                logger.info(f"{table_name} 数据同步完成, {total_time_str} ({steps_time_str}), 总查询到的行数: {total_row_count} 行, 插入的行数: {inserted_rows} 行")

                # Step 5: 手动释放内存
                del all_data_by_date
                logger.info(f"{table_name} 内存已释放, 处理完成")
                return True

            except aiomysql.MySQLError as e:
                logger.error(f"处理 {table_name} 时发生 MySQL 错误信息: {e}")
                retries += 1
            except Exception as e:
                logger.error(f"处理 {table_name} 时发生未捕获的错误信息: {e}")
                retries += 1

            if retries <= max_retries:
                logger.info(f"表 {table_name} 处理失败, 等待 10 秒后重试 (第 {retries} 次)")
                await asyncio.sleep(10)
            else:
                logger.error(f"表 {table_name} 重试次数已达到最大限制 ({max_retries}) 次, 放弃处理")
                return False

# sync_small_table_step1: 从公司数据库中查询需要更新的日期(updatedAt)
async def fetch_data(conn, query, table_name):
    start_time = time.time()  # 记录查询开始时间
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(f"SET @start_date = CURRENT_DATE - INTERVAL {days_interval} DAY;")
            await cursor.execute(f"SET @end_date = CURRENT_DATE - INTERVAL {days_offset} DAY;")
            await cursor.execute(f"SET @filter_date = CURRENT_DATE - INTERVAL {days_updated} DAY;")
            await cursor.execute(query)
            data = await cursor.fetchall()
            row_count = len(data)
    finally:
        end_time = time.time()  # 记录查询结束时间
        query_time = end_time - start_time  # 计算查询用时
    return data, row_count, query_time

# sync_small_table_step2: 插入或更新个人数据库的数据(注: 使用了警告忽略)
async def upsert_data(table_name, data):
    total_inserted = 0
    batch_size = 10000  # 根据需要调整批次大小
    data_length = len(data)
    for i in range(0, data_length, batch_size):
        batch_data = data[i:i + batch_size]
        retries = 0
        max_retries = 3
        while retries <= max_retries:
            conn = None
            try:
                conn = await get_personal_connection()
                async with conn.cursor() as cursor:
                    # 构建插入语句, 明确指定字段名
                    columns = ', '.join(batch_data[0].keys())
                    placeholders = ', '.join(['%s'] * len(batch_data[0]))
                    
                    # 只根据 id 字段进行重复检查, 忽略其他字段
                    updates = ', '.join([f'{key} = VALUES({key})' for key in batch_data[0].keys() if key != 'id'])
                    
                    # 插入语句, 只通过 id 确保唯一性, 避免其他字段(如 phone)的冲突
                    insert_query = f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {updates}
                    """
                    await cursor.executemany(insert_query, [tuple(row.values()) for row in batch_data])
                await conn.commit()
                total_inserted += len(batch_data)
                break  # 当前批次成功, 退出重试循环
            except aiomysql.MySQLError as e:
                logger.error(f"在表 {table_name} 中同步数据时发生 MySQL 错误信息: {e}")
                retries += 1
                if retries > max_retries:
                    logger.error(f"表 {table_name} 中同步数据超过最大重试次数, 跳过当前批次")
                    break
                else:
                    logger.info(f"表 {table_name} 重试插入批次 {i // batch_size + 1}, 暂停 5 秒后重试")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"在表 {table_name} 中同步数据时发生未知错误信息: {e}")
                break
            finally:
                if conn:
                    await db_manager.release_connection('myDB_Alicloud', conn)
    return total_inserted

# sync_small_table: 处理查询和数据同步任务
async def sync_small_table(table_name, query, semaphore):
    async with semaphore:
        try:
            total_start_time = time.time()  # 记录整个同步过程的开始时间
            logger.info(f"开始处理表: {table_name}")
            
            # Step 1: 从公司数据库中查询数据
            start_query_time = time.time()
            conn1 = await get_company_connection()
            data, row_count, query_time = await fetch_data(conn1, query, table_name)
            await db_manager.release_connection('zcwDB_Alicloud', conn1)
            query_time = time.time() - start_query_time

            if row_count > 0:
                # Step 2: 删除旧数据
                start_delete_time = time.time()
                conn2 = await get_personal_connection()
                async with conn2.cursor() as cursor:
                    await cursor.execute(f"DELETE FROM {table_name} WHERE updatedAt BETWEEN @start_date AND @end_date")
                await db_manager.release_connection('myDB_Alicloud', conn2)
                delete_time = time.time() - start_delete_time
                
                # Step 3: 插入新数据
                start_insert_time = time.time()
                inserted_rows = await upsert_data(table_name, data)
                insert_time = time.time() - start_insert_time
                
                if inserted_rows > 0:
                    total_time = time.time() - total_start_time
                    logger.info(f"{table_name} 数据同步完成, 总耗时: {total_time:.2f} 秒 "
                              f"(其中日期查询: {query_time:.2f} 秒 ({(query_time/total_time*100):.1f}%), "
                              f"数据删除: {delete_time:.2f} 秒 ({(delete_time/total_time*100):.1f}%), "
                              f"数据插入: {insert_time:.2f} 秒 ({(insert_time/total_time*100):.1f}%)), "
                              f"总查询到的行数: {row_count} 行, 同步的行数: {inserted_rows} 行")
                else:
                    logger.info(f"查询用时: {query_time:.2f} 秒, 总查询到的行数: {row_count}, 但是同步数据没有成功 {table_name}")
                    return False
            else:
                logger.info(f"查询用时: {query_time:.2f} 秒, 总查询到的行数: {row_count}, 没有数据需要同步, {table_name} 处理完成")

        except aiomysql.MySQLError as e:
            logger.error(f"处理 {table_name} 时发生 MySQL 错误信息: {e}")
            return False
        except Exception as e:
            logger.error(f"处理 {table_name} 时发生未捕获的错误信息: {e}")
            return False
        return True

# refresh_full_table: 处理全表刷新任务
async def refresh_full_table(table_name, query, semaphore):
    async with semaphore:
        try:
            total_start_time = time.time()  # 记录整个同步过程的开始时间
            logger.info(f"开始全量刷新表: {table_name}")

            # Step 1: 在目标数据库中使用 TRUNCATE TABLE 清空表
            start_delete_time = time.time()
            conn2 = await get_personal_connection()
            async with conn2.cursor() as cursor:
                await cursor.execute(f"TRUNCATE TABLE {table_name};")
            await db_manager.release_connection('myDB_Alicloud', conn2)
            delete_time = time.time() - start_delete_time
            logger.info(f"已清空 {table_name} 表的数据")

            # Step 2: 从源数据库中获取所有数据
            retries = 0
            max_retries = 3
            data = []
            start_query_time = time.time()
            while retries <= max_retries:
                conn1 = None
                try:
                    conn1 = await get_company_connection()
                    async with conn1.cursor() as cursor:
                        # 不需要执行时间范围的设置
                        await cursor.execute(query)
                        data = await cursor.fetchall()
                        row_count = len(data)
                    query_time = time.time() - start_query_time
                    break
                except aiomysql.MySQLError as e:
                    logger.error(f"查询 {table_name} 时发生 MySQL 错误信息: {e}")
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"超过最大重试次数, 放弃查询 {table_name}")
                        return False
                    else:
                        logger.info(f"重试查询 {table_name}, 暂停 5 秒后重试")
                        await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"查询 {table_name} 时发生未知错误信息: {e}")
                    return False
                finally:
                    if conn1:
                        await db_manager.release_connection('zcwDB_Alicloud', conn1)

            if not data:
                logger.info(f"未获取到任何数据, 跳过插入 {table_name}")
                return True

            # Step 3: 将数据插入目标数据库
            total_inserted = 0
            batch_size = 10000  # 根据需要调整批次大小
            data_length = len(data)
            start_insert_time = time.time()  # 开始插入的时间
            for i in range(0, data_length, batch_size):
                batch_data = data[i:i + batch_size]
                insert_retries = 0
                while insert_retries <= max_retries:
                    conn2 = None
                    try:
                        conn2 = await get_personal_connection()
                        async with conn2.cursor() as cursor:
                            columns = ', '.join(batch_data[0].keys())
                            placeholders = ', '.join(['%s'] * len(batch_data[0]))
                            insert_query = f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                            """
                            await cursor.executemany(insert_query, [tuple(row.values()) for row in batch_data])
                        await conn2.commit()
                        total_inserted += len(batch_data)
                        break
                    except aiomysql.MySQLError as e:
                        logger.error(f"插入 {table_name} 时发生 MySQL 错误信息: {e}")
                        insert_retries += 1
                        if insert_retries > max_retries:
                            logger.error(f"超过最大重试次数, 放弃插入 {table_name}")
                            return False
                        else:
                            logger.info(f"重试插入 {table_name}, 暂停 5 秒后重试")
                            await asyncio.sleep(5)
                    except Exception as e:
                        logger.error(f"插入 {table_name} 时发生未知错误信息: {e}")
                        return False
                    finally:
                        if conn2:
                            await db_manager.release_connection('myDB_Alicloud', conn2)
            insert_time = time.time() - start_insert_time  # 插入数据所用的时间
            total_time = time.time() - total_start_time

            logger.info(f"{table_name} 全量刷新完成, 总耗时: {total_time:.2f} 秒 "
                       f"(其中数据删除: {delete_time:.2f} 秒 ({(delete_time/total_time*100):.1f}%), "
                       f"数据查询: {query_time:.2f} 秒 ({(query_time/total_time*100):.1f}%), "
                       f"数据插入: {insert_time:.2f} 秒 ({(insert_time/total_time*100):.1f}%)), "
                       f"总查询到的行数: {data_length} 行, 插入的行数: {total_inserted} 行")
            return True

        except Exception as e:
            logger.error(f"全量刷新 {table_name} 时发生未捕获的错误信息: {e}")
            return False

# 主函数
async def main():
    start_time = time.time()
    
    # 计算同步的日期区间
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = current_date - timedelta(days=days_offset)
    start_date = current_date - timedelta(days=days_interval)
    
    logger.info("开始同步数据...")
    logger.info(f"同步日期区间: {start_date.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

    # 动态查找项目根目录
    def find_project_root(root_name='Python'):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while True:
            if os.path.basename(current_dir) == root_name:
                return current_dir
            new_dir = os.path.dirname(current_dir)
            if new_dir == current_dir:
                raise RuntimeError(f"无法找到包含目录 '{root_name}' 的项目根目录")
            current_dir = new_dir

    # 加载yaml配置
    def load_query_config():
        project_root = find_project_root()
        config_path = os.path.join(project_root, 'auto_scripts', 'sql', 'config', 'daily_database_query.yaml')
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"未找到配置文件: {config_path}")
        with open(config_path, 'r', encoding='utf-8') as f:
            configs = yaml.safe_load(f)
        return configs['daily_database_query']

    query_configs = load_query_config()
    queries_large_table = {}
    queries_small_table = {}
    queries_full_refresh_table = {}
    for table, conf in query_configs.items():
        if conf.get('type') == 'large_table':
            queries_large_table[table] = {
                'date_query': conf['queries']['date_query'],
                'data_query_template': conf['queries']['data_query']
            }
        elif conf.get('type') == 'small_table':
            queries_small_table[table] = conf['query']
        elif conf.get('type') == 'full_refresh':
            queries_full_refresh_table[table] = conf['query']

    failed_steps = []
    error_count = 0
    max_errors = 5
    
    # 限制同时处理的表数量, 防止过多并发
    semaphore = asyncio.Semaphore(1)

    tasks = []

    # 处理使用 sync_large_table 的表
    for table, config in queries_large_table.items():
        task = asyncio.create_task(
            sync_large_table(table, config['date_query'], config['data_query_template'], semaphore)
        )
        tasks.append(task)

    # 处理使用 sync_small_table 的表
    for table, query in queries_small_table.items():
        task = asyncio.create_task(
            sync_small_table(table, query, semaphore)
        )
        tasks.append(task)

    # 处理使用 refresh_full_table 的表
    for table, query in queries_full_refresh_table.items():
        task = asyncio.create_task(
            refresh_full_table(table, query, semaphore)
        )
        tasks.append(task)

    # 等待所有任务完成
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # 检查结果
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"任务 {i} 发生错误信息: {result}")
            error_count += 1
            if error_count >= max_errors:
                logger.error(f"错误次数达到最大限制, 停止处理")
                break

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"脚本运行的总时长: {total_time:.2f} 秒")
    if failed_steps:
        logger.error(f"以下表处理失败: {', '.join(failed_steps)}")
    else:
        logger.info("所有表处理成功")

    # 关闭所有连接池
    await db_manager.close_all()

if __name__ == "__main__":
    asyncio.run(main())