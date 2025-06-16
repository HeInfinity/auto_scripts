import os
import sys
import asyncio
import requests
import pandas as pd
import numpy as np
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 添加项目根目录到sys.path
def load_sys_path():
    """动态查找项目根目录并将相关目录添加到sys.path"""
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
        
    paths_to_add = [
        os.path.join(project_root, 'auto_scripts'),
        os.path.join(project_root, 'auto_scripts', 'jobs'),
        os.path.join(project_root, 'auto_scripts', 'modules')
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.append(path)
    return project_root

# 加载sys.path
load_sys_path()

from modules.db_conn import DBManager
from modules.log_tools import setup_logger

# 获取logger - 使用规则要求的日志格式
logger = setup_logger(__file__)

# 重新配置logger格式以符合规则要求
for handler in logger.handlers:
    formatter = handler.formatter
    if formatter:
        # 修改为规则要求的格式: [日期 时间] [应用名] [日志级别] 消息
        new_formatter = logging.Formatter('[%(asctime)s] [employee] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(new_formatter)

# 考试数据字段定义
EXAM_COLUMNS = ['examId', 'userid', 'reexam', 'userName', 'makeUp', 
                'stateValue', 'state', 'examName', 'gradetime']

def load_config_from_env():
    """从环境变量加载配置"""
    config_str = os.environ.get('MOXUEYUAN_CONFIG')
    if config_str:
        try:
            config = json.loads(config_str)
            return config.get('exams', {}), config.get('courses', {})
        except json.JSONDecodeError as e:
            logger.error(f"解析配置失败: {str(e)}")
            return {}, {}
    else:
        # 如果没有环境变量, 使用默认配置
        logger.warning("未找到环境变量配置, 使用默认配置")
        return {
            '线上课程-通关考试1.0': '2483139',
            '线上课程-通关考试2.0': '2804991',
        }, {
            '【客户经营】第八课-经营技巧': '3964706',
        }

async def get_access_token(db_manager: DBManager) -> str:
    """从数据库获取access_token"""
    try:
        conn = await db_manager.get_connection('myDB_Alicloud', 'zcw_hr')
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT token FROM moxueyuan_token LIMIT 1")
                result = await cursor.fetchone()
                if result and result['token']:
                    token = result['token']
                    logger.info(f"成功获取access_token: {token}")
                    return token
                else:
                    logger.error(f"数据库中未找到access_token")
                    return None
        finally:
            await db_manager.release_connection('myDB_Alicloud', conn)
    except Exception as e:
        logger.error(f"获取access_token时发生错误: {str(e)}")
        return None

def setup_requests_session(
    retries=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset(['GET', 'POST'])
):
    """
    配置requests会话, 添加重试机制
    :param retries: 重试次数
    :param backoff_factor: 重试间隔因子, 实际间隔为 {backoff_factor * (2 ** (重试次数 - 1))} 秒
    :param status_forcelist: 需要重试的HTTP状态码
    :param allowed_methods: 允许重试的HTTP方法
    :return: 配置好的requests会话对象
    """
    session = requests.Session()
    
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def fetch_all_exam_data(access_token: str, scene: str, exam_id: str, is_range: str) -> pd.DataFrame:
    """获取考试数据"""
    url = "https://open.moxueyuan.com/api/v2/count/statistical"
    all_pages = []
    page = 1
    
    # 初始化考试名称为空字符串, 后续从API返回数据中获取
    exam_name = ""
    
    # 根据is_range参数确定获取的是在职还是离职员工数据
    # is_range为空字符串表示获取离职员工数据
    # is_range为"0"表示获取在职员工数据
    status_description = "离职" if is_range == "" else "在职"
    
    # 创建配置了重试机制的会话
    session = setup_requests_session()
    
    while True:
        params = {
            'scene': scene,
            'access_token': access_token,
            'examId': exam_id,
            'isrange': is_range,
            'page': page
        }
        
        try:
            # 添加请求间隔, 避免频繁请求
            time.sleep(0.5)  # 500ms的请求间隔
            
            response = session.get(url, params=params, timeout=30)  # 添加30秒超时
            data = response.json()
            
            if data['status'] == 'Y' and 'results' in data:
                if not exam_name and data['results']:
                    exam_name = data['results'][0].get('examName', 'Unknown Exam')
                
                logger.info(f"获取考试数据: {exam_name}-{status_description}, 第{page}页")
                
                page_data = pd.DataFrame(data['results'])
                page_data = page_data[EXAM_COLUMNS]
                page_data.replace('', None, inplace=True)
                all_pages.append(page_data)
                
                if page >= data['pageCount']:
                    break
                page += 1
            else:
                logger.error(f"获取数据失败: {exam_name}-{status_description}, 第{page}页")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"请求发生错误: {str(e)}")
            # 如果是最后一次重试也失败了, 就退出循环
            if getattr(e.response, 'status_code', None) in (429, 403):  # 如果是频率限制, 多等待一会
                logger.warning(f"检测到请求频率限制, 等待60秒后继续...")
                time.sleep(60)
                continue
            break
            
    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()

def fetch_all_course_data(access_token: str, course_id: str) -> pd.DataFrame:
    """获取课程数据"""
    url = "https://open.moxueyuan.com/api/v2/course/state-query"
    all_pages = []
    page = 1
    
    # 初始化课程名称为空字符串, 后续从API返回数据中获取
    course_name = ""
    
    # 创建配置了重试机制的会话
    session = setup_requests_session()
    
    while True:
        params = {
            'access_token': access_token,
            'courseId': course_id,
            'page': page
        }
        
        try:
            # 添加请求间隔, 避免频繁请求
            time.sleep(0.5)  # 500ms的请求间隔
            
            response = session.get(url, params=params, timeout=30)  # 添加30秒超时
            data = response.json()
            
            if data['status'] == 'Y' and 'results' in data:
                if not course_name and data['results']:
                    course_name = data['results'][0].get('name', 'Unknown Course')
                
                logger.info(f"获取课程数据: {course_name}, 第{page}页")
                
                page_data = pd.DataFrame(data['results'])
                page_data = page_data[['state', 'courseId', 'userid', 'percent', 'learningProgress']]
                all_pages.append(page_data)
                
                if page >= data.get('pageCount', 0):
                    break
                page += 1
            else:
                logger.error(f"获取课程数据失败: {course_name}, 第{page}页")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"请求发生错误: {str(e)}")
            # 如果是最后一次重试也失败了, 就退出循环
            if getattr(e.response, 'status_code', None) in (429, 403):  # 如果是频率限制, 多等待一会
                logger.warning(f"检测到请求频率限制, 等待60秒后继续...")
                time.sleep(60)
                continue
            break
            
    return pd.concat(all_pages, ignore_index=True) if all_pages else pd.DataFrame()

async def save_to_database(db_manager: DBManager, df: pd.DataFrame, table_name: str):
    """保存数据到数据库"""
    try:
        # 处理DataFrame中的NaN值
        df = df.replace({np.nan: None})
        
        conn = await db_manager.get_connection('myDB_Alicloud', 'zcw_hr')
        try:
            async with conn.cursor() as cursor:
                # 清空表
                await cursor.execute(f"TRUNCATE TABLE `{table_name}`")
                logger.info(f"已清空表 {table_name}")
                
                # 分批插入数据
                batch_size = 10000
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size]
                    values = batch.values.tolist()
                    columns = batch.columns.tolist()
                    placeholders = ', '.join(['%s'] * len(columns))
                    
                    sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"
                    await cursor.executemany(sql, values)
                    await conn.commit()
                    
                    logger.info(f"已插入 {min(i + batch_size, len(df))}/{len(df)} 条记录到表 {table_name}")
        finally:
            await db_manager.release_connection('myDB_Alicloud', conn)
    except Exception as e:
        logger.error(f"保存数据到数据库时发生错误: {str(e)}")
        raise

async def process_and_save_data():
    """主处理函数"""
    try:
        logger.info(f"开始执行数据获取任务")
        db_manager = DBManager(logger=logger)
        
        # 加载配置
        exam_config, course_config = load_config_from_env()
        logger.info(f"加载配置 - 考试数量: {len(exam_config)}, 课程数量: {len(course_config)}")
        
        # 获取access_token
        access_token = await get_access_token(db_manager)
        if not access_token:
            logger.error(f"无法获取access_token, 任务终止")
            return
        
        # 获取所有课程数据
        all_course_data = pd.DataFrame()
        if course_config:
            logger.info(f"开始获取课程数据")
            course_data_list = []
            for course_name, course_id in course_config.items():
                logger.info(f"获取课程: {course_name}")
                course_data = fetch_all_course_data(access_token, course_id)
                if not course_data.empty:
                    # 添加课程名称和ID标识
                    course_data['courseName'] = course_name
                    course_data['courseIdOriginal'] = course_id
                    course_data_list.append(course_data)
            
            if course_data_list:
                all_course_data = pd.concat(course_data_list, ignore_index=True)
                logger.info(f"合并课程数据完成, 总记录数: {len(all_course_data)}")
            
        # 处理每个考试
        for exam_name, exam_id in exam_config.items():
            logger.info(f"开始处理考试: {exam_name}")
            
            # 获取在职和离职员工数据
            resigned_data = fetch_all_exam_data(access_token, 'examUsers', exam_id, '')
            active_data = fetch_all_exam_data(access_token, 'examUsers', exam_id, '0')
            combined_data = pd.concat([resigned_data, active_data])
            
            # 合并课程数据
            if not all_course_data.empty:
                final_data = pd.merge(combined_data, all_course_data, on='userid', how='left')
                logger.info(f"已合并课程数据")
            else:
                final_data = combined_data
                logger.info(f"无课程数据需要合并")
            
            # 确保只保留考试表中存在的字段, 避免courseName等额外字段导致数据库错误
            # 只保留存在于final_data中且在EXAM_COLUMNS列表中的字段
            available_columns = [col for col in EXAM_COLUMNS if col in final_data.columns]
            final_data = final_data[available_columns]
            
            # 保存到数据库
            table_name = f"新员工_考试数据{'1.0' if exam_id == '2483139' else '2.0'}"
            await save_to_database(db_manager, final_data, table_name)
            
        logger.info("所有数据处理完成!")
        
    except Exception as e:
        logger.error(f"处理数据时发生错误: {str(e)}")
    finally:
        await db_manager.close_all()

if __name__ == "__main__":
    try:
        asyncio.run(process_and_save_data())
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")