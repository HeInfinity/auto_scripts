import os
import sys
import asyncio
import subprocess
import logging
from datetime import datetime
from typing import Dict, List, Any

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

from modules.log_tools import setup_logger

# 获取logger - 使用规则要求的日志格式
logger = setup_logger(__file__)

# 重新配置logger格式以符合规则要求
for handler in logger.handlers:
    formatter = handler.formatter
    if formatter:
        # 修改为规则要求的格式: [日期 时间] [应用名] [日志级别] 消息
        new_formatter = logging.Formatter('[%(asctime)s] [scheduler] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(new_formatter)

# 魔学院数据获取配置
MOXUEYUAN_CONFIG = {
    # 考试ID配置
    'exams': {
        '线上课程-通关考试1.0': '2483139',
        '线上课程-通关考试2.0': '2804991',
        # 可以在这里添加更多考试
    },
    
    # 课程ID配置
    'courses': {
        '【客户经营】第八课-经营技巧': '3964706',
        # 可以在这里添加更多课程
    }
}

# 任务执行配置
TASK_CONFIG = [
    {
        'name': '获取access_token',
        'script_path': 'jobs/moxueyuan/get_mxy_token.py',
        'description': '获取魔学院API访问令牌',
        'required': True  # 必须执行的任务
    },
    {
        'name': '获取员工考试数据',
        'script_path': 'jobs/moxueyuan/get_mxy_employee.py',
        'description': '获取员工考试和课程数据',
        'required': True,
        'config_required': True  # 需要传递配置的任务
    }
    # 可以在这里添加更多任务
]

async def execute_script(script_path: str, config: Dict = None) -> bool:
    """执行指定的Python脚本"""
    try:
        project_root = load_sys_path()
        full_script_path = os.path.join(project_root, 'auto_scripts', script_path)
        
        if not os.path.exists(full_script_path):
            logger.error(f"脚本文件不存在: {full_script_path}")
            return False
        
        logger.info(f"开始执行脚本: {script_path}")
        
        # 如果需要传递配置，通过环境变量传递
        env = os.environ.copy()
        if config:
            import json
            env['MOXUEYUAN_CONFIG'] = json.dumps(config, ensure_ascii=False)
        
        # 执行脚本并实时读取输出
        process = await asyncio.create_subprocess_exec(
            sys.executable, full_script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,  # 合并stderr到stdout
            env=env
        )
        
        # 实时读取输出
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            # 解码并去除换行符
            line_text = line.decode('utf-8').strip()
            if line_text:
                # 转发子脚本的日志，保持原格式
                print(line_text)
        
        # 等待进程结束
        await process.wait()
        
        if process.returncode == 0:
            logger.info(f"脚本执行成功: {script_path}")
            return True
        else:
            logger.error(f"脚本执行失败: {script_path}, 返回码: {process.returncode}")
            return False
            
    except Exception as e:
        logger.error(f"执行脚本时发生异常: {script_path}, 错误: {str(e)}")
        return False

async def main():
    """主函数 - 按顺序执行所有任务"""
    try:
        logger.info(f"开始执行魔学院数据获取任务调度")
        
        success_count = 0
        total_tasks = len(TASK_CONFIG)
        
        for i, task in enumerate(TASK_CONFIG, 1):
            logger.info(f"执行任务 {i}/{total_tasks}: {task['name']}")
            logger.info(f"任务描述: {task['description']}")
            
            # 准备配置参数
            config = None
            if task.get('config_required', False):
                config = MOXUEYUAN_CONFIG
            
            # 执行任务
            success = await execute_script(task['script_path'], config)
            
            if success:
                success_count += 1
                logger.info(f"任务完成: {task['name']}")
            else:
                logger.error(f"任务失败: {task['name']}")
                
                # 如果是必须执行的任务失败了，停止后续任务
                if task.get('required', False):
                    logger.error(f"必需任务失败，停止执行后续任务")
                    break
            
            # 任务间隔，避免服务器压力过大
            if i < total_tasks:
                logger.info(f"等待5秒后执行下一个任务...")
                await asyncio.sleep(5)
        
        logger.info(f"任务调度完成! 成功: {success_count}/{total_tasks}")
        
    except Exception as e:
        logger.error(f"任务调度过程中发生错误: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}") 