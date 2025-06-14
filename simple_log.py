import logging
import os
from logging.handlers import TimedRotatingFileHandler

def init_logging(name="correlation_analysis"):
    """
    初始化日志配置
    """
    # 配置日志
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, name+".log")

    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # 设置logger的级别为DEBUG，这样所有级别的日志都会被处理
    
    # 清除已存在的处理器，避免重复添加
    if logger.handlers:
        logger.handlers.clear()

    # 创建按天轮转的文件处理器 - 记录DEBUG及以上级别
    file_handler = TimedRotatingFileHandler(
        log_file,
        when='midnight',     # 每天午夜轮转
        interval=1,         # 轮转间隔为1天
        backupCount=7,      # 保留最近7天的日志文件
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # 文件处理器记录DEBUG及以上级别的日志
    file_handler.suffix = "%Y-%m-%d"  # 设置日志文件后缀格式

    # 创建控制台处理器 - 只记录INFO及以上级别
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # 控制台处理器只记录INFO及以上级别的日志

    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger