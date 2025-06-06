# build_fact_table.py
import pandas as pd
import os
import sys
import time
import shutil
import signal
import uuid
import threading
from datetime import datetime, timedelta
from utils import get_all_stock_codes
from config import *
from jq_utils import get_trading_days
import multiprocessing as mp
from tqdm import tqdm

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import HS300
from simple_log import init_logging

# 在文件顶部导入新的特征计算器
from feature_calculator import calculate_features

END_DAY = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
logger = init_logging("fact_table_builder")
DM = DataManager()  # 单例，避免重复初始化



def process_batch(args_list):
    for args in args_list:
        process_stock(args)

def process_stock(args):
    """处理单个股票的独立函数"""
    code, stock_info = args
    
    try:
        for period in PERIODS:
            # 加载原始数据的逻辑
            # 在process_stock函数中
            raw = load_raw(code, period, stock_info) 
            if raw.empty:
                continue 
            fact = calculate_features(raw)
            t3 = time.time()
            
            for day, g in fact.groupby(fact.datetime.dt.date):
                part_dir = f"{DATA_ROOT}/date={day}"
                output_file = f"{part_dir}/{code}_{period}.parquet"
                # 使用pyarrow引擎和优化参数
                g.to_parquet(
                    output_file, 
                    index=False, 
                    compression='snappy'
                )
                
            t4 = time.time()
            logger.info(f"处理 {code} {period}  写入磁盘耗时: {t4-t3:.2f}s")
            
        # 返回成功标志
        return True
    except Exception as e:
        logger.error(f"处理股票 {code} 时出错: {e}")
        return False

# 将load_raw也移到类外部
def load_raw(code, period, stock_info):
    # 根据股票的上市和退市时间调整 START_DAY 和 END_DAY
    actual_start_day = START_DAY
    actual_end_day = END_DAY  # 修复：使用全局变量 END_DAY
    
    if code in stock_info:
        # 如果股票上市时间晚于 START_DAY，则使用上市时间作为开始时间
        list_date = stock_info.get('list_date')
        if list_date and list_date > START_DAY:
            actual_start_day = list_date
        
        # 如果股票已退市，则使用退市时间作为结束时间
        delist_date = stock_info.get('delist_date')
        # 将这行
        if delist_date and delist_date != '99991231' and delist_date < END_DAY:
            actual_end_day = delist_date
    
    if period == "1d":
        fields = ['open', 'high', 'low', 'close', 'volume', 'amount']
        raw_dict = DM.get_local_daily_data(
            fields, [code], actual_start_day, actual_end_day, "none"
        )
        is_daily = True
    else:
        fields = ['open', 'high', 'low', 'close', 'volume', 'amount']
        raw_dict = DM.get_local_minutes_data(
            fields, [code], actual_start_day, actual_end_day, "none"
        )
        is_daily = False

    if not raw_dict:
        return pd.DataFrame()

    # 格式探测
    if code in raw_dict:  # 股票→DF 结构
        fact_df = reshape_from_stock_key(raw_dict[code], code, is_daily)
    else:  # 字段→DF 结构
        fact_df = reshape_from_field_key(raw_dict, code, is_daily)

    return fact_df

# 辅助函数
def reshape_from_stock_key(df, code, is_daily=True):
    """从以股票为键的数据结构重塑数据"""
    df = df.reset_index().rename(columns={'index': 'datetime'})
    fmt = "%Y%m%d" if is_daily else "%Y%m%d%H%M%S"
    df['datetime'] = pd.to_datetime(df['datetime'].astype(str), format=fmt)
    df['ts_code'] = code
    return df

def reshape_from_field_key(dm_dict, code, is_daily=True):
    """从以字段为键的数据结构重塑数据"""
    long_df = pd.concat(
        {fld: mat.loc[code] for fld, mat in dm_dict.items() if code in mat.index},
        axis=1
    )
    long_df.index.name = 'datetime'
    long_df.reset_index(inplace=True)
    fmt = "%Y%m%d" if is_daily else "%Y%m%d%H%M%S"
    long_df['datetime'] = pd.to_datetime(long_df['datetime'].astype(str), format=fmt)
    long_df['ts_code'] = code
    return long_df



class FactTableBuilder:
    """股票事实表构建器类"""
    """目前属于一个空壳"""
    def __init__(self):
        self.processes = []
        self.is_exiting = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        logger.info(f"主进程ID: {os.getpid()}")
    
    
    # 修改方法定义
    def _signal_handler(self, sig, frame=None):
        """信号处理函数"""
        if not self.is_exiting:
            self.is_exiting = True
            logger.warning(f"检测到中断信号 {sig}，正在终止所有子进程...")
            sys.stdout.flush()
            
            # 终止所有子进程
            for p in self.processes:
                logger.warning(f"终止子进程 {p.pid}...")
                p.terminate()
                p.join()

    
    def clean_output_directory(self):
        """使用重命名策略清理输出目录，效率更高"""
        if os.path.exists(DATA_ROOT):
            logger.info(f"重命名现有输出目录: {DATA_ROOT}")
            try:
                # 生成临时目录名
                temp_dir = f"{DATA_ROOT}_old_{uuid.uuid4().hex[:8]}"
                
                # 重命名旧目录（这是一个快速操作，不需要复制文件）
                os.rename(DATA_ROOT, temp_dir)
                logger.info(f"旧目录已重命名为: {temp_dir}")
                
                # 创建新的空目录
                os.makedirs(DATA_ROOT, exist_ok=True)
                logger.info(f"创建新的输出目录: {DATA_ROOT}")
                
                # 在后台异步删除旧目录
                threading.Thread(target=lambda: shutil.rmtree(temp_dir, ignore_errors=True), 
                               daemon=True).start()
                logger.info(f"已启动后台线程删除旧目录")
                
            except Exception as e:
                logger.warning(f"清理输出目录时出错: {e}")
                # 确保目录存在
                os.makedirs(DATA_ROOT, exist_ok=True)
        else:
            # 如果目录不存在，创建它
            os.makedirs(DATA_ROOT, exist_ok=True)
            logger.info(f"创建新的输出目录: {DATA_ROOT}")
    
    def run(self):
        """运行事实表构建流程"""
        logger.info(f"开始构建事实表 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 清理输出目录，确保重新运行时数据一致
        self.clean_output_directory()

        # 预先获取交易日期并创建目录
        logger.info("预先获取交易日期并创建目录...")
        trading_dates = get_trading_days(START_DAY, END_DAY)  # 使用全局常量
        
        # 创建所有交易日目录
        for day in trading_dates:
            part_dir = f"{DATA_ROOT}/date={day}"
            os.makedirs(part_dir, exist_ok=True)

        logger.info(f"已创建 {len(trading_dates)} 个交易日目录")
        
        # 使用get_all_stock_codes获取所有股票信息
        all_stocks_df = get_all_stock_codes()
        if all_stocks_df.empty:
            logger.error("无法获取股票列表，将使用默认的HS300")
            return False
        else:
            # 创建股票代码到上市信息的映射字典
            stock_info_dict = {}
            for _, row in all_stocks_df.iterrows():
                stock_info_dict[row['ts_code']] = {
                    'list_date': row['list_date'],
                    'delist_date': row['delist_date'],
                    'is_active': row['is_active'] if 'is_active' in row else (row['delist_date'] == '99991231')
                }
            
            # 获取活跃股票列表（未退市的股票）
            active_stocks = all_stocks_df[all_stocks_df['delist_date'] == '99991231']
            stock_list = active_stocks['ts_code'].tolist()
            logger.info(f"获取到 {len(stock_list)} 只活跃股票")

        #debug
        stock_list = HS300
        logger.info(f"数据时间范围: {START_DAY} 至 {END_DAY}")  # 使用全局常量
        logger.info(f"处理周期: {PERIODS}")
        logger.info(f"输出目录: {DATA_ROOT}")
        logger.info(f"待处理股票数量: {len(stock_list)}")
        
        # 多进程处理股票数据
        num_processes = min(8, mp.cpu_count() - 1)  # 使用CPU核心数减1，留一个核心给系统
        if num_processes < 1:
            num_processes = 1

        # 在run方法中
        args_list = [(code, stock_info_dict.get(code, {})) for code in stock_list]  # 修复：添加 data_root 参数
        batch_size = len(stock_list) // num_processes + 1
        for task_id in range(num_processes):
            sub_args_list = args_list[task_id * batch_size: (task_id + 1) * batch_size]
            # 如果想让每个进程处理多个股票，应该修改为：
            p = mp.Process(target=process_batch, args=(sub_args_list,))

            self.processes.append(p)
            p.start()
        
        logger.info(f"使用 {num_processes} 个进程并行处理数据...")
        logger.info(f"\n开始处理...")
        
        try:
            # 等待所有子进程完成
            for p in self.processes:
                p.join()
        except KeyboardInterrupt:
            logger.warning("检测到键盘中断，正在终止所有子进程...")
            self._signal_handler(signal.SIGINT)  # 移除第二个参数
            raise
        except Exception as e:
            logger.error(f"发生错误: {e}")
            raise
        else:
            logger.info("所有任务正常完成")
     
        logger.info(f"处理完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"输出目录: {DATA_ROOT}")


if __name__ == "__main__":
    builder = FactTableBuilder()
    builder.run()

