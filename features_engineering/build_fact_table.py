# build_fact_table.py
import pandas as pd
import os
import sys
import time
import shutil
import signal  # 添加signal模块导入
from datetime import datetime, timedelta
from utils import get_all_stock_codes
from config import *
from jq_utils import get_trading_dates
import multiprocessing as mp  # 添加多进程支持
from tqdm import tqdm  # 添加进度条支持（如果没有安装，可以使用 pip install tqdm）


# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import HS300
END_DAY = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

DM = DataManager()                     # 单例，避免重复初始化

# 全局变量存储进程池引用
_pool = None

# 信号处理函数
def signal_handler(sig, frame):
    global _pool
    if _pool is not None:
        print("\n检测到中断信号，正在终止所有子进程...", flush=True)
        _pool.terminate()
        _pool.join()
    sys.exit(1)

def _reshape_from_stock_key(df: pd.DataFrame, code: str, is_daily=True):
    df = df.reset_index().rename(columns={'index': 'datetime'})
    fmt = "%Y%m%d" if is_daily else "%Y%m%d%H%M%S"
    df['datetime'] = pd.to_datetime(df['datetime'].astype(str), format=fmt)
    df['ts_code'] = code
    return df

def _reshape_from_field_key(dm_dict: dict, code: str, is_daily=True):
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

def load_raw(code: str, period: str, stock_info:dict) -> pd.DataFrame:
    # 根据股票的上市和退市时间调整 START_DAY 和 END_DAY
    actual_start_day = START_DAY
    actual_end_day = END_DAY
    
    if code in stock_info:
        # 如果股票上市时间晚于 START_DAY，则使用上市时间作为开始时间
        list_date = stock_info.get('list_date')
        if list_date and list_date > START_DAY:
            actual_start_day = list_date
            print(f"[INFO] 调整 {code} 的开始时间为上市日期: {actual_start_day}")
        
        # 如果股票已退市，则使用退市时间作为结束时间
        delist_date = stock_info.get('delist_date')
        if delist_date and delist_date != '99991231' and delist_date < END_DAY:
            actual_end_day = delist_date
            print(f"[INFO] 调整 {code} 的结束时间为退市日期: {actual_end_day}")
    
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
        print(f"[WARN] DataManager returned empty dict for {code} {period}")
        return pd.DataFrame()

    # ---------- 新增：格式探测 ----------
    if code in raw_dict:                         # 股票→DF 结构
        fact_df = _reshape_from_stock_key(raw_dict[code], code, is_daily)
    else:                                        # 字段→DF 结构
        fact_df = _reshape_from_field_key(raw_dict, code, is_daily)

    if fact_df.empty:
        print(f"[WARN] No rows after reshape for {code} {period}")
    return fact_df

def add_features(df:pd.DataFrame) -> pd.DataFrame:
    #print(df)
    df['vwap'] = (df['amount'] / 100) / df['volume'].replace(0, pd.NA)
    df['ret_1p'] = df.close.pct_change().shift(-1)       # 下一周期收益 label
    # 简易涨跌停价（无聚宽时用规则法，后续可替换）
    pct = 0.1
    st_mask = df['ts_code'].str.contains('ST')
    pct_series = pd.Series(pct, index=df.index)
    pct_series.loc[st_mask] = 0.05
    df['high_limit'] = df['close'].shift(1) * (1+pct_series)
    df['low_limit']  = df['close'].shift(1) * (1-pct_series)
    return df

# 处理单个股票的函数
def process_stock(args):
    code, stock_info = args
    try:
        for period in PERIODS:
            t1 = time.time()
            raw = load_raw(code, period, stock_info)
            if raw.empty:
                continue
            t2 = time.time()    
            fact = add_features(raw)
            t3 = time.time()
            
            # 直接写入文件，不需要创建目录
            for day, g in fact.groupby(fact.datetime.dt.date):
                part_dir = f"{DATA_ROOT}/date={day}"
                output_file = f"{part_dir}/{code}_{period}.parquet"
                g.to_parquet(output_file, index=False, compression='snappy')
                
            t4 = time.time()
            print(f"[INFO] 处理 {code} {period} 耗时: {t2-t1:.2f}s, 特征处理耗时: {t3-t2:.2f}s, 写入磁盘耗时: {t4-t3:.2f}s", flush=True)
            
        # 返回成功标志
        return True
    except Exception as e:
        print(f"处理股票 {code} 时出错: {e}", flush=True)
        return False


def main():
    # 在main函数开始处添加
    if sys.platform == 'win32':
        mp.set_start_method('spawn', force=True)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"\n{'='*60}")
    print(f"开始构建事实表 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    # 清理输出目录，确保重新运行时数据一致
    clean_output_directory()

    # 预先获取交易日期并创建目录
    print("预先获取交易日期并创建目录...")
    trading_dates = get_trading_dates(START_DAY, END_DAY)

    # 创建所有交易日目录
    for day in trading_dates:
        part_dir = f"{DATA_ROOT}/date={day}"
        os.makedirs(part_dir, exist_ok=True)

    print(f"已创建 {len(trading_dates)} 个交易日目录")
    
    # 使用get_all_stock_codes获取所有股票信息
    all_stocks_df = get_all_stock_codes()
    
    # 检查是否成功获取股票列表
    if all_stocks_df.empty:
        print("错误：无法获取股票列表，将使用默认的HS300")
        stock_list = HS300
        stock_info_dict = {}
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
        print(f"获取到 {len(stock_list)} 只活跃股票")

    # 删除初始化处理统计的代码
    
    print(f"\n[配置信息]")
    print(f"数据时间范围: {START_DAY} 至 {END_DAY}")
    print(f"处理周期: {PERIODS}")
    print(f"输出目录: {DATA_ROOT}")
    print(f"待处理股票数量: {len(stock_list)}")
    
    # 多进程处理股票数据
    num_processes = min(8, mp.cpu_count() - 1)  # 使用CPU核心数减1，留一个核心给系统
    if num_processes < 1:
        num_processes = 1
    
    print(f"使用 {num_processes} 个进程并行处理数据...")
    print(f"\n开始处理...")
    
    # 准备参数列表
    args_list = [(code, stock_info_dict.get(code, {})) for code in stock_list]
    
    # 手动创建进程池
    global _pool
    _pool = mp.Pool(processes=num_processes)

    # 使用进程池并行处理
    try:
        # 使用imap_unordered可能会更高效，因为不需要保持顺序
        results = list(tqdm(_pool.imap_unordered(process_stock, args_list, chunksize=40), 
                          total=len(args_list), 
                          desc="总体进度"))
    except KeyboardInterrupt:
        print("检测到中断信号，正在终止所有子进程...")
        _pool.terminate()
        _pool.join()
        raise
    except Exception as e:
        print(f"发生错误: {e}")
        _pool.terminate()
        _pool.join()
        raise
    else:
        # 正常完成时关闭池
        _pool.close()
    finally:
        # 确保在所有情况下都等待进程结束
        _pool.join()
    
    # 最终统计
    success_count = results.count(True)
    
    print(f"\n{'='*60}")
    print(f"处理完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"成功处理: {success_count}/{len(stock_list)} 只股票")
    # 删除处理速度相关的输出
    print(f"输出目录: {DATA_ROOT}")
    
    if success_count < len(stock_list):
        failed_count = len(stock_list) - success_count
        print(f"\n[警告] {failed_count} 只股票处理失败")

def clean_output_directory():
    """使用重命名策略清理输出目录，效率更高"""
    import uuid
    
    if os.path.exists(DATA_ROOT):
        print(f"[清理] 重命名现有输出目录: {DATA_ROOT}")
        try:
            # 生成临时目录名
            temp_dir = f"{DATA_ROOT}_old_{uuid.uuid4().hex[:8]}"
            
            # 重命名旧目录（这是一个快速操作，不需要复制文件）
            os.rename(DATA_ROOT, temp_dir)
            print(f"[清理] 旧目录已重命名为: {temp_dir}")
            
            # 创建新的空目录
            os.makedirs(DATA_ROOT, exist_ok=True)
            print(f"[初始化] 创建新的输出目录: {DATA_ROOT}")
            
            # 在后台异步删除旧目录
            import threading
            threading.Thread(target=lambda: shutil.rmtree(temp_dir, ignore_errors=True), 
                           daemon=True).start()
            print(f"[清理] 已启动后台线程删除旧目录")
            
        except Exception as e:
            print(f"[警告] 清理输出目录时出错: {e}")
            # 确保目录存在
            os.makedirs(DATA_ROOT, exist_ok=True)
    else:
        # 如果目录不存在，创建它
        os.makedirs(DATA_ROOT, exist_ok=True)
        print(f"[初始化] 创建新的输出目录: {DATA_ROOT}")



if __name__ == "__main__":
    main()

