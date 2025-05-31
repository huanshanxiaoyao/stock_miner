# download_data.py
import sys
import os
import multiprocessing as mp
from xtquant import xtdata
from config import PERIODS, START_DAY
from tqdm import tqdm
from utils import get_all_stock_codes

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import HS300

data_manager = DataManager()

def on_progress_fd(data):
	print(data)
	# {'finished': 1, 'total': 50, 'stockcode': '000001.SZ', 'message': ''}

def download_stock_data(stock_codes, stock_info_dict):
    """下载单个进程负责的股票数据"""
    for code in tqdm(stock_codes, desc=f"进程{os.getpid()}下载行情"):
        # 获取股票上市时间
        list_date = stock_info_dict.get(code, {}).get('list_date', '')
        
        # 确定下载开始时间：如果上市时间晚于START_DAY，则使用上市时间
        start_date = list_date if list_date and list_date > START_DAY else START_DAY
        
        for period in PERIODS:
            data_manager.download_data_async(
                [code], period=period, start_date=start_date
            )
    print("下载基本面数据...")
    xtdata.download_financial_data2(stock_codes, callback=on_progress_fd)      # 基本面


def main():
    # 获取CPU核心数，但最多使用8个进程以避免过度并行
    num_processes = min(4, mp.cpu_count())
    
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
    
    # 将股票列表分成多个批次
    batch_size = len(stock_list) // num_processes
    batches = []
    
    for i in range(num_processes):
        start_idx = i * batch_size
        # 最后一个批次可能会更大一些，确保包含所有剩余股票
        end_idx = (i + 1) * batch_size if i < num_processes - 1 else len(stock_list)
        batches.append(stock_list[start_idx:end_idx])
    
    # 创建并启动多个进程
    processes = []
    for batch in batches:
        if batch:  # 确保批次不为空
            p = mp.Process(target=download_stock_data, args=(batch, stock_info_dict))
            processes.append(p)
            p.start()
    
    # 等待所有进程完成
    for p in processes:
        p.join()
    
    # 下载基本面和板块数据（这部分保持单进程，因为它们是一次性操作）
 
    print("下载板块数据...")
    xtdata.download_sector_data()                   # 板块
    print("所有数据下载完成！")

if __name__ == "__main__":
    main()

