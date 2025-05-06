# coding:utf-8
import os
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import SH50

def check_and_download_data(data_manager, stock_codes, start_date, end_date=None):
    """
    检查数据是否存在，如果不存在或不足则下载
    
    参数:
    data_manager: DataManager实例
    stock_codes: 股票代码列表
    start_date: 开始日期
    end_date: 结束日期，默认为None
    
    返回:
    下载后的数据
    """
    print(f"检查数据是否存在...")
    
    # 先尝试获取本地数据
    data = data_manager.get_local_minutes_data(['volume'], stock_codes, start_date, end_date)
    
    # 检查数据是否存在且充足
    need_download = False
    missing_codes = []
    
    if data is None:
        print("本地数据获取失败，需要下载")
        need_download = True
        missing_codes = stock_codes
    else:
        # 检查每只股票的数据
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"股票 {code} 数据不存在或为空，需要下载")
                missing_codes.append(code)
                need_download = True
            else:
                # 检查数据是否足够7天
                volume_data = data[code]['volume']
                volume_data.index = pd.to_datetime(volume_data.index)
                dates = sorted(list(volume_data.groupby(volume_data.index.date).groups.keys()))
                
                if len(dates) < 6:  # 至少需要6个交易日
                    print(f"股票 {code} 的交易日数据不足，仅有 {len(dates)} 个交易日，需要下载")
                    missing_codes.append(code)
                    need_download = True
    
    # 如果需要下载，则下载数据
    if need_download:
        print(f"开始下载 {missing_codes} 的分钟级数据...")
        # 使用同步下载，确保数据下载完成
        data_manager.download_data_sync(missing_codes, '1m', start_date, end_date)
        print("数据下载完成，重新获取数据...")
        
        # 重新获取数据
        data = data_manager.get_local_minutes_data(['volume'], stock_codes, start_date, end_date)
        
        # 再次检查数据
        if data is None:
            print("下载后数据仍然获取失败")
            return None
        
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"下载后股票 {code} 数据仍然不存在或为空")
                return None
    
    return data

def detect_volume_anomalies(stock_codes, log_file=None):
    """
    检测给定股票列表在最近交易日的每分钟成交量是否超过过去5个交易日同一分钟成交量均值的3倍
    
    参数:
    stock_codes: 股票代码列表
    log_file: 日志文件路径，默认为None，将在当前目录创建日志文件
    
    返回:
    无，结果输出到日志文件
    """
    # 初始化DataManager
    data_manager = DataManager()
    
    # 设置日志文件
    if log_file is None:
        log_file = os.path.join(os.path.dirname(__file__), f"volume_anomaly_{time.strftime('%Y%m%d', time.localtime())}.log")
    
    # 获取当前日期
    current_date = time.strftime("%Y%m%d", time.localtime())
    
    # 计算开始日期（往前推7天，确保能获取到5个交易日的数据）
    start_date = (datetime.now() - timedelta(days=12)).strftime("%Y%m%d")
    
    # 检查并下载数据
    data = check_and_download_data(data_manager, stock_codes, start_date)
    
    if data is None:
        print("数据获取失败，即使尝试下载后仍然失败")
        sys.exit(1)
    
    # 打开日志文件
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"成交量异常检测报告 - {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n")
        f.write("="*50 + "\n\n")
        
        # 处理每只股票
        for code in stock_codes:
            f.write(f"股票代码: {code}\n")
            f.write("-"*30 + "\n")
            
            # 获取该股票的成交量数据
            volume_data = data[code]['volume']
            
            # 按日期分组
            volume_data.index = pd.to_datetime(volume_data.index)
            volume_data_by_date = volume_data.groupby(volume_data.index.date)
            
            # 获取日期列表
            dates = sorted(list(volume_data_by_date.groups.keys()))
            
            # 确保有足够的交易日数据
            if len(dates) < 6:  # 至少需要6个交易日（最近1天 + 过去5天）
                f.write(f"警告: 股票 {code} 的交易日数据不足，仅有 {len(dates)} 个交易日\n\n")
                continue
            
            # 获取最近的交易日
            latest_date = dates[-1]
            
            # 获取最近交易日的数据
            latest_data = volume_data[volume_data.index.date == latest_date]
            
            # 获取每分钟的时间（小时:分钟）
            minute_times = [time.strftime("%H:%M") for time in latest_data.index.time]
            
            # 检查每分钟的成交量
            anomalies_found = False
            
            for i, (minute_time, volume) in enumerate(zip(minute_times, latest_data.values)):
                # 获取过去5个交易日同一分钟的成交量
                historical_volumes = []
                
                for j in range(2, 7):  # 倒数第2到倒数第6个交易日
                    if j <= len(dates):
                        historical_date = dates[-j]
                        historical_data = volume_data[volume_data.index.date == historical_date]
                        
                        # 确保历史数据中有足够的分钟数据
                        if i < len(historical_data):
                            historical_volumes.append(historical_data.iloc[i])
                
                # 如果没有足够的历史数据，跳过
                if len(historical_volumes) == 0:
                    continue
                
                # 计算历史成交量均值
                avg_volume = sum(historical_volumes) / len(historical_volumes)
                
                # 检查是否超过4倍
                if (volume > 3 * avg_volume and avg_volume > 3000) or (volume > 4 * avg_volume and avg_volume > 100):
                    anomalies_found = True
                    f.write(f"  异常时间: {latest_date.strftime('%Y-%m-%d')} {minute_time}\n")
                    f.write(f"  当前成交量: {volume:.0f}\n")
                    f.write(f"  历史均值: {avg_volume:.0f}\n")
                    f.write(f"  倍数: {volume/avg_volume:.2f}\n")
                    f.write("\n")
            
            if not anomalies_found:
                f.write("  未发现异常成交量\n\n")
    
    print(f"成交量异常检测完成，结果已保存到 {log_file}")

if __name__ == "__main__":
    # 可以从命令行参数获取股票代码列表，或者直接在这里指定
    if len(sys.argv) > 1:
        stock_codes = sys.argv[1:]
    else:
        # 默认检测的股票列表
        stock_codes = ["300870.SZ", "430139.BJ", "688449.SH", "920029.BJ"]
        stock_codes = SH50
    
    detect_volume_anomalies(stock_codes)