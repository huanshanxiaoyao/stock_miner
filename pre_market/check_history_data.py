# coding:utf-8
import os
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import SH50, BJ50

def check_and_download_daily_data(data_manager, stock_codes, start_date, end_date=None):
    """
    检查并下载天级数据
    
    参数:
    data_manager: DataManager实例
    stock_codes: 股票代码列表
    start_date: 开始日期
    end_date: 结束日期，默认为None
    
    返回:
    下载后的数据
    """
    print(f"检查天级数据是否存在...")
    
    # 先尝试获取本地数据
    data = data_manager.get_local_daily_data(['close', 'volume'], stock_codes, start_date, end_date)
    
    # 检查数据是否存在且充足
    need_download = False
    missing_codes = []
    
    if data is None:
        print("本地天级数据获取失败，需要下载")
        need_download = True
        missing_codes = stock_codes
    else:
        # 检查每只股票的数据
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"股票 {code} 天级数据不存在或为空，需要下载")
                missing_codes.append(code)
                need_download = True
            else:
                # 检查数据是否足够130个交易日
                if len(data[code]) < 150:
                    print(f"股票 {code} 的交易日数据不足，仅有 {len(data[code])} 个交易日，需要下载")
                    missing_codes.append(code)
                    need_download = True
    
    # 如果需要下载，则下载数据
    if need_download:
        print(f"开始下载 {len(missing_codes)} 只股票的天级数据...")
        # 使用同步下载，确保数据下载完成
        data_manager.download_data_sync(missing_codes, '1d', start_date, end_date)
        print("天级数据下载完成，重新获取数据...")
        
        # 重新获取数据
        data = data_manager.get_local_daily_data(['close', 'volume'], stock_codes, start_date, end_date)
        
        # 再次检查数据
        if data is None:
            print("下载后天级数据仍然获取失败")
            return None
        
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"下载后股票 {code} 天级数据仍然不存在或为空")
            else:
                print(f"股票 {code} 天级数据已下载，共 {len(data[code])} 个交易日")
    else:
        print("所有股票的天级数据已存在且充足")
    
    return data

def check_and_download_minutes_data(data_manager, stock_codes, start_date, end_date=None):
    """
    检查并下载分钟级数据
    
    参数:
    data_manager: DataManager实例
    stock_codes: 股票代码列表
    start_date: 开始日期
    end_date: 结束日期，默认为None
    
    返回:
    下载后的数据
    """
    print(f"检查分钟级数据是否存在...")
    
    # 先尝试获取本地数据
    data = data_manager.get_local_minutes_data(['close','volume'], stock_codes, start_date, end_date)
    
    # 检查数据是否存在且充足
    need_download = False
    missing_codes = []
    
    if data is None:
        print("本地分钟级数据获取失败，需要下载")
        need_download = True
        missing_codes = stock_codes
    else:
        # 检查每只股票的数据
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"股票 {code} 分钟级数据不存在或为空，需要下载")
                missing_codes.append(code)
                need_download = True
            else:
                # 检查数据是否足够
                # 分钟级数据每天约240个数据点，30天大概有20个交易日，这里取19*240=4560
                if len(data[code]) < 4800:  # 设置一个较低的阈值，考虑到节假日等因素
                    print(f"股票 {code} 的分钟级数据不足，仅有 {len(data[code])} 个数据点，需要下载")
                    missing_codes.append(code)
                    need_download = True
    
    # 如果需要下载，则下载数据
    if need_download:
        print(f"开始下载 {len(missing_codes)} 只股票的分钟级数据...")
        # 使用同步下载，确保数据下载完成
        data_manager.download_data_sync(missing_codes, '1m', start_date, end_date)
        time.sleep(3)  # 等待1秒，确保数据下载完成，避免因网络问题导致数据不完整或缺失
        print("分钟级数据下载完成，重新获取数据...")
        # 重新获取数据
        data = data_manager.get_local_minutes_data(['close','volume'], stock_codes, start_date, end_date)
        
        # 再次检查数据
        if data is None:
            print("下载后分钟级数据仍然获取失败")
            return None
        
        for code in stock_codes:
            if code not in data or data[code].empty:
                print(f"下载后股票 {code} 分钟级数据仍然不存在或为空")
            else:
                print(f"股票 {code} 分钟级数据已下载，共 {len(data[code])} 个数据点")
    else:
        print("所有股票的分钟级数据已存在且充足")
    
    return data

def get_related_codes():
    """
    从shared目录下的correlation_results.json文件中读取所有涉及到的股票代码
    
    返回:
    所有涉及到的股票代码列表
    """
    # 构建correlation_results.json文件的路径
    json_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shared", "correlation_results.json")
    
    # 检查文件是否存在
    if not os.path.exists(json_file_path):
        print(f"文件不存在: {json_file_path}")
        return []
    
    try:
        # 读取JSON文件
        with open(json_file_path, 'r', encoding='utf-8') as f:
            correlation_data = json.load(f)
        
        # 收集所有股票代码
        all_codes = set()
        
        # 添加所有北交所股票代码（JSON的key）
        for bj_code in correlation_data.keys():
            all_codes.add(bj_code)
            
            # 添加相似股票的代码
            if 'similar_stocks' in correlation_data[bj_code]:
                for stock_info in correlation_data[bj_code]['similar_stocks']:
                    if 'code' in stock_info:
                        all_codes.add(stock_info['code'])
        
        print(f"从correlation_results.json中读取到 {len(all_codes)} 只股票代码")
        return list(all_codes)
    
    except Exception as e:
        print(f"读取correlation_results.json文件时出错: {str(e)}")
        return []

def main():
    """
    主函数，检查并下载历史数据
    """
    # 初始化DataManager
    data_manager = DataManager()
    
    # 获取当前日期
    current_date = datetime.now().strftime("%Y%m%d")
    
    # 计算开始日期
    daily_start_date = (datetime.now() - timedelta(days=210)).strftime("%Y%m%d")  # 过去210天
    minutes_start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")  # 过去30天
    
    # 获取相关股票代码
    related_codes = get_related_codes()
    
    # 合并股票代码列表，BJ50和SH50是默认必选的
    all_codes = set(SH50 + BJ50)  # 使用集合避免重复
    
    # 如果从correlation_results.json中读取到股票代码，也加入进来
    if related_codes:
        all_codes.update(related_codes)  # 使用update方法合并集合
        print(f"从correlation_results.json中读取到 {len(related_codes)} 只股票代码")
    
    # 转换为列表
    all_codes = list(all_codes)
    print(f"共需检查 {len(all_codes)} 只股票的历史数据")
    
    # 检查并下载天级数据
    print("\n" + "="*50)
    print("开始检查天级数据")
    print("="*50)
    daily_data = check_and_download_daily_data(data_manager, all_codes, daily_start_date, current_date)
    
    # 检查并下载分钟级数据
    print("\n" + "="*50)
    print("开始检查分钟级数据")
    print("="*50)
    minutes_data = check_and_download_minutes_data(data_manager, all_codes, minutes_start_date, current_date)
    
    print("\n" + "="*50)
    print("历史数据检查与下载完成")
    print("="*50)

if __name__ == "__main__":
    main()
