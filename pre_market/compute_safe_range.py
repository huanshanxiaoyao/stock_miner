import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import time
import json

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import *
from date_utils import get_trading_days
from simple_log import init_logging

CalendarDaysCount = 190
LongSMALength = 55
logger = init_logging("compute_safe_range")

debug = False

# 初始化DataManager
data_manager = DataManager()

def get_historical_data(ticker, start_date, end_date):
    """
    获取股票的历史数据
    
    参数:
    ticker (str): 股票代码
    days (int): 需要的交易日数量
    
    返回:
    pandas.DataFrame: 包含历史价格数据的DataFrame
    """

    trading_days = get_trading_days(start_date, end_date)
    trading_days_count = len(trading_days)
    
    # 使用DataManager获取数据
    data_dict = data_manager.get_local_daily_data(['close', 'high', 'low'], [ticker], start_date, end_date)
    
    if data_dict is None or ticker not in data_dict or data_dict[ticker].empty or len(data_dict[ticker]) < trading_days_count:
        logger.warning(f"警告: 无法获取 {ticker} 的数据，尝试下载...")
        data_manager.download_data_sync([ticker], '1d', start_date, end_date)
        data_dict = data_manager.get_local_daily_data(['close', 'high', 'low'], [ticker], start_date, end_date)
        
        if data_dict is None or ticker not in data_dict or data_dict[ticker].empty or len(data_dict[ticker]) < trading_days_count:
            logger.error(f"错误: 无法获取 {ticker} 的数据 退出")
            return None
    
    # 转换为与原代码兼容的格式
    data = data_dict[ticker].copy()
    data.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low'}, inplace=True)
    
    
    return data # 返回最近的days个交易日数据

def calculate_ema(data, period=LongSMALength ):
    """
    计算指数移动平均线(EMA)
    
    参数:
    data (pandas.DataFrame): 价格数据
    period (int): EMA周期
    
    返回:
    pandas.Series: EMA值
    """
    return data['Close'].ewm(span=period, adjust=False).mean()

def calculate_sma(data, period=5):
    """
    计算简单移动平均线(SMA)
    
    参数:
    data (pandas.DataFrame): 价格数据
    period (int): SMA周期
    
    返回:
    pandas.Series: SMA值
    """
    return data['Close'].rolling(window=period).mean()

def calculate_atr(data, period=20):
    """
    计算平均真实范围(ATR)
    参数:
    data (pandas.DataFrame): 价格数据
    period (int): ATR周期
    
    返回:
    pandas.Series: ATR值
    """
    high = data['High']
    low = data['Low']
    close = data['Close']
    
    # 计算真实范围(TR)
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    
    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    
    # 计算ATR
    atr = tr.rolling(window=period).mean()
    
    return atr

def compute_safe_range(ticker, start_date, end_date):
    """
    计算股票的安全交易区间（长期和短期两个口径）
    
    参数:
    ticker (str): 股票代码
    
    返回:
    dict: 包含长期和短期安全区间的字典
    """
    # 获取历史数据
    data = get_historical_data(ticker, start_date, end_date)
    
    if data is None or len(data) < LongSMALength *2:
        logger.warning(f"警告: 股票 {ticker} 的数据不足，无法计算安全区间")
        return None
    
    # 获取最新收盘价
    latest_close = data['Close'].iloc[-1]
    
    # 计算长期安全区间 (EMA55 + ATR20)
    ema55 = calculate_ema(data, LongSMALength )
    atr20 = calculate_atr(data, 20)
    long_term_upper = ema55.iloc[-1] + (atr20.iloc[-1] * 2.0)
    long_term_lower = ema55.iloc[-1] - (atr20.iloc[-1] * 2.0)
    
    # 计算短期安全区间 - SMA5
    sma5 = calculate_sma(data, 5)
    atr10 = calculate_atr(data, 10)
    short_term_sma_upper = sma5.iloc[-1] + (atr10.iloc[-1] * 1.5)
    short_term_sma_lower = sma5.iloc[-1] - (atr10.iloc[-1] * 1.5)
    
    # 计算短期安全区间 - EMA8
    ema8 = calculate_ema(data, 8)
    short_term_ema_upper = ema8.iloc[-1] + (atr10.iloc[-1] * 1.5)
    short_term_ema_lower = ema8.iloc[-1] - (atr10.iloc[-1] * 1.5)
    
    # 检查索引类型并适当处理日期
    if isinstance(data.index[-1], str):
        date_str = data.index[-1]
    else:
        date_str = data.index[-1].strftime('%Y-%m-%d')
    
    return {
        'ticker': ticker,
        'date': date_str,
        'close': latest_close,
        # 长期安全区间
        'long_term': {
            'ema55': ema55.iloc[-1],
            'atr20': atr20.iloc[-1],
            'upper': long_term_upper,
            'lower': long_term_lower,
            'is_in_range': long_term_lower <= latest_close <= long_term_upper
        },
        # 短期安全区间 - SMA5
        'short_term_sma': {
            'sma5': sma5.iloc[-1],
            'atr10': atr10.iloc[-1],
            'upper': short_term_sma_upper,
            'lower': short_term_sma_lower,
            'is_in_range': short_term_sma_lower <= latest_close <= short_term_sma_upper
        },
        # 短期安全区间 - EMA8
        'short_term_ema': {
            'ema8': ema8.iloc[-1],
            'atr10': atr10.iloc[-1],
            'upper': short_term_ema_upper,
            'lower': short_term_ema_lower,
            'is_in_range': short_term_ema_lower <= latest_close <= short_term_ema_upper
        }
    }

def get_stock_name(ticker):
    """
    从codeA2industry或codeB2industry文件中获取股票名称
    
    参数:
    ticker (str): 股票代码
    
    返回:
    str: 股票名称，如果未找到则返回空字符串
    """
    # 获取A2BJ目录路径
    a2bj_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "A2BJ")
    
    # 检查股票代码类型
    if ticker.endswith('.BJ'):
        # 北交所股票
        file_path = os.path.join(a2bj_dir, "codeB2industry")
    else:
        # A股股票
        file_path = os.path.join(a2bj_dir, "codeA2industry")
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        logger.warning(f"警告: 文件不存在 {file_path}")
        return ""
    
    # 读取文件查找股票名称
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2 and parts[0] == ticker:
                    return parts[1]
    except Exception as e:
        logger.error(f"读取股票名称时出错: {str(e)}")
    
    return ""

def format_stock_data_to_json(results):
    """
    将股票数据格式化为JSON格式
    
    参数:
    results (list): 包含股票数据的列表
    
    返回:
    str: JSON格式的字符串
    """
    json_data = {}
    
    for result in results:
        if result is None:
            continue
        
        # 获取股票名称
        stock_name = get_stock_name(result['ticker'])
        
        # 创建JSON对象
        stock_data = {
            "name": stock_name,
            "long_ema55": round(result['long_term']['ema55'], 2),
            "long_atr20": round(result['long_term']['atr20'], 2),
            "short_sma5": round(result['short_term_sma']['sma5'], 2),
            "short_ema8": round(result['short_term_ema']['ema8'], 2),
            "short_atr10": round(result['short_term_sma']['atr10'], 2),
            "last_date": result['date']
        }
        
        # 使用股票代码作为键
        json_data[result['ticker']] = stock_data
    
    # 转换为JSON字符串
    return json.dumps(json_data, ensure_ascii=False, indent=2)

def output_results_to_json(results, output_file=None):
    """
    将结果输出到JSON文件
    
    参数:
    results (list): 包含股票数据的列表
    output_file (str): 输出文件路径，默认为None，将在shared目录创建文件
    
    返回:
    str: JSON文件路径
    """
    # 格式化为JSON
    json_str = format_stock_data_to_json(results)
    json_data = json.loads(json_str)
    
    # 如果未指定输出文件，则在shared目录创建
    if output_file is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shared")
        # 确保shared目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 创建文件名
        current_date = datetime.now().strftime("%Y%m%d")
        output_file = os.path.join(output_dir, f"stock_safe_range_{current_date}.json")
        output_file2 = os.path.join(output_dir, f"stock_safe_range.json")
    
    # 在写入之前检查当前要写的内容和当前文件内容（即前一天计算结果）的diff
    has_significant_change = False
    
    # 检查前一天的结果文件是否存在
    if os.path.exists(output_file2):
        try:
            # 读取前一天的结果
            with open(output_file2, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                
            # 比较每支股票的数值变化
            for ticker, new_stock_data in json_data.items():
                if ticker in old_data:
                    old_stock_data = old_data[ticker]
                    
                    # 检查各项指标的变化
                    fields_to_check = [
                        "long_ema55",  
                        "short_ema8", 
                    ]
                    
                    for field in fields_to_check:
                        if field in new_stock_data and field in old_stock_data:
                            new_value = new_stock_data[field]
                            old_value = old_stock_data[field]
                            
                            # 避免除以零错误
                            if old_value != 0:
                                change_percent = abs((new_value - old_value) / old_value * 100)
                                
                                # 如果变化大于5%，记录到日志
                                if change_percent > 5:
                                    logger.warning(f"股票 {ticker} ({new_stock_data.get('name', '')}) 的 {field} 变化较大: {old_value} -> {new_value}, 变化率: {change_percent:.2f}%")
                                    has_significant_change = True
        except Exception as e:
            logger.error(f"比较数据时出错: {str(e)}")
    
    # 写入文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json_str)
    
    with open(output_file2, 'w', encoding='utf-8') as f:
        f.write(json_str)
    
    logger.info(f"结果已保存到: {output_file2}")
    
    # 如果没有任何变化大于5%的情况，则生成一个标记文件
    if not has_significant_change:
        mark_file = os.path.join(output_dir, f"safe_range_done_{current_date}")
        with open(mark_file, 'w', encoding='utf-8') as f:
            f.write(f"计算完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"成功计算了 {len(json_data)} 支股票的指标\n")
            f.write(f"没有发现任何股票的指标变化超过5%\n")
        logger.info(f"没有发现显著变化，已生成标记文件: {mark_file} 本次共完成 {len(json_data)} 支股票的指标计算")
    
    return output_file

def main():
    """
    主函数，处理股票列表并计算安全交易区间
    """
    # 股票代码列表
    #tickers = BJ50_Trust
    tickers = HS300 + BJ50_Trust
    if debug:
        tickers = ["832522.BJ"]

    
    # 初始化DataManager
    data_manager = DataManager()
    
    # 计算开始日期和结束日期
    end_date = datetime.now() - timedelta(days=1 )
    start_date = end_date - timedelta(days=CalendarDaysCount )
    
    # 格式化日期为字符串
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    trading_days = get_trading_days(start_date_str, end_date_str)
    #print(trading_days)
    trading_days_count = len(trading_days)
    
    # 检查历史数据是否准备好，如果没有则下载
    missing_tickers = []
    for ticker in tickers:
        data_dict = data_manager.get_local_daily_data(['close', 'high', 'low'], [ticker], start_date_str, end_date_str)
        if data_dict is None or ticker not in data_dict or data_dict[ticker].empty :
            logger.info(f"股票 {ticker} 的历史数据不存在，需要下载...")
            missing_tickers.append(ticker)
        if len(data_dict[ticker]) < trading_days_count:
            logger.info(f"股票 {ticker} 的历史数据长度:{len(data_dict[ticker])}不足 {trading_days_count} 天，需要下载...")
            #print(data_dict[ticker].index)
            missing_tickers.append(ticker)

    
    if missing_tickers:
        data_manager.download_data_sync(missing_tickers, '1d', start_date_str, end_date_str)
    time.sleep(len(missing_tickers)/50)  
    logger.info(f"股票 {ticker} 的历史数据下载完成")
    
    # 计算每个股票的安全交易区间
    results = []
    
    for ticker in tickers:
        result = compute_safe_range(ticker, start_date_str, end_date_str)
        if result:
            results.append(result)
            logger.info(f"已计算 {ticker} 的安全交易区间")
        else:
            logger.warning(f"无法计算 {ticker} 的安全交易区间")
    
    if debug:
        print(results)
        return results
    # 输出结果到JSON文件
    json_file = output_results_to_json(results)
    
    # 打印JSON文件路径
    logger.info(f"JSON文件已保存到: {json_file}")
    
    return results

if __name__ == "__main__":
    main()
