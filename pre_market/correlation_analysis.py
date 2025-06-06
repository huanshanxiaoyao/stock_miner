# coding:utf-8
import os
import json
import sys
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import time

from datetime import datetime, timedelta

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import BJ50,BJ50_BLACKLIST 
from date_utils import get_trading_days
from simple_log import init_logging

MaxSimilarStockCount = 4
CalendarDaysCount = 180
logger = init_logging("correlation_analysis")
# 初始化DataManager
data_manager = DataManager()

#临时加了几个本策略特有的黑名单，
BLACKLIST = BJ50_BLACKLIST + ["833533.BJ", "832982.BJ", "835438.BJ"]


def calculate_stock_correlation(stock_a, stock_b, start_date, end_date):
    """
    计算主函数
    计算两只股票之间的收盘价和成交量的皮尔逊相关系数
    
    参数:
    data_manager: DataManager实例
    stock_a: 第一只股票代码
    stock_b: 第二只股票代码
    start_date: 开始日期
    end_date: 结束日期，默认为当前日期
    
    返回:
    包含相关系数和p值的字典，如果计算失败则返回None
    """

    trading_days = get_trading_days(start_date, end_date)
    latest_trading_day = trading_days[-1]
    trading_day_count = len(trading_days)
    
    # 获取两只股票的数据
    codes = [stock_a, stock_b]
    data = data_manager.get_local_daily_data(['close', 'volume'], codes, start_date, end_date)

    #logger.info(f"数据获取, {codes},{start_date},{end_date}")
    
    if data is None or stock_a not in data or stock_b not in data:
        logger.error(f"数据获取失败，股票代码: {stock_a}, {stock_b}")
        return None
    
    if data[stock_a].empty or data[stock_b].empty:
        logger.error(f"数据为空，股票代码: {stock_a}, {stock_b}")
        return None
    
    # 提取收盘价和成交量
    a_close = data[stock_a]['close']
    a_volume = data[stock_a]['volume']
    b_close = data[stock_b]['close']
    b_volume = data[stock_b]['volume']
    
    # 确保两个时间序列有共同的索引
    common_index = a_close.index.intersection(b_close.index)
    
    if len(common_index) < trading_day_count:
        logger.error(f"数据不足，股票代码: {stock_a}, {stock_b}, {len(a_close)},{len(b_close)},{len(common_index)}")
        return None
    
    # 对齐数据
    a_close_aligned = a_close.loc[common_index]
    b_close_aligned = b_close.loc[common_index]
    a_volume_aligned = a_volume.loc[common_index]
    b_volume_aligned = b_volume.loc[common_index]
    
    # 计算价格比值的对数序列 Xt = ln(北交所股票价格/A股股票价格)
    try:
        # 确保stock_a是北交所股票，stock_b是A股股票
        if stock_a.endswith('.BJ'):
            # 北交所股票价格/A股股票价格
            price_ratio = a_close_aligned / b_close_aligned
        else:
            # 如果顺序相反，则计算b/a
            price_ratio = b_close_aligned / a_close_aligned
            
        log_price_ratio = np.log(price_ratio)
        
        # 确保有足够的数据点计算z-score
        if len(log_price_ratio) > 61:  # 需要至少61个点(60个历史点+1个当前点)
            # 获取最新的数据点
            latest_point = log_price_ratio.iloc[-1]
            
            # 获取最新数据点的日期 - 处理不同类型的索引
            try:
                if hasattr(log_price_ratio.index[-1], 'strftime'):
                    latest_date = log_price_ratio.index[-1].strftime('%Y%m%d')
                else:
                    # 如果索引不是datetime对象，直接转为字符串
                    latest_date = str(log_price_ratio.index[-1])
                
                # 检查latest_date是否是latest_trading_day
                if latest_date != latest_trading_day:
                    logger.error(f"股票 {stock_a}, {stock_b} 的最新数据日期 {latest_date} 不是最新交易日 {latest_trading_day}，退出计算")
                    return None
                
                logger.debug(f"股票 {stock_a}, {stock_b} 的最新数据日期: {latest_date}")
                
            except Exception as e:
                logger.error(f"获取日期时出错: {str(e)}, 股票代码: {stock_a}, {stock_b}")
                latest_date = "未知日期"
                return None
            
            # 获取倒数第2个到倒数第61个数据点(共60个点)用于计算均值和标准差
            historical_points = log_price_ratio.iloc[-61:-1]
            
            # 记录用于计算均值和标准差的60个数据点
            logger.debug(f"股票 {stock_a}, {stock_b} 的历史数据点(用于计算均值和标准差):")
            for i, (date, value) in enumerate(historical_points.items()):
                date_str = date.strftime('%Y%m%d') if hasattr(date, 'strftime') else str(date)
                logger.debug(f"  点 {i+1}: 日期={date_str}, 值={value:.6f}")
            
            # 计算均值和标准差
            mean_value = historical_points.mean()
            std_value = historical_points.std()
            
            logger.debug(f"股票 {stock_a}, {stock_b} 的均值: {mean_value:.6f}, 标准差: {std_value:.6f}")
            
            # 计算z-score
            if std_value != 0:  # 避免除以零
                z_score = (latest_point - mean_value) / std_value
                # 保存均值和标准差，方便后续使用
                ratio_mean = mean_value
                ratio_std = std_value
                logger.debug(f"股票 {stock_a}, {stock_b} 的z-score: {z_score:.6f}")
            else:
                z_score = 0
                ratio_mean = mean_value
                ratio_std = 0
                logger.warning(f"股票 {stock_a}, {stock_b} 的标准差为0，z-score设为0")
        else:
            logger.error(f"股票 {stock_a}, {stock_b} 的数据点不足，无法计算z-score，需要至少61个点，当前只有 {len(log_price_ratio)} 个点")
            z_score = None
            latest_date = None
    except Exception as e:
        logger.error(f"计算价格比值对数序列时出错: {str(e)}, 股票代码: {stock_a}, {stock_b}")
        z_score = None
        latest_date = None
    
    try:
        # 计算收盘价的相关系数
        close_corr, close_p = pearsonr(a_close_aligned, b_close_aligned)
        
        # 计算成交量的相关系数
        volume_corr, volume_p = pearsonr(a_volume_aligned, b_volume_aligned)
        
        result = {
            'close_corr': close_corr,
            'close_p_value': close_p,
            'volume_corr': volume_corr,
            'volume_p_value': volume_p,
            'data_points': len(common_index)
        }
        
        # 添加z-score到结果中
        if z_score is not None:
            result['price_ratio_z_score'] = z_score
            result['price_ratio_mean'] = ratio_mean
            result['price_ratio_std'] = ratio_std
            result['latest_date'] = latest_date
        
        return result
    except Exception as e:
        logger.error(f"计算相关系数时出错: {str(e)}")
        return None

def prepare_industry_data():
    """
    准备行业分类数据，返回行业与股票代码的映射
    
    返回:
    包含行业和股票代码的字典
    """
    # 读取行业分类文件
    industry_file = os.path.join(os.path.dirname(__file__), "../A2BJ/industry_classification.txt")
    
    if not os.path.exists(industry_file):
        logger.error(f"文件不存在: {industry_file}")
        return {}
    
    # 存储行业分类的字典
    industry_data = {}
    
    # 读取行业分类文件
    with open(industry_file, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                industry = parts[0]
                try:
                    codes = json.loads(parts[2])
                    
                    # 分离北交所股票和A股股票
                    bj_codes = [code for code in codes if code.endswith('.BJ')]
                    a_codes = [code for code in codes if code.endswith('.SH') or code.endswith('.SZ')]
                    
                    if bj_codes and a_codes:
                        industry_data[industry] = {
                            'bj_codes': bj_codes,
                            'a_codes': a_codes
                        }
                except json.JSONDecodeError:
                    logger.error(f"解析JSON失败: {parts[2]}")
    
    return industry_data

def calculate_correlations():
    """
    业务主函数
    读取industry_classification.txt，计算同一分组内北交所股票与A股股票的皮尔逊相关系数
    """

    # 设置日期范围
    start_date = (datetime.now() - timedelta(days=CalendarDaysCount)).strftime("%Y%m%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    # 准备行业数据
    industry_data = prepare_industry_data()
    
    if not industry_data:
        logger.warning("没有找到有效的行业分类数据")
        return
    
    # 存储结果的字典
    results = {}
    
    # 处理每个行业
    for industry, codes in industry_data.items():
        bj_codes = codes['bj_codes']
        a_codes = codes['a_codes']
        
        logger.info(f"处理行业: {industry}")
        logger.info(f"  北交所股票数量: {len(bj_codes)}")
        logger.info(f"  A股股票数量: {len(a_codes)}")
        
        # 计算每个北交所股票与每个A股股票的相关系数
        industry_results = []
        
        for bj_code in bj_codes:
            bj_results = {}
            
            for a_code in a_codes:
                # 计算相关系数
                corr_result = calculate_stock_correlation(
                    bj_code, a_code, start_date, end_date
                )
                
                if corr_result:
                    bj_results[a_code] = corr_result
            
            if bj_results:  # 如果有结果
                # 按收盘价相关系数排序
                sorted_results = sorted(
                    bj_results.items(), 
                    key=lambda x: x[1]['close_corr'], 
                    reverse=True
                )
                
                industry_results.append({
                    'bj_code': bj_code,
                    'correlations': sorted_results
                })
        
        if industry_results:
            results[industry] = industry_results
    
    # 输出结果到文件
    output_correlation_results(results)

def output_correlation_results(results):
    """
    将相关性分析结果输出到文件
    
    参数:
    results: 包含相关性分析结果的字典
    """
    # 输出结果
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shared")
    # 确保shared目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, "correlation_results.txt")
    
    # 读取股票代码和名称的对应关系
    stock_names = {}
    
    # 读取北交所股票代码和名称
    bj_file = os.path.join(os.path.dirname(__file__), "../A2BJ/codeB2industry")
    if os.path.exists(bj_file):
        with open(bj_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    code = parts[0]
                    name = parts[1]
                    stock_names[code] = name
    
    # 读取A股股票代码和名称
    a_file = os.path.join(os.path.dirname(__file__), "../A2BJ/codeA2industry")
    if os.path.exists(a_file):
        with open(a_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    code = parts[0]
                    name = parts[1]
                    stock_names[code] = name
    
    # 创建JSON格式的相似度数据
    similarity_data = {}
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for industry, industry_results in results.items():
            f.write(f"行业: {industry}\n")
            f.write("=" * 50 + "\n")
            
            for bj_result in industry_results:
                bj_code = bj_result['bj_code']
                bj_name = stock_names.get(bj_code, "")
                f.write(f"北交所股票: {bj_code}({bj_name})\n")
                f.write("-" * 40 + "\n")
                
                # 为JSON输出准备数据
                similar_stocks = []
                
                # 输出前5个相关性最高的A股
                # 目前有订阅数量不能超过100的限制，所以下面做了一些过滤，减少生成数量
                for i, (a_code, corr_data) in enumerate(bj_result['correlations'][:4]):
                    a_name = stock_names.get(a_code, "")
                    if corr_data['close_corr'] < 0.5:#TODO
                        continue
                    f.write(f"  排名 {i+1}: {a_code}({a_name})\n")
                    f.write(f"    收盘价相关系数: {corr_data['close_corr']:.4f} (p值: {corr_data['close_p_value']:.4f})\n")
                    f.write(f"    成交量相关系数: {corr_data['volume_corr']:.4f} (p值: {corr_data['volume_p_value']:.4f})\n")
                    f.write(f"    共同交易日数量: {corr_data['data_points']}\n")
                    
                    # 添加z-score输出
                    if 'price_ratio_z_score' in corr_data:
                        f.write(f"    价格比值z-score: {corr_data['price_ratio_z_score']:.4f} (日期: {corr_data['latest_date']})\n")
                    
                    # 收集相似度大于0.6的股票
                    if corr_data['close_corr'] >= 0.65:#TODO
                        stock_info = {
                            "code": a_code,
                            "similarity": round(corr_data['close_corr'], 4)
                        }
                        
                        # 添加z-score到JSON输出
                        if 'price_ratio_z_score' in corr_data:
                            stock_info["z_score"] = round(corr_data['price_ratio_z_score'], 4)
                            stock_info["mean"] = round(corr_data['price_ratio_mean'], 4)
                            stock_info["std"] = round(corr_data['price_ratio_std'], 4)
                            stock_info["date"] = corr_data['latest_date']
                        similar_stocks.append(stock_info)
                
                f.write("\n")
                
                # 将相似股票添加到数据中
                if similar_stocks and len(similar_stocks) >= 2 and bj_code in BJ50 and bj_code not in BLACKLIST:
                    similarity_data[bj_code] = {
                        "name": bj_name,
                        "similar_stocks": similar_stocks[:MaxSimilarStockCount]
                    }
            
            f.write("\n\n")
    
    # 统计北交所股票和相似股票的数量
    total_bj_codes = len(similarity_data)
    all_similar_codes = set()
    for bj_data in similarity_data.values():
        for stock in bj_data['similar_stocks']:
            all_similar_codes.add(stock['code'])
    total_similar_codes = len(all_similar_codes)
    
    logger.info(f"统计结果:")
    logger.info(f"  - 具有相似股票的北交所股票数量: {total_bj_codes}")
    logger.info(f"  - 去重后的相似A股数量: {total_similar_codes}")
    
    # 输出JSON格式的相似度数据
    json_output_file = os.path.join(output_dir, "correlation_results.json")
    with open(json_output_file, 'w', encoding='utf-8') as f:
        json.dump(similarity_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"相关性分析结果已保存到: {output_file}")
    logger.info(f"JSON格式的相似度数据已保存到: {json_output_file}")

def check_data_ready():
    """
    检查数据完整性，确保所有股票从过去180天至今的天级数据完整
    """
    logger.info("开始检查数据完整性...")
    
    # 设置日期范围 - 获取180天前的日期
    start_date = (datetime.now() - timedelta(days=CalendarDaysCount)).strftime("%Y%m%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # 获取180天前的交易日作为起始日期
    trading_days = get_trading_days(start_date, end_date)
    trading_day_count = len(trading_days)
    
    logger.info(f"数据检查日期范围: {start_date} 至 {end_date}")
    logger.info(f"交易日总数: {len(trading_days)}")
    
    # 准备行业数据
    industry_data = prepare_industry_data()
    
    if not industry_data:
        logger.error("没有找到有效的行业分类数据")
        return False
    
    # 收集所有股票代码
    all_codes = set()
    for industry, codes in industry_data.items():
        all_codes.update(codes['bj_codes'])
        all_codes.update(codes['a_codes'])
    
    logger.info(f"共需检查 {len(all_codes)} 只股票的数据完整性")
    
    # 检查数据完整性
    incomplete_stocks = []
    missing_stocks = []
    
    # 批量检查，每次检查100只股票
    batch_size = 100
    code_batches = [list(all_codes)[i:i+batch_size] for i in range(0, len(all_codes), batch_size)]
    
    
    for batch_idx, code_batch in enumerate(code_batches):
        #time.sleep(0.1)  # 每批次检查后暂停1秒
        logger.info(f"检查批次 {batch_idx+1}/{len(code_batches)}，共 {len(code_batch)} 只股票")
        
        # 获取数据
        data = data_manager.get_local_daily_data(['close', 'volume'], code_batch, start_date, end_date)
        if data is None:
            logger.error("数据获取失败")
            missing_stocks.extend(code_batch)
            continue
        
        # 检查每只股票的数据
        for code in code_batch:
            if code not in data:
                logger.error(f"  股票 {code} 数据不存在")
                missing_stocks.append(code)
                continue
            
            if data[code].empty:
                logger.error(f" {code} 数据为空")
                missing_stocks.append(code)
                continue
            
            # 检查数据点数量
            data_points = len(data[code])
            #logger.info(f"  股票 {code} 数据点数量: {data_points}, 时间索引: {len(data[code].index)}")
            if data_points < trading_day_count:
                logger.warning(f"  股票 {code} 数据不足，仅有 {data_points} 条记录, 预期: {trading_day_count} 条记录")
                incomplete_stocks.append((code, data_points))
    
    # 输出检查结果
    if not missing_stocks and not incomplete_stocks:
        logger.info("所有股票数据完整，可以进行相关性分析")
        return True
    
    # 如果有缺失或不完整的数据，尝试下载
    if missing_stocks or incomplete_stocks:
        logger.warning("\n发现数据不完整的股票，尝试下载数据...")
        
        # 合并需要下载的股票代码
        download_codes = missing_stocks + [code for code, _ in incomplete_stocks]
        logger.info(f"需要下载 {len(download_codes)} 只股票的数据")
        
        # 下载数据
        if download_codes:
            logger.info("开始下载数据...")
            data_manager.download_data_async(download_codes, '1d', start_date)
            logger.info("数据下载完成，请稍后再次运行程序检查数据完整性")
            return False
    
    return len(missing_stocks) == 0 and len(incomplete_stocks) == 0

def test_pair():
    stock_a = "839946.BJ"
    stock_b = "002239.SZ"
    start_date = (datetime.now() - timedelta(days=CalendarDaysCount)).strftime("%Y%m%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    data_manager.download_data_async([stock_a, stock_b], '1d', start_date, end_date)
    ret = calculate_stock_correlation(data_manager, stock_a, stock_b, start_date, end_date)
    print(ret)

if __name__ == "__main__":
    #calculate_correlations()
    #test_pair()
    #check_data_ready()
    #sys.exit()
    if check_data_ready():
        calculate_correlations()
        pass
    else:
        logger.warning("数据准备不完整，请确保所有股票数据已下载且完整后再运行")
    sys.exit()
    
