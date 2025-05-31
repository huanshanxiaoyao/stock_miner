from xtquant import xtdata
import pandas as pd
import os

def get_financial_data(stock_codes):
    """
    获取股票的财务数据并返回结构化的结果
    
    Args:
        stock_codes: 股票代码列表，如 ['000001.SZ', '600519.SH']
        
    Returns:
        dict: 包含所有股票财务数据的字典，格式为 {股票代码: {数据类型: DataFrame}}
    """
    return xtdata.get_financial_data(stock_codes)

def analyze_financial_data(financial_data):
    """
    分析财务数据，提取关键信息并返回结构化结果
    
    Args:
        financial_data: 从 get_financial_data 获取的财务数据
        
    Returns:
        dict: 包含分析结果的字典
    """
    results = {}
    
    for stock_code, data_types in financial_data.items():
        stock_result = {}
        
        # 处理各类财务数据
        for data_type, df in data_types.items():
            # 确保数据是 DataFrame 类型
            if not isinstance(df, pd.DataFrame):
                continue
                
            # 根据数据类型提取关键信息
            if data_type == 'Balance':  # 资产负债表
                stock_result['Balance'] = {
                    'latest_report': df.iloc[-1].to_dict() if len(df) > 0 else {},
                    'report_dates': df['m_timetag'].tolist() if 'm_timetag' in df.columns else [],
                    'total_rows': len(df),
                    'columns': df.columns.tolist()
                }
            elif data_type == 'Income':  # 利润表
                stock_result['Income'] = {
                    'latest_report': df.iloc[-1].to_dict() if len(df) > 0 else {},
                    'report_dates': df['m_timetag'].tolist() if 'm_timetag' in df.columns else [],
                    'total_rows': len(df),
                    'columns': df.columns.tolist()
                }
            elif data_type == 'CashFlow':  # 现金流量表
                stock_result['CashFlow'] = {
                    'latest_report': df.iloc[-1].to_dict() if len(df) > 0 else {},
                    'report_dates': df['m_timetag'].tolist() if 'm_timetag' in df.columns else [],
                    'total_rows': len(df),
                    'columns': df.columns.tolist()
                }
            elif data_type == 'Capital':  # 股本结构
                stock_result['Capital'] = {
                    'latest_report': df.iloc[-1].to_dict() if len(df) > 0 else {},
                    'report_dates': df['m_timetag'].tolist() if 'm_timetag' in df.columns else [],
                    'total_rows': len(df),
                    'columns': df.columns.tolist()
                }
            elif data_type == 'HolderNum':  # 股东人数
                stock_result['HolderNum'] = {
                    'latest_report': df.iloc[-1].to_dict() if len(df) > 0 else {},
                    'report_dates': df['endDate'].tolist() if 'endDate' in df.columns else [],
                    'total_rows': len(df),
                    'columns': df.columns.tolist()
                }
        
        results[stock_code] = stock_result
    
    return results

def print_financial_summary(analysis_results):
    """
    打印财务数据的摘要信息
    
    Args:
        analysis_results: 从 analyze_financial_data 获取的分析结果
    """
    for stock_code, data in analysis_results.items():
        print(f"\n{'='*50}")
        print(f"股票代码: {stock_code}")
        print(f"{'='*50}")
        
        for data_type, info in data.items():
            print(f"\n{'-'*20} {data_type} {'-'*20}")
            print(f"报告期数量: {info['total_rows']}")
            
            if info['report_dates']:
                print(f"最早报告期: {info['report_dates'][0]}")
                print(f"最新报告期: {info['report_dates'][-1]}")
            
            print(f"\n最新报告期数据摘要:")
            # 只打印部分关键字段
            key_fields = get_key_fields_for_data_type(data_type)
            for field in key_fields:
                if field in info['latest_report']:
                    print(f"  {field}: {info['latest_report'][field]}")

def get_key_fields_for_data_type(data_type):
    """
    根据数据类型返回关键字段列表
    
    Args:
        data_type: 数据类型
        
    Returns:
        list: 关键字段列表
    """
    if data_type == 'Balance':
        return ['m_timetag', 'm_anntime', 'm_totalAssets', 'm_totalLiabilities', 'm_totalEquity']
    elif data_type == 'Income':
        return ['m_timetag', 'm_anntime', 'revenue_inc', 'net_profit_inc', 'gross_profit']
    elif data_type == 'CashFlow':
        return ['m_timetag', 'm_anntime', 'm_netCashFlowsFromOperatingActivities', 'm_netCashFlowsFromInvestingActivities']
    elif data_type == 'Capital':
        return ['m_timetag', 'total_capital', 'circulating_capital', 'freeFloatCapital']
    elif data_type == 'HolderNum':
        return ['declareDate', 'endDate', 'shareholder', 'shareholderA']
    return []

def save_financial_data(financial_data, output_dir='financial_data'):
    """
    将财务数据保存为CSV文件
    
    Args:
        financial_data: 从 get_financial_data 获取的财务数据
        output_dir: 输出目录
    """
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for stock_code, data_types in financial_data.items():
        stock_dir = os.path.join(output_dir, stock_code)
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir)
        
        for data_type, df in data_types.items():
            if isinstance(df, pd.DataFrame):
                output_file = os.path.join(stock_dir, f"{data_type}.csv")
                df.to_csv(output_file, index=False, encoding='utf-8')
                print(f"已保存 {stock_code} 的 {data_type} 数据到 {output_file}")

def main():
    # 获取股票列表
    stock_list = ['000001.SZ', '600519.SH']
    
    # 获取财务数据
    print("正在获取财务数据...")
    financial_data = get_financial_data(stock_list)
    
    # 分析财务数据
    print("\n正在分析财务数据...")
    analysis_results = analyze_financial_data(financial_data)
    
    # 打印财务摘要
    print("\n财务数据摘要:")
    print_financial_summary(analysis_results)
    
    # 保存财务数据
    save_option = input("\n是否保存财务数据到CSV文件? (y/n): ")
    if save_option.lower() == 'y':
        save_financial_data(financial_data)

if __name__ == "__main__":
    main()