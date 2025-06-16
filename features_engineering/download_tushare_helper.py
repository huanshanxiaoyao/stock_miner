import pandas as pd
from datetime import datetime

def get_periods(start_date, end_date):
    periods = []
    
    # 将起止日期转换为年和月
    start_year = int(start_date[:4])
    start_month = int(start_date[4:6])
    end_year = int(end_date[:4])
    end_month = int(end_date[4:6])
    
    # 确定起始季度
    if start_month <= 3:
        start_quarter = 1
    elif start_month <= 6:
        start_quarter = 2
    elif start_month <= 9:
        start_quarter = 3
    else:
        start_quarter = 4
    
    # 确定结束季度
    if end_month <= 3:
        end_quarter = 4
        end_year = end_year - 1
    elif end_month <= 6:
        end_quarter = 1
    elif end_month <= 9:
        end_quarter = 2
    else:
        end_quarter = 3
    
    # 生成所有季度的截止日期
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            # 跳过起始日期之前的季度
            if year == start_year and quarter < start_quarter:
                continue
            # 跳过结束日期之后的季度
            if year == end_year and quarter > end_quarter:
                continue
            
            # 根据季度生成对应的截止日期
            if quarter == 1:
                period = f"{year}0331"
            elif quarter == 2:
                period = f"{year}0630"
            elif quarter == 3:
                period = f"{year}0930"
            else:  # quarter == 4
                period = f"{year}1231"
            
            periods.append(period)
    
    return periods


def get_time_points(start_date, end_date):
    """生成日期序列，包含起始日期、结束日期和中间每月1号"""
    
    # 转换日期格式
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    # 创建月初日期序列
    date_range = pd.date_range(
        start=start.replace(day=1) + pd.offsets.MonthBegin(1 if start.day == 1 else 0),
        end=end.replace(day=1),
        freq='MS'  # 月初
    )
    
    # 转换为YYYYMMDD格式的字符串列表
    time_points = [start.strftime("%Y%m%d")] + \
                 [d.strftime("%Y%m%d") for d in date_range] + \
                 [end.strftime("%Y%m%d")]
    
    # 如果起始日期和结束日期相同，或者起始日期已经是月初且下一个月初就是结束日期，则去重
    return sorted(list(set(time_points)))