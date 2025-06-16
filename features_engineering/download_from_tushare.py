import os
import time
import tushare as ts
import pandas as pd
from download_tushare_helper import *

pro = ts.pro_api(token="34e154b48c608f6dd444cb1749b69828a4eec91c1065da4ff5fca6d7")
TuShareDataRoot = "./tushare_data"


def get_all_codes():
    outfile2 = TuShareDataRoot + "/codes_all.csv" 
    if os.path.exists(outfile2):
        df = pd.read_csv(outfile2)
        return df
    
    ret = download_all_codes(outfile2)
    return ret

def download_all_codes(save_path):
    df1 = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    df2 = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    df3 = pro.stock_basic(exchange='', list_status='P', fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    
    # 合并三个DataFrame
    df = pd.concat([df1, df2, df3], ignore_index=True)
    
    # 保存合并后的数据
    outfile2 = save_path
    df.to_csv(outfile2, index=False, encoding='utf-8')
    return df



# 1. 三大表
def download_balance(start_date, end_date):
    periods = get_periods(start_date, end_date)
    for period in periods:
        df_bs2 = pro.balancesheet_vip(period=period)
        outfile2 = TuShareDataRoot + "/balance/" + period + ".csv"
        df_bs2.to_csv(outfile2, index=False, encoding='utf-8')

    return 

def download_income(start_date, end_date):
    #df_bs1 = pro.income(ts_code=code)
    #outfile1 = TuShareDataRoot + "/income/" + code + ".csv"
    #df_bs1.to_csv(outfile1, index=False, encoding='utf-8')
    
    periods = get_periods(start_date, end_date)
    for period in periods:
        df_bs2 = pro.income_vip(period=period)
        outfile2 = TuShareDataRoot + "/income/" + period + ".csv"
        df_bs2.to_csv(outfile2, index=False, encoding='utf-8')
    return  

def download_cachflow(start_date, end_date):
    periods = get_periods(start_date, end_date)
    for period in periods:
        df_bs2 = pro.cashflow_vip(period=period)
        outfile2 = TuShareDataRoot + "/cashflow/" + period + ".csv"
        df_bs2.to_csv(outfile2, index=False, encoding='utf-8')
    return 


# 2. 每股指标
def download_pershare(start_date, end_date):
    periods = get_periods(start_date, end_date)
    for period in periods:
        df_bs2 = pro.fina_indicator_vip(period=period)
        outfile2 = TuShareDataRoot + "/pershare/" + period + ".csv"
        df_bs2.to_csv(outfile2, index=False, encoding='utf-8')
    return 

# 3. 股东信息
def download_holders(code_list, start_date, end_date):
    for idx, code in enumerate(code_list):
        df_num  = pro.stk_holdernumber(ts_code=code, start_date=start_date, end_date=end_date)
        outfile2 = TuShareDataRoot + "/holdernumber/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        df_top  = pro.top10_holders(ts_code=code, start_date=start_date, end_date=end_date)
        outfile2 = TuShareDataRoot + "/top10h/" + code + ".csv"
        df_top.to_csv(outfile2, index=False, encoding='utf-8')
        df_tflt = pro.top10_floatholders(ts_code=code, start_date=start_date, end_date=end_date)
        outfile2 = TuShareDataRoot + "/top10fh/" + code + ".csv"
        df_tflt.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 10 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec")
    return

def download_block_trade(code_list, start_date, end_date):
    """股票代码或交易日期至少指定一个，这里采用按股票代码"""
    """5000积分，每分钟400次访问呢"""
    """成交量的单位 万股"""
    for idx, code in enumerate(code_list):
        df_num  = pro.block_trade(ts_code=code)
        outfile2 = TuShareDataRoot + "/block_trade/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 7 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec")

def download_share_float(code_list, start_date, end_date):
    """5000积分，每分钟400次访问呢"""
    for idx, code in enumerate(code_list):
        df_num  = pro.share_float(ts_code=code)
        outfile2 = TuShareDataRoot + "/share_float/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 7 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec")    

def download_repurchase(start_date, end_date):
    time_points = get_time_points(start_date, end_date)
    
    for i  in range(len(time_points) - 1):
        df_num = pro.repurchase(ann_date='', start_date=time_points[i], end_date=time_points[i+1])
        outfile2 = TuShareDataRoot + "/repurchase/" + time_points[i] + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')

def download_margin_detail(code_list, start_date, end_date):
    for idx, code in enumerate(code_list):
        df_num = pro.margin_detail(ts_code=code)
        outfile2 = TuShareDataRoot + "/margin_detail/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 7 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec") 

def download_margin(start_date, end_date):
    dfs = []
    time_points = get_time_points(start_date, end_date)
    
    for i  in range(len(time_points) - 1):
        df = pro.margin(start_date=time_points[i], end_date=time_points[i+1])
        print(f"{time_points[i]}--{time_points[i+1]}")
        dfs.append(df)

        if i % 5 == 0:
            time.sleep(1)
            print(f" {i} done, sleep 1 sec") 
    
    # 合并所有数据框
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        result_df = result_df.sort_values(by='trade_date')
        outfile2 = TuShareDataRoot  + "/margin.csv"
        result_df.to_csv(outfile2, index=False, encoding='utf-8')
    else:
        print("没有获取到任何融资融券数据")


def download_shibor(start_date, end_date):
    time_points = get_time_points(start_date, end_date)
    
    for i  in range(len(time_points) - 1):
        df_num = pro.shibor(start_date=time_points[i], end_date=time_points[i+1])
        outfile2 = TuShareDataRoot + "/shibor/" + time_points[i] + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')

def download_m1m2(start_date, end_date):
    time_points = get_time_points(start_date, end_date)
    
    for i  in range(len(time_points) - 1):
        start_month = time_points[i][:6]
        end_month = time_points[i+1][:6]
        df_num = pro.cn_m(start_date=start_month, end_date=end_month)
        outfile2 = TuShareDataRoot + "/m1m2/" + time_points[i] + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')

def download_sf_month(start_date, end_date):
    time_points = get_time_points(start_date, end_date)

    
    for i  in range(0, len(time_points) - 11, 12):
        start_month = time_points[i][:6]
        end_month = time_points[i+11][:6]
        df_num = pro.sf_month(start_m=start_month, end_m=end_month)
        outfile2 = TuShareDataRoot + "/sf_month/" + time_points[i] + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')

def download_daily_basic(code_list, start_date, end_date):
    for idx, code in enumerate(code_list):
        df_num = pro.daily_basic(ts_code=code)
        outfile2 = TuShareDataRoot + "/daily_basic/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 7 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec") 

def download_ccass_hold_detail(code_list, start_date, end_date):
    """2024年5月停止了 公布该数据，"""
    """ ret=pro.ccass_hold_detail(ts_code="00686.HK") 港股可查"""
    for idx, code in enumerate(code_list):
        df_num = pro.ccass_hold_detail(ts_code=code)
        outfile2 = TuShareDataRoot + "/ccass_hold_detail/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 6 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec") 

def download_ccass_hold(code_list, start_date, end_date):
    for idx, code in enumerate(code_list):
        df_num = pro.ccass_hold(ts_code=code)
        outfile2 = TuShareDataRoot + "/ccass_hold/" + code + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        if idx % 9 == 0:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec") 

def dowload_ths_index(start_date, end_date):
    """因为没有板块成员的历史快照数据，所以之类不能作为因子"""
    df = pro.ths_index()
    outfile2 = TuShareDataRoot + "/ths_index.csv"
    df.to_csv(outfile2, index=False, encoding='utf-8')
    code_list = df['ts_code'].tolist()
    print(f"共获取到 {len(code_list)} 个股票代码")
    for idx, code in enumerate(code_list):
        df = pro.ths_daily(ts_code=code)
        outfile2 = TuShareDataRoot + "/ths_index/" + "daily_" + code + ".csv"
        df.to_csv(outfile2, index=False, encoding='utf-8')

        df = pro.ths_member(ts_code=code)
        outfile2 = TuShareDataRoot + "/ths_index/" + "member_" + code + ".csv"
        df.to_csv(outfile2, index=False, encoding='utf-8')

        if idx % 10 == 1:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec")

def download_industry_data(code_list):
    df = pro.index_classify(level='L3', src='SW2021')
    code_list = df['index_code'].tolist()
    dfs = []
    print(f"共获取到 {len(code_list)} 个股票代码")
    for idx, code in enumerate(code_list):
        df = pro.index_member_all(l3_code=code)
        dfs.append(df)
        if idx % 10 == 1:
            time.sleep(1)
            print(f" {idx} done, sleep 1 sec")

    outfile2 = TuShareDataRoot + "/industry_data"  + ".csv"
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(outfile2, index=False, encoding='utf-8')

def download_hs_const():
    """陆股通，也叫沪深通"""
    df1 = pro.hs_const(hs_type='SH')
    df2 = pro.hs_const(hs_type='SZ')
    df = pd.concat([df1, df2], ignore_index=True)
    outfile2 = TuShareDataRoot + "/hs_const"  + ".csv"
    df.to_csv(outfile2, index=False, encoding='utf-8')

def download_index(index_code, start_date, end_date):
    """下载指数的股票清单和权重，需要循环多次调用，目前看每个月有两周的数据，做不到每天准确更新"""
    df = pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)
    outfile2 = TuShareDataRoot + "/index"  + ".csv"
    df.to_csv(outfile2, index=False, encoding='utf-8')


def download_au_price(start_date, end_date):
    time_points = get_time_points(start_date, end_date)
    
    for i  in range(len(time_points) - 1):
        start_month = time_points[i][:6]
        end_month = time_points[i+1][:6]
        df_num = pro.sge_daily(ts_code='', start_date=start_month, end_date=end_month)
        outfile2 = TuShareDataRoot + "/au_price/" + time_points[i] + ".csv"
        df_num.to_csv(outfile2, index=False, encoding='utf-8')
        time.sleep(0.2)
        print(end_month)

if __name__ == "__main__":
    start_date = "20190101"
    end_date   = "20250601"
    #download_income(start_date, end_date)
    #download_balance(start_date, end_date)
    #download_cachflow(start_date, end_date)
    #download_pershare(start_date, end_date)
    #download_all_codes()
    df = get_all_codes()
    
    # 从df中提取股票代码列表
    code_list = df['ts_code'].tolist()
    print(f"共获取到 {len(code_list)} 个股票代码")
    #download_holders(code_list, start_date, end_date)
    #download_block_trade(code_list, start_date, end_date)
    #download_share_float(code_list, start_date, end_date)
    #download_repurchase(start_date, end_date)
    #download_margin_detail(code_list, start_date, end_date)
    #download_shibor(start_date, end_date)
    #download_m1m2(start_date, end_date)
    #download_sf_month(start_date, end_date)
    #download_daily_basic(code_list, start_date, end_date)
    #download_ccass_hold_detail(code_list, start_date, end_date)
    #download_ccass_hold(code_list, start_date, end_date)
    #dowload_ths_index(start_date, end_date)
    #download_industry_data(code_list)
    #download_hs_const()
    #download_index('399300.SZ', start_date, end_date)
    #download_au_price(start_date, end_date)

    download_margin(start_date, end_date)