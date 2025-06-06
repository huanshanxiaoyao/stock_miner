"""
自算 A 股前复权 / 后复权因子
author: you
"""
from pathlib import Path
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import tushare as ts
import time
from utils import get_all_stock_codes

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import DataManager
from stock_code_config import HS300

# 初始化DataManager实例
DM = DataManager()

# 设置默认的开始和结束日期
START_DAY = "20200101"  # 可以根据需要调整
END_DAY = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

# 本地缓存目录
ACTIONS_CACHE_DIR = Path("data/actions")
ACTIONS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

pro = ts.pro_api(token="34e154b48c608f6dd444cb1749b69828a4eec91c1065da4ff5fca6d7")

# ---------------------------------------------------------------------
# 工具函数：读取本地或 API 数据（使用DataManager获取数据）
# ---------------------------------------------------------------------
def load_daily_price(ts_code: str) -> pd.DataFrame:
    """
    返回列: trade_date(str, YYYYMMDD), close(float)
    必须连续自然日，有停牌就同价填充或 forward-fill
    """
    fields = ['close']
    raw_dict = DM.get_local_daily_data(fields, [ts_code], START_DAY, END_DAY, "none")
    
    if not raw_dict:
        print(f"[WARN] DataManager returned empty dict for {ts_code}")
        return pd.DataFrame()
    
    # 处理返回的数据格式
    if ts_code in raw_dict:  # 股票→DF 结构
        df = raw_dict[ts_code].reset_index().rename(columns={'index': 'trade_date'})
        df['trade_date'] = df['trade_date'].astype(str)
    else:  # 字段→DF 结构
        if 'close' in raw_dict and ts_code in raw_dict['close'].index:
            df = pd.DataFrame({
                'close': raw_dict['close'].loc[ts_code]
            })
            df.index.name = 'trade_date'
            df.reset_index(inplace=True)
            df['trade_date'] = df['trade_date'].astype(str)
        else:
            print(f"[WARN] No data found for {ts_code}")
            return pd.DataFrame()
    
    return df


def load_actions(ts_code: str) -> pd.DataFrame:
    """
    获取股票分红送股数据，优先从本地缓存读取，如果数据不存在或不是最新的则从Tushare下载
    返回列: ex_date(str), cash_div(float, 每股现金)、split_ratio(float, >=1.0)
    cash_div 单位: 元/股；split_ratio = 除权后总股本 / 除权前
    """
    cache_file = ACTIONS_CACHE_DIR / f"{ts_code}.csv"
    
    # 检查本地缓存是否存在且是最新的
    need_download = True
    if cache_file.exists():
        try:
            # 检查文件修改时间
            file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            
            # 读取本地缓存
            cached_df = pd.read_csv(cache_file)
            
            # 检查数据是否为空
            if not cached_df.empty:
                # 更智能的缓存策略：
                # 1. 如果文件是今天创建/修改的，直接使用
                # 2. 如果是工作日且文件是昨天的，重新下载（可能有新公告）
                # 3. 如果是周末，允许使用2天内的缓存
                
                now = datetime.now()
                days_old = (now - file_mtime).days
                
                # 今天的缓存直接使用
                if days_old == 0:
                    print(f"[INFO] Using today's cached dividend data for {ts_code}")
                    need_download = False
                # 工作日：只使用当天缓存
                elif now.weekday() < 5:  # 周一到周五
                    if days_old <= 1 and now.hour < 9:  # 早上9点前可以使用昨天的
                        print(f"[INFO] Using recent cached dividend data for {ts_code}")
                        need_download = False
                # 周末：可以使用2天内的缓存
                else:
                    if days_old <= 2:
                        print(f"[INFO] Using weekend cached dividend data for {ts_code}")
                        need_download = False
                        
                if not need_download:
                    return process_dividend_data(cached_df)
                    
        except Exception as e:
            print(f"[WARN] Error reading cached data for {ts_code}: {e}")
    
    if need_download:
        print(f"[INFO] Downloading dividend data for {ts_code} from Tushare")
        try:
            # 从Tushare Pro获取分红送股数据
            # 获取更长时间范围的数据，确保完整性
            dividend_df = pro.dividend(
                ts_code=ts_code,
                fields='ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date'
            )
            time.sleep(0.5)  # 防止请求过快
            if dividend_df.empty:
                print(f"[WARN] No dividend data found for {ts_code}")
                # 创建空的DataFrame但保存到缓存避免重复请求
                empty_df = pd.DataFrame(columns=['ex_date', 'cash_div', 'split_ratio'])
                empty_df.to_csv(cache_file, index=False)
                return empty_df
            
            # 保存原始数据到缓存
            dividend_df.to_csv(cache_file, index=False)
            print(f"[INFO] Saved dividend data for {ts_code} to cache")
            
            return process_dividend_data(dividend_df)
            
        except Exception as e:
            print(f"[ERROR] Failed to download dividend data for {ts_code}: {e}")
            # 如果下载失败且有缓存，使用缓存数据
            if cache_file.exists():
                try:
                    cached_df = pd.read_csv(cache_file)
                    print(f"[INFO] Fallback to cached data for {ts_code}")
                    return process_dividend_data(cached_df)
                except:
                    pass
            # 返回空DataFrame
            return pd.DataFrame(columns=['ex_date', 'cash_div', 'split_ratio'])

def process_dividend_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理Tushare dividend数据，转换为所需格式
    """
    print(df)
    if df.empty:
        return pd.DataFrame(columns=['ex_date', 'cash_div', 'split_ratio'])
    
    # 只保留有除权除息日期的记录
    df = df[df['ex_date'].notna()].copy()
    
    if df.empty:
        return pd.DataFrame(columns=['ex_date', 'cash_div', 'split_ratio'])
    
    # 处理现金分红：cash_div已经是元/股
    df['cash_div'] = df['cash_div'].fillna(0.0)
    
    # 处理送转股比例
    # stk_div: 送股比例（每10股送X股）
    # stk_bo_rate: 转股比例（每10股转X股） 
    # 计算总的股本扩张比例
    df['stk_div'] = df['stk_div'].fillna(0.0)
    # 修改后
    df['stk_bo_rate'] = df['stk_bo_rate'].astype(float).fillna(0.0)
    
    df["split_ratio"] = 1.0 + (df["stk_div"] +
                               df["stk_bo_rate"] +
                               df["stk_co_rate"]) / 10.0
    # 确保split_ratio >= 1.0
    df['split_ratio'] = df['split_ratio'].clip(lower=1.0)
    
    out = df[["ex_date", "cash_div", "split_ratio"]].copy()
    out["ex_date"] = out["ex_date"].astype(str).str.split(".").str[0]
    return out.sort_values("ex_date").reset_index(drop=True)

# ---------------------------------------------------------------------
# 核心：计算复权因子
# ---------------------------------------------------------------------
def calc_adj_factor(ts_code: str, latest_as_1: bool = False) -> pd.DataFrame:
    """
    复权因子计算
    Parameters
    ----------
    latest_as_1 : bool, default True
        • True  —— 前复权：最新交易日因子 = 1（常用于内部回测）  
        • False —— 与 Tushare adj_factor 对齐：最早交易日因子 = 1
    Returns
    -------
    DataFrame [trade_date, adj_factor]
    """
    price = load_daily_price(ts_code).sort_values("trade_date")
    act   = load_actions(ts_code)[["ex_date", "cash_div", "split_ratio"]]

    # 缺字段默认值
    act["cash_div"] = act["cash_div"].fillna(0.0)
    act["split_ratio"] = act["split_ratio"].fillna(1.0)


    # 合并：把企业行为并到日线
    df = price.merge(
        act.rename(columns={"ex_date": "trade_date"}),
        on=["trade_date"],
        how="left"
    ).fillna({"cash_div": 0.0, "split_ratio": 1.0}).sort_values("trade_date")


    n = len(df)
    factors = np.ones(n)                 # 最新交易日因子 = 1
    for i in range(n-1, 0, -1):          # 从后往前递推
        pre_close = df.at[i-1, "close"]      # 前一交易日原始价
        cash      = df.at[i,   "cash_div"]   # 当天 (i) 现金分红
        split_r   = df.at[i,   "split_ratio"]# 当天 (i) 送转比例
        # 因子递推公式（始终 ≤1）
        factors[i-1] = factors[i] * (pre_close - cash) / pre_close / split_r

    df["adj_factor_fwd"] = factors
    df["adj_factor_bwd"] = factors / factors[0]
    df["adj_factor"]     = df["adj_factor_fwd"] if latest_as_1 \
                                                else df["adj_factor_bwd"]
    return df[["trade_date", "adj_factor"]]

# ---------------------------------------------------------------------
# 单元测试：示例对比 Tushare 官方因子
# ---------------------------------------------------------------------
def validate_against_tushare(ts_code: str,
                             pro,
                             tol: float = 1e-6) -> None:
    """
    pro: tushare.pro_api() 实例
    """
    df_local = calc_adj_factor(ts_code, latest_as_1=False)
    df_tu    = pro.adj_factor(ts_code=ts_code)
    print(df_tu)
    merged   = df_local.merge(df_tu, on="trade_date", how="inner",
                              suffixes=("_local", "_tu"))
    print(merged)
    merged["diff"] = np.abs(merged["adj_factor_local"] - merged["adj_factor_tu"])
    max_diff = merged["diff"].max()
    if max_diff > tol:
        raise ValueError(f"{ts_code} adj_factor mismatch, max diff={max_diff}")
    print(f"[PASS] {ts_code}: max diff={max_diff:.2e}")

# ---------------------------------------------------------------------
# CLI / Cron 入口
# ---------------------------------------------------------------------
def main():
    all_stocks_df = get_all_stock_codes() 
    codes = all_stocks_df['ts_code'].tolist() if not all_stocks_df.empty else [p.stem for p in Path("data/price").glob("*.csv")]
    codes = HS300
    out_dir = Path("feature_table/adj_factor"); out_dir.mkdir(exist_ok=True)
    for code in codes:
        adj = calc_adj_factor(code, latest_as_1=False)
        adj.to_parquet(out_dir / f"{code}.parquet", index=False)

def validate(codes):
    for code in codes:
        validate_against_tushare(code, pro)

if __name__ == "__main__":
    #main()
    codes = ["601088.SH", "300760.SZ", "300750.SZ","000651.SZ", "000001.SZ", "600036.SH"]
    validate(codes)