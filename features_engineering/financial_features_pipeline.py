import sys
import os
import time
import pandas as pd
import numpy as np
from functools import reduce
from xtquant import xtdata

import pyarrow.dataset as ds
import pyarrow.compute as pc

from config import *
from financial_pipeline_helper import *

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simple_log import init_logging
logger = init_logging('financial_features_pipeline')

def get_financial_data(stock_codes):
    return xtdata.get_financial_data(stock_codes)

###############################################################################
# Core pipeline
###############################################################################

def merge_financial_tables(tables: dict, stock_code: str = None) -> pd.DataFrame:
    """Merge multiple raw financial DataFrames (quarter frequency) on `m_anntime`.

    Each DataFrame is expected to contain an announcement‑date column `m_anntime`.
    Columns are prefixed by their table name (lower‑case) to avoid collision.

    Parameters
    ----------
    tables : dict[str, pd.DataFrame]
        Mapping of table name → DataFrame as returned by XtData.
    stock_code : str, optional
        Stock code for saving the merged data to CSV file.

    Returns
    -------
    pd.DataFrame
        Wide quarterly DataFrame indexed by announcement date.
    """
    dfs = []
    for tbl_name, df in tables.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
          
        # 删除全为0或空的列
        df = remove_empty_columns(df)

        # 处理Top10FlowHolder和Top10Holder
        if tbl_name in ['Top10FlowHolder', 'Top10Holder']:
            df = process_top10_holders(df, tbl_name)
            
        # 检查并处理列名：如果缺少 m_anntime 但有 declareDate，则重命名
        if "m_anntime" not in df.columns:
            if "declareDate" in df.columns:
                df = df.rename(columns={"declareDate": "m_anntime"})
            else:
                # 兼容 HolderNum / Top10holder 等使用其他时间列的情况，跳过本步骤
                continue
                
        # 检查并处理列名：如果缺少 m_timetag 但有 endDate，则重命名
        if "m_timetag" not in df.columns:
            if "endDate" in df.columns:
                df = df.rename(columns={"endDate": "m_timetag"})
            else:
                # 兼容使用其他时间列的情况，跳过本步骤
                continue
                
        df = df.sort_values("m_anntime").reset_index(drop=True)
        dfs.append(add_prefix(df, tbl_name.lower()))
    if not dfs:
        return pd.DataFrame()
    merged = reduce(lambda left, right: pd.merge(left, right, on="m_anntime", how="outer"), dfs)
    merged.sort_values("m_anntime", inplace=True)
    merged.reset_index(drop=True, inplace=True)
    
    print(merged)
    
    # 保存合并后的表格为CSV文件
    if stock_code:
        # 创建保存路径
        save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "financial_data")
        # 确保目录存在
        os.makedirs(save_dir, exist_ok=True)
        # 构建文件名
        file_name = f"{stock_code}_fdata.csv"
        # 构建完整的文件路径
        file_path = os.path.join(save_dir, file_name)
        # 保存为CSV文件
        merged.to_csv(file_path, index=False)
        logger.info(f"已将合并后的财务数据保存到: {file_path}")
    
    return merged

def preprocess_financial_data(financial_data: dict) -> dict:
    """Stage‑1: Convert snapshot reports → time‑series (per stock).

    For each stock code we
      1) merge Balance/Income/CashFlow/PershareIndex/Capital into one DataFrame;
      2) compute TTM aggregates for key P&L and CashFlow items;
      3) add YoY & QoQ growth rates.

    Parameters
    ----------
    financial_data : dict
        {stock_code: {table_name: DataFrame}}

    Returns
    -------
    dict[str, pd.DataFrame]
        {stock_code: preprocessed quarterly DataFrame}
    """
    preprocessed = {}
    KEY_ITEMS = {
        # Income表
        "income_revenue_inc": "rev_ttm",
        "income_net_profit_excl_min_int_inc": "np_ttm",
        "income_oper_profit": "op_ttm",
        "income_net_profit_incl_min_int_inc_after": "np_ttm_after",
        # CashFlow表
        "cashflow_net_cash_flows_oper_act": "ocf_ttm",
    }
    
    for code, tables in financial_data.items():
        
        df_q = merge_financial_tables(tables, stock_code=code)
        if df_q.empty:
            continue

        # Calculate TTM for key items
        for col, new_col in KEY_ITEMS.items():
            if col in df_q.columns:
                # 打印非零值和对应的日期
                non_zero_data = df_q[['m_anntime', col]].loc[df_q[col] != 0]
                print(f"非零 {col} 数据:\n{non_zero_data}")
                
                df_q[new_col] = roll_sum(df_q[col])
                print(f"计算了 {new_col}")
                
                # 打印非零值和对应的日期
                non_zero_result = df_q[['m_anntime', new_col]].loc[df_q[new_col] != 0]
                print(f"非零 {new_col} 数据:\n{non_zero_result}")

        # Growth rates (YoY & QoQ) for Rev / NP
        for base_col, alias in [("rev_ttm", "rev"), ("np_ttm", "np")]:
            if base_col in df_q.columns:
                df_q[f"{alias}_yoy"] = roll_diff(df_q[base_col], lag=4)
                df_q[f"{alias}_qoq"] = roll_diff(df_q[base_col], lag=1)

        preprocessed[code] = df_q

    return preprocessed


def compute_classic_ratios(preprocessed: dict, facts: pd.DataFrame | None = None) -> pd.DataFrame:
    """Stage‑2: Generate ~35 single‑period fundamental ratios per stock.

    Parameters
    ----------
    preprocessed : dict[str, pd.DataFrame]
        Output from `preprocess_financial_data`.
    price_data : pd.DataFrame, optional
        Daily price table with columns ["stock_code", "trade_date", "close"]. If given,
        valuation ratios (PE/PB/PS/EV/EBITDA/FCF Yield) will be computed by matching the
        latest close price *before* the announcement date. If omitted these ratios are skipped.

    Returns
    -------
    pd.DataFrame
        Multi‑indexed DataFrame (stock_code, m_anntime) → ratio features.
    """
    feature_frames = []

    for code, df in preprocessed.items():
        if df.empty:
            continue
        feat = pd.DataFrame(index=df.index)
        feat["ts_code"] = code
        feat["m_anntime"] = df["m_anntime"]

        # ---------------- Valuation ----------------
        if {"capital_total_capital", "balance_tot_shrhldr_eqy_excl_min_int"}.issubset(df.columns):
            shares = df["capital_total_capital"]
            bv = df["balance_tot_shrhldr_eqy_excl_min_int"]
            if facts is not None:
                # Align prices: pick last close before announcement date
                px = facts.loc[facts["ts_code"] == code, ["trade_date", "close"]].copy()
                px.sort_values("trade_date", inplace=True)
                # forward‑fill so that each ann_time finds previous trading close
                px.set_index("trade_date", inplace=True)
                px = px.resample("D").ffill()
                feat["close"] = px.reindex(pd.to_datetime(df["m_anntime"])).values

                mkt_cap = feat["close"] * shares
                # Avoid division by zero for negative / zero profits
                if "np_ttm" in df.columns:
                    feat["pe_ttm"] = safe_div(mkt_cap, df["np_ttm"].replace({0: np.nan}))
                feat["pb"] = safe_div(mkt_cap, bv)
                if "rev_ttm" in df.columns:
                    feat["ps_ttm"] = safe_div(mkt_cap, df["rev_ttm"])  # 市销率

        # ---------------- Profitability ----------------
        if {"np_ttm", "balance_tot_shrhldr_eqy_excl_min_int"}.issubset(df.columns):
            feat["roe_ttm"] = safe_div(df["np_ttm"], df["balance_tot_shrhldr_eqy_excl_min_int"])
        if {"op_ttm", "rev_ttm"}.issubset(df.columns):
            feat["oper_margin_ttm"] = safe_div(df["op_ttm"], df["rev_ttm"])
        if {"rev_ttm", "balance_tot_assets"}.issubset(df.columns):
            feat["asset_turnover"] = safe_div(df["rev_ttm"], df["balance_tot_assets"])

        # ---------------- Growth ----------------
        if "rev_yoy" in df.columns:
            feat["rev_yoy"] = df["rev_yoy"]
        if "np_yoy" in df.columns:
            feat["np_yoy"] = df["np_yoy"]
        if "rev_qoq" in df.columns:
            feat["rev_qoq"] = df["rev_qoq"]
        if "np_qoq" in df.columns:
            feat["np_qoq"] = df["np_qoq"]

        # ---------------- Leverage / Solvency ----------------
        if {"balance_tot_liab", "balance_tot_assets"}.issubset(df.columns):
            feat["debt_assets"] = safe_div(df["balance_tot_liab"], df["balance_tot_assets"])
        if {"income_oper_profit", "income_less_int_exp"}.issubset(df.columns):
            feat["interest_coverage"] = safe_div(df["income_oper_profit"], df["income_less_int_exp"])
        if {"balance_total_current_assets", "balance_total_current_liability"}.issubset(df.columns):
            feat["current_ratio"] = safe_div(df["balance_total_current_assets"], df["balance_total_current_liability"])

        # ---------------- Cash Flow ----------------
        if {"ocf_ttm", "np_ttm"}.issubset(df.columns):
            feat["ocf_to_np"] = safe_div(df["ocf_ttm"], df["np_ttm"])
        if {"ocf_ttm", "rev_ttm"}.issubset(df.columns):
            feat["ocf_margin"] = safe_div(df["ocf_ttm"], df["rev_ttm"])

        feature_frames.append(feat)

    if not feature_frames:
        return pd.DataFrame()

    features = pd.concat(feature_frames, ignore_index=True)
    features.set_index(["stock_code", "m_anntime"], inplace=True)
    return features


def load_fact_table(stock_list: list[str]) -> pd.DataFrame:
    """
    从 parquet 文件中加载每个股票每天的收盘价数据。

    Parameters
    ----------
    stock_list : list[str]
        股票代码列表。

    Returns
    -------
    pd.DataFrame
        以 (stock_code, date) 为多级索引的 DataFrame，包含收盘价数据。
    """
    if not stock_list:
        return pd.DataFrame()
        
    dsf = ds.dataset(DATA_ROOT, format="parquet")
    
    # 使用 pc.is_in 进行批量查询，一次性获取所有股票数据
    t1 = time.time()
    
    # 创建过滤条件：ts_code 在 stock_list 中
    filter_expr = pc.is_in(ds.field("ts_code"), stock_list)
    
    # 一次性读取所有需要的股票数据
    features = dsf.to_table(
        filter=filter_expr,
        columns=["ts_code", "datetime", "close"]
    ).to_pandas()
    
    t2 = time.time()
    logger.info(f"批量加载 {len(stock_list)} 只股票数据耗时：{t2 - t1} 秒")
    
    if features.empty:
        return pd.DataFrame()
    
    # 确保日期列是日期类型
    if pd.api.types.is_datetime64_any_dtype(features["datetime"]):
        # 如果已经是日期类型，只保留日期部分（去掉时间）
        features["datetime"] = features["datetime"].dt.date
    else:
        # 如果不是日期类型，尝试转换
        features["datetime"] = pd.to_datetime(features["datetime"]).dt.date
    
    # 设置多级索引
    features.set_index(["ts_code", "datetime"], inplace=True)
    
    return features


###############################################################################
# Example usage (to be removed/commented out in production)
###############################################################################
if __name__ == "__main__":

        # 获取股票列表
    stock_list = ['000001.SZ', '600519.SH', '000651.SZ']
    
    # 获取财务数据
    print("正在获取财务数据...")
    financial_data = get_financial_data(stock_list)
    
    preproc = preprocess_financial_data(financial_data)
    facts = load_fact_table(stock_list)
    feat_df = compute_classic_ratios(preproc, facts)
    print(feat_df.head())
   
