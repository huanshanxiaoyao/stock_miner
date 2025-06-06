import pandas as pd
import numpy as np
from functools import reduce
from xtquant import xtdata

def get_financial_data(stock_codes):
    """
    获取股票的财务数据并返回结构化的结果
    
    Args:
        stock_codes: 股票代码列表，如 ['000001.SZ', '600519.SH']
        
    Returns:
        dict: 包含所有股票财务数据的字典，格式为 {股票代码: {数据类型: DataFrame}}
    """
    return xtdata.get_financial_data(stock_codes)

###############################################################################
# Utility helpers
###############################################################################

def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    """Element‑wise division that avoids ZeroDivisionError and preserves NaNs."""
    return a.divide(b.replace({0: np.nan}))


def _add_prefix(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Add prefix to all columns except the timestamp key columns."""
    key_cols = {"m_anntime", "m_timetag", "declareDate", "endDate"}
    new_cols = {c: f"{prefix}_{c}" for c in df.columns if c not in key_cols}
    return df.rename(columns=new_cols)


def _roll_sum(series: pd.Series, window: int = 4) -> pd.Series:
    """Quarterly rolling window sum → TTM."""
    return series.rolling(window=window, min_periods=window).sum()


def _roll_diff(series: pd.Series, lag: int = 4) -> pd.Series:
    """Lagged difference divided by abs(lagged value (YoY / QoQ))."""
    return (series - series.shift(lag)).divide(series.shift(lag).abs())

###############################################################################
# Core pipeline
###############################################################################

def merge_financial_tables(tables: dict) -> pd.DataFrame:
    """Merge multiple raw financial DataFrames (quarter frequency) on `m_anntime`.

    Each DataFrame is expected to contain an announcement‑date column `m_anntime`.
    Columns are prefixed by their table name (lower‑case) to avoid collision.

    Parameters
    ----------
    tables : dict[str, pd.DataFrame]
        Mapping of table name → DataFrame as returned by XtData.

    Returns
    -------
    pd.DataFrame
        Wide quarterly DataFrame indexed by announcement date.
    """
    dfs = []
    for tbl_name, df in tables.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        if "m_anntime" not in df.columns:
            # 兼容 HolderNum / Top10holder 等使用其他时间列的情况，跳过本步骤
            continue
        df = df.sort_values("m_anntime").reset_index(drop=True)
        dfs.append(_add_prefix(df, tbl_name.lower()))
    if not dfs:
        return pd.DataFrame()
    merged = reduce(lambda left, right: pd.merge(left, right, on="m_anntime", how="outer"), dfs)
    merged.sort_values("m_anntime", inplace=True)
    merged.reset_index(drop=True, inplace=True)
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
        # CashFlow表
        "cashflow_net_cash_flows_oper_act": "ocf_ttm",
    }

    for code, tables in financial_data.items():
        df_q = merge_financial_tables(tables)
        if df_q.empty:
            continue

        # Calculate TTM for key items
        for col, new_col in KEY_ITEMS.items():
            if col in df_q.columns:
                df_q[new_col] = _roll_sum(df_q[col])

        # Growth rates (YoY & QoQ) for Rev / NP
        for base_col, alias in [("rev_ttm", "rev"), ("np_ttm", "np")]:
            if base_col in df_q.columns:
                df_q[f"{alias}_yoy"] = _roll_diff(df_q[base_col], lag=4)
                df_q[f"{alias}_qoq"] = _roll_diff(df_q[base_col], lag=1)

        preprocessed[code] = df_q

    return preprocessed


def compute_classic_ratios(preprocessed: dict, price_data: pd.DataFrame | None = None) -> pd.DataFrame:
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
        feat["stock_code"] = code
        feat["m_anntime"] = df["m_anntime"]

        # ---------------- Valuation ----------------
        if {"capital_total_capital", "balance_tot_shrhldr_eqy_excl_min_int"}.issubset(df.columns):
            shares = df["capital_total_capital"]
            bv = df["balance_tot_shrhldr_eqy_excl_min_int"]
            if price_data is not None:
                # Align prices: pick last close before announcement date
                px = price_data.loc[price_data["stock_code"] == code, ["trade_date", "close"]].copy()
                px.sort_values("trade_date", inplace=True)
                # forward‑fill so that each ann_time finds previous trading close
                px.set_index("trade_date", inplace=True)
                px = px.resample("D").ffill()
                feat["close"] = px.reindex(pd.to_datetime(df["m_anntime"])).values

                mkt_cap = feat["close"] * shares
                # Avoid division by zero for negative / zero profits
                if "np_ttm" in df.columns:
                    feat["pe_ttm"] = _safe_div(mkt_cap, df["np_ttm"].replace({0: np.nan}))
                feat["pb"] = _safe_div(mkt_cap, bv)
                if "rev_ttm" in df.columns:
                    feat["ps_ttm"] = _safe_div(mkt_cap, df["rev_ttm"])  # 市销率

        # ---------------- Profitability ----------------
        if {"np_ttm", "balance_tot_shrhldr_eqy_excl_min_int"}.issubset(df.columns):
            feat["roe_ttm"] = _safe_div(df["np_ttm"], df["balance_tot_shrhldr_eqy_excl_min_int"])
        if {"op_ttm", "rev_ttm"}.issubset(df.columns):
            feat["oper_margin_ttm"] = _safe_div(df["op_ttm"], df["rev_ttm"])
        if {"rev_ttm", "balance_tot_assets"}.issubset(df.columns):
            feat["asset_turnover"] = _safe_div(df["rev_ttm"], df["balance_tot_assets"])

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
            feat["debt_assets"] = _safe_div(df["balance_tot_liab"], df["balance_tot_assets"])
        if {"income_oper_profit", "income_less_int_exp"}.issubset(df.columns):
            feat["interest_coverage"] = _safe_div(df["income_oper_profit"], df["income_less_int_exp"])
        if {"balance_total_current_assets", "balance_total_current_liability"}.issubset(df.columns):
            feat["current_ratio"] = _safe_div(df["balance_total_current_assets"], df["balance_total_current_liability"])

        # ---------------- Cash Flow ----------------
        if {"ocf_ttm", "np_ttm"}.issubset(df.columns):
            feat["ocf_to_np"] = _safe_div(df["ocf_ttm"], df["np_ttm"])
        if {"ocf_ttm", "rev_ttm"}.issubset(df.columns):
            feat["ocf_margin"] = _safe_div(df["ocf_ttm"], df["rev_ttm"])

        feature_frames.append(feat)

    if not feature_frames:
        return pd.DataFrame()

    features = pd.concat(feature_frames, ignore_index=True)
    features.set_index(["stock_code", "m_anntime"], inplace=True)
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
    feat_df = compute_classic_ratios(preproc, price_df)
    print(feat_df.head())
   
