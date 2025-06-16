import os
import sys
import pandas as pd

# 添加上级目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simple_log import init_logging
logger = init_logging('financial_features_pipeline')

def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    删除全为0或空的列
    
    Args:
        df: 输入的DataFrame
        
    Returns:
        DataFrame: 处理后的DataFrame，已删除全为0或空的列
    """
    if df.empty:
        return df
        
    # 检查每一列是否全为0或空
    cols_to_drop = []
    for col in df.columns:
        # 跳过时间列
        if col in ['m_anntime', 'declareDate', 'endDate']:
            continue
            
        # 检查是否全为0
        is_all_zero = (df[col] == 0).all()
        
        # 检查是否全为空
        is_all_na = df[col].isna().all()
        
        if is_all_zero or is_all_na:
            cols_to_drop.append(col)
    
    # 记录被删除的列
    if cols_to_drop:
        logger.info(f"删除全为0或空的列: {cols_to_drop}")
        
    # 删除列并返回
    return df.drop(columns=cols_to_drop, errors='ignore')


def process_top10_holders(df: pd.DataFrame, holder_type: str) -> pd.DataFrame:
    """
    处理Top10FlowHolder和Top10Holder数据，将同一日期的多条记录合并为一条
    
    Args:
        df: Top10FlowHolder或Top10Holder的DataFrame
        holder_type: 持有者类型，用于日志记录
        
    Returns:
        DataFrame: 处理后的DataFrame，同一日期的多条记录已合并为一条
    """
    if df.empty:
        return df
        
    # 确保有必要的列
    if 'declareDate' not in df.columns:
        logger.warning(f"{holder_type} 缺少 declareDate 列，无法处理")
        return df
        
    # 按declareDate分组
    grouped = df.groupby('declareDate')
    result_rows = []
    
    for date, group in grouped:
        # 按rank排序
        if 'rank' in group.columns:
            group = group.sort_values('rank')
            
        # 创建新行，保留共同列
        new_row = {'declareDate': date}
        
        # 如果有endDate列，保留第一个值
        if 'endDate' in group.columns:
            new_row['endDate'] = group['endDate'].iloc[0]
            
        # 处理每个持有者的数据，添加后缀
        for i, (_, row) in enumerate(group.iterrows()):
            for col in row.index:
                # 跳过已处理的列
                if col in ['declareDate', 'endDate', 'rank']:
                    continue
                    
                # 添加后缀
                new_col = f"{col}_{i}"
                new_row[new_col] = row[col]
                
        result_rows.append(new_row)
    
    # 创建新的DataFrame
    result_df = pd.DataFrame(result_rows)
    logger.info(f"处理 {holder_type} 数据: 从 {len(df)} 行合并为 {len(result_df)} 行")
    
    return result_df

def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    """Element‑wise division that avoids ZeroDivisionError and preserves NaNs."""
    return a.divide(b.replace({0: np.nan}))


def add_prefix(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Add prefix to all columns except the timestamp key column m_anntime."""
    # 只保留 m_anntime 列不添加前缀，其他列都添加前缀
    new_cols = {c: f"{prefix}_{c}" for c in df.columns if c != "m_anntime"}
    return df.rename(columns=new_cols)


def roll_sum(series: pd.Series, window: int = 4) -> pd.Series:
    """Quarterly rolling window sum → TTM."""
    return series.rolling(window=window, min_periods=window).sum()


def roll_diff(series: pd.Series, lag: int = 4) -> pd.Series:
    """Lagged difference divided by abs(lagged value (YoY / QoQ))."""
    return (series - series.shift(lag)).divide(series.shift(lag).abs())