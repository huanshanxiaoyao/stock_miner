import pandas as pd

    
def calculate_features(df):
    """计算所有特征的主函数，对应原 add_features 的主要逻辑"""
    # 计算交易特征
    df = _calculate_trading_features(df)
    # 计算价格限制特征
    df = _calculate_price_limit_features(df)
    # 计算收益率特征
    df = _calculate_return_features(df)
    
    return df

def _calculate_trading_features(df):
    """计算交易相关特征，如成交量加权平均价格等"""
    df['vwap'] = (df['amount'] / 100) / df['volume'].replace(0, pd.NA)
    return df

def _calculate_return_features(df):
    """计算收益率相关特征"""
    df['ret_1p'] = df.close.pct_change().shift(-1)  # 下一周期收益 label
    return df

def _calculate_price_limit_features(df):
    """计算价格限制相关特征，如涨跌停价"""
    # 简易涨跌停价（无聚宽时用规则法，后续可替换）
    pct = 0.1
    st_mask = df['ts_code'].str.contains('ST')
    pct_series = pd.Series(pct, index=df.index)
    pct_series.loc[st_mask] = 0.05
    df['high_limit'] = df['close'].shift(1) * (1+pct_series)
    df['low_limit'] = df['close'].shift(1) * (1-pct_series)
    return df