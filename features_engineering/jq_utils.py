import jqdatasdk as jq

jq.auth("18701341286", "120502JoinQuant")

def get_trading_days(start_date, end_date):
    """获取实际的交易日期列表,日期格式2025-01-02"""
    if isinstance(start_date, str) and len(start_date) == 8:
        # 将"20200101"格式转换为"2020-01-01"格式
        start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    if isinstance(end_date, str) and len(end_date) == 8:
        # 将"20200101"格式转换为"2020-01-01"格式
        end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    trading_dates = jq.get_trade_days(start_date=start_date, end_date=end_date)
    return trading_dates