# config.py
STOCK_LIST  = ["000001.SZ", "600519.SH"]        # 或动态读取交易所股票列表
START_DAY   = "20200101"
DATA_ROOT   = "./fact_test_table"                   # Parquet 目标目录
PERIODS     = ["1d","1m"]                      # 需要日线、1 分钟线
