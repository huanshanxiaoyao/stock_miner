import tushare as ts
import pandas as pd
from datetime import date
from pathlib import Path  # 添加 Path 导入
import pyarrow as pa      # 添加 pyarrow 导入
import pyarrow.parquet as pq  # 添加 pyarrow.parquet 导入

# 使用你已有的token
pro = ts.pro_api(token="34e154b48c608f6dd444cb1749b69828a4eec91c1065da4ff5fca6d7")

def get_stock_lists():
    # 添加错误处理和重试机制
    try:
        all_stocks = pro.stock_basic(exchange='', list_status='', fields=
                'ts_code,symbol,name,list_date,delist_date,list_status')
        print(f"所有股票数量: {len(all_stocks)}")

        # 方法2：分别获取各状态股票并合并
        listed_stocks = pro.stock_basic(exchange='', list_status='L', fields=
                'ts_code,symbol,name,list_date,delist_date,list_status')  # 上市中
        suspended_stocks = pro.stock_basic(exchange='', list_status='P', fields=
                'ts_code,symbol,name,list_date,delist_date,list_status')  # 暂停上市
        
        # 这里添加重试机制，因为获取已退市股票时出现了错误
        max_retries = 3
        for attempt in range(max_retries):
            try:
                delisted_stocks = pro.stock_basic(exchange='', list_status='D', fields=
                        'ts_code,symbol,name,list_date,delist_date,list_status')  # 已退市
                break
            except requests.exceptions.ChunkedEncodingError:
                if attempt < max_retries - 1:
                    print(f"获取已退市股票失败，正在重试 {attempt+1}/{max_retries}...")
                    time.sleep(2)  # 等待2秒后重试
                else:
                    print("获取已退市股票失败，使用空DataFrame代替")
                    delisted_stocks = pd.DataFrame(columns=['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'list_status'])

        # 合并所有状态的股票
        all_stocks_combined = pd.concat([listed_stocks, suspended_stocks, delisted_stocks])
        print(f"合并后所有股票数量: {len(all_stocks_combined)}")

        # 查看各状态股票数量
        print(f"上市中股票数量: {len(listed_stocks)}")
        print(f"暂停上市股票数量: {len(suspended_stocks)}")
        print(f"已退市股票数量: {len(delisted_stocks)}")
        return all_stocks_combined, listed_stocks, suspended_stocks, delisted_stocks
    except Exception as e:
        print(f"获取股票列表时发生错误: {e}")
        # 返回空DataFrame
        empty_df = pd.DataFrame(columns=['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'list_status'])
        return empty_df, empty_df, empty_df, empty_df

def print_stocks(stocks):
    for index, row in stocks.iterrows():
        print(f"{row['ts_code']}, {row['symbol']}, {row['name']}, {row['list_date']}, {row['delist_date']}, {row['list_status']}")
    return True

def write_stocks_to_parquet(stocks_df):
    # 规范列
    df = (stocks_df.rename(columns={'ts_code':'ts_code','list_date':'list_date',
                            'delist_date':'delist_date'})
            .assign(
                delist_date=lambda d: d.delist_date.fillna('99991231'),
                update_ts=pd.Timestamp.now(),
                exchange=lambda d: d.ts_code.str[-2:],
            )[[
                'ts_code','symbol','name','list_date','delist_date',
                'exchange','update_ts'
            ]])

    # 写 parquet
    snap = date.today().strftime("%Y%m%d")
    out_dir = f"universe/snapshot_date={snap}"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), f"{out_dir}/security_master.parquet",
                compression="zstd")
    return True


def test_read_parquet():
    # 获取最新的快照目录
    snap = date.today().strftime("%Y%m%d")
    parquet_path = f"universe/snapshot_date={snap}/security_master.parquet"
    
    # 检查文件是否存在
    if not Path(parquet_path).exists():
        print(f"错误: 文件 {parquet_path} 不存在")
        return False
    
    try:
        # 读取 parquet 文件
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        # 打印基本信息
        print(f"成功读取 parquet 文件: {parquet_path}")
        print(f"数据形状: {df.shape}")
        print(f"列名: {df.columns.tolist()}")
        
        # 检查必要的列是否存在
        required_columns = ['ts_code', 'symbol', 'name', 'list_date', 'delist_date', 'exchange', 'update_ts']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"警告: 缺少以下列: {missing_columns}")
        
        # 显示前5行数据
        print("\n数据预览 (前5行):")
        print(df.head())
        
        # 检查数据类型
        print("\n数据类型:")
        print(df.dtypes)
        
        # 检查是否有空值
        null_counts = df.isnull().sum()
        print("\n空值统计:")
        print(null_counts)
        
        return True
    
    except Exception as e:
        print(f"读取 parquet 文件时出错: {e}")
        return False

# 扩展功能：按条件查询数据
def query_stocks(condition=None):
    # 获取最新的快照目录
    snap = date.today().strftime("%Y%m%d")
    parquet_path = f"universe/snapshot_date={snap}/security_master.parquet"
    
    if not Path(parquet_path).exists():
        print(f"错误: 文件 {parquet_path} 不存在")
        return None
    
    try:
        # 读取 parquet 文件
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        # 如果有查询条件，应用条件
        if condition:
            # 例如: condition = "exchange == 'SZ' and delist_date == '99991231'"
            result = df.query(condition)
            print(f"查询条件: {condition}")
            print(f"查询结果数量: {len(result)}")
            return result
        
        return df
    
    except Exception as e:
        print(f"查询数据时出错: {e}")
        return None

if __name__ == "__main__":
    #all_stocks_combined, listed_stocks, suspended_stocks, delisted_stocks = get_stock_lists()
    #print_stocks(all_stocks_combined)
    #write_stocks_to_parquet(all_stocks_combined)
    
    # 添加测试读取功能
    print("\n测试读取parquet文件:")
    test_read_parquet()
    
    # 示例：查询上海交易所的未退市股票
    # print("\n查询上海交易所未退市股票:")
    # active_sh_stocks = query_stocks("exchange == 'SH' and delist_date == '99991231'")
    # if active_sh_stocks is not None and not active_sh_stocks.empty:
    #     print(f"上海交易所未退市股票数量: {len(active_sh_stocks)}")

