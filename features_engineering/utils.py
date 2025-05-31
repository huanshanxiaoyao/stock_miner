import pyarrow.parquet as pq
from datetime import date
import pandas as pd
from pathlib import Path  # Add this import

def get_all_stock_codes():
    """
    获取所有股票代码，包括上市和退市时间
    返回：包含所有股票信息的DataFrame，包括ts_code, symbol, name, list_date, delist_date等字段
    """

    # 获取最新的快照目录
    # 注意：如果需要指定特定日期的快照，可以修改此处的逻辑
    snap = date.today().strftime("%Y%m%d")
    base_dir = Path(__file__).parent  # 修改为当前目录
    parquet_path = base_dir / "universe/snapshot_date={}/security_master.parquet".format(snap)
    
    # 检查文件是否存在
    if not parquet_path.exists():
        # 尝试查找其他可能的快照目录
        universe_dir = base_dir / "universe"
        if universe_dir.exists():
            snapshot_dirs = list(universe_dir.glob("snapshot_date=*"))
            if snapshot_dirs:
                # 使用最新的快照
                latest_snapshot = sorted(snapshot_dirs)[-1]
                parquet_path = latest_snapshot / "security_master.parquet"
    
    # 如果找到了parquet文件，读取它
    if parquet_path.exists():
        try:
            table = pq.read_table(parquet_path)
            df = table.to_pandas()
            
            # 处理delist_date列，将'99991231'转换为None表示未退市
            df['is_active'] = (df['delist_date'] == '99991231')
            
            return df
        except Exception as e:
            print(f"读取股票列表时出错: {e}")
            return pd.DataFrame()
    else:
        print(f"错误: 找不到股票列表文件，请先运行collect_data/get_all_codes.py生成数据")
        return pd.DataFrame()