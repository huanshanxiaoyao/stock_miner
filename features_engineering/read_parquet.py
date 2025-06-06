import pyarrow.dataset as ds, pandas as pd
dsf = ds.dataset("./fact_test_table", format="parquet")
sample = dsf.to_table(filter=ds.field("ts_code")=="001965.SZ").to_pandas()
print(sample)
