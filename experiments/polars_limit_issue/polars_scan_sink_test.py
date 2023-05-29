# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import polars as pl
import pyarrow.dataset as ds

# + active=""
# # Works
# df = pl.scan_csv('data/test_result_201*.txt', n_rows=1000, separator='|', cache=False, try_parse_dates=True)
# df.schema

# + active=""
# # Results in OOM with subsequent limit cell
# df = pl.scan_parquet('../../test_result.parquet/*')
# df.schema
# -

# Doesn't work with sink_parquet
# PanicException: sink_parquet not yet supported in standard engine. Use 'collect().write_parquet()'
df = pl.scan_pyarrow_dataset(ds.dataset('../../test_result.parquet', format="parquet"))
df.schema

df.limit(10).collect(streaming=True)

df.sink_parquet('polars_test.parquet')


