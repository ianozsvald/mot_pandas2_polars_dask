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

# +
# df = pl.read_parquet('test_result.parquet/*.parquet')
# -

lf = pl.scan_parquet('test_result_pyspark.parquet/*.parquet')
lf.dtypes

lf = pl.scan_parquet('test_result.parquet/*.parquet')
lf.dtypes

lf.limit(10)

df = lf.limit(10).collect()
df


