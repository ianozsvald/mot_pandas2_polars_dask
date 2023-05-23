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
import pandas as pd

# +
# %%time

counts_df = (
    pl
    .scan_parquet('../../test_result.parquet/*.parquet')
    .select('test_date', 'test_result')
    .filter(pl.col('test_date') > pd.Timestamp('2021-01-01'))
    .filter(pl.col('test_result') == 'P')
    .groupby(pl.col('test_date').dt.weekday())
    .count()
    .collect()
)

counts_df

# +
# %%time

counts_df = (
    pl.read_parquet('../../test_result.parquet',
                    use_pyarrow=True,
                    pyarrow_options = {'filters':[('test_date', '>=', pd.Timestamp('2021-01-01')),
                                                  ('test_result', "==", "P"),
                                                 ],},
                    columns=['test_date', 'test_result'],
                   )
    .groupby(pl.col('test_date').dt.weekday())
    .count()
)

counts_df

# +
# %%time
# No results because reading from partitions with older data

counts_df = (
    pl.read_parquet('../../test_result.parquet/*.parquet',
                    n_rows=1000000,
                    columns=['test_date', 'test_result'],
                   )
    .filter(pl.col('test_date') > pd.Timestamp('2021-01-01'))
    .filter(pl.col('test_result') == 'P')
    .groupby(pl.col('test_date').dt.weekday())
    .count()
)

counts_df

# + active=""
# %%time
#
# OOM
#
# counts_df = (
#     pl
#     .read_parquet('test_result.parquet/*.parquet')
#     .select('test_date', 'test_result')
#     .filter(pl.col('test_date') > pd.Timestamp('2021-01-01'))
#     .filter(pl.col('test_result') == 'P')
#     .groupby(pl.col('test_date').dt.weekday()).count()
#     .collect()
# )
# -


