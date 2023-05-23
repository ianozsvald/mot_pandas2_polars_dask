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
# df = pl.read_parquet('test_result.parquet/*.parquet')
# -

lf = pl.scan_parquet('test_result.parquet/*.parquet', n_rows=100)
lf.dtypes

lazyhead = lf.head(10)
lazyhead.show_graph()

lazyhead.collect()

lazylimit = lf.limit(10)
lazylimit.show_graph()

pl.scan_parquet('test_result.parquet/*.parquet').fetch(n_rows=int(10))

new_path = 'test_result.parquet/*.parquet'

f"{pl.scan_parquet(new_path).select(pl.count()).collect().item():,} rows"

pl.scan_parquet('test_result.parquet/*.parquet').limit(10).fetch(n_rows=10)

pl.scan_parquet('test_result.parquet/*.parquet', n_rows=10).limit(10).collect()

f = pl.scan_parquet('test_result.parquet/*.parquet', n_rows=100).limit(10)
f.show_graph()

pl.scan_parquet('test_result.parquet/*.parquet', n_rows=1000000000).limit(10).collect(streaming=True)
#pl.scan_parquet('test_result.parquet/*.parquet', n_rows=100).limit(10).collect(streaming=True)

head_df

pl.read_parquet('test_result.parquet', use_pyarrow=True).limit(10)

# +
# %%time

counts_df = (
    pl
    .scan_parquet('test_result.parquet/*.parquet')
    .select('test_date', 'test_result')
    .filter(pl.col('test_date') > pd.Timestamp('2021-01-01'))
    .filter(pl.col('test_result') == 'P')
    .groupby(pl.col('test_date').dt.weekday()).count()
    .collect()
)
# -

counts_df

filters=[('test_date', '>=', pd.Timestamp('2021-01-01')),
         ('test_result', "==", "P"),
        ],

pl.read_parquet('test_result.parquet/*.parquet',
                #use_pyarrow=True,
                pyarrow_options = {'filters':filters},
                #n_rows=10,
                columns=['test_date', 'test_result'],
               )\
.groupby(pl.col('test_date').dt.weekday()).count()

# +
# %%time

counts_df = (
    pl.read_parquet('test_result.parquet',
                    use_pyarrow=True,
                    pyarrow_options = {'filters':[('test_date', '>=', pd.Timestamp('2021-01-01')),
                                                  ('test_result', "==", "P"),
                                                 ],},
                    columns=['test_date', 'test_result'],
                   )
    .groupby(pl.col('test_date').dt.weekday()).count()
)
# -

counts_df

# +
# %%time

counts_df = (
    pl
    .read_parquet('test_result.parquet/*.parquet')
    .select('test_date', 'test_result')
    .filter(pl.col('test_date') > pd.Timestamp('2021-01-01'))
    .filter(pl.col('test_result') == 'P')
    .groupby(pl.col('test_date').dt.weekday()).count()
    .collect()
)
# -


