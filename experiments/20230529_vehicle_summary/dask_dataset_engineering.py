# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
import dask.dataframe as dd
import pandas as pd

import dask
# IAN has set this temporary folder, I keep running out of space...
#dask.config.set(temporary_directory='/media/ian/data/mot_pandas2_polars_dask/data')
dask.config.set(temporary_directory='.')

from distributed import Client, progress, wait

import dask
import os
# -

client = Client()
client

# +
f = (
    dd
    .read_parquet('../../test_result.parquet',
                  dtype_backend='pyarrow',
                  pyarrow_options = {'filters':[('test_class_id', '==', 4)]},
                 )
    #.partitions[:3]
    .set_index('vehicle_id', shuffle='p2p')
    .to_parquet('../../test_result_sorted.parquet', overwrite=True, compute=False)
    .persist()
)

progress(f)

# + active=""
# 1/0 # IAN is unsure whether we need this subsequent cell

# + active=""
# f = (
#     dd
#     .read_parquet('../../test_result.parquet',
#                   dtype_backend='pyarrow',
#                   pyarrow_options = {'filters':[('test_class_id', '==', 4)]},
#                  )
#     #.partitions[:3]
#     .dropna(subset='first_use_date')
#     .astype({'first_use_date':'string[pyarrow]'})
#     .set_index('first_use_date', shuffle='tasks')
#     .map_partitions(lambda x: x.sort_values(by=['vehicle_id', 'test_date']))
#     .to_parquet('../../test_result_by_first_use_date.parquet', overwrite=True, compute=False)
#     .persist()
# )
#
# progress(f)
