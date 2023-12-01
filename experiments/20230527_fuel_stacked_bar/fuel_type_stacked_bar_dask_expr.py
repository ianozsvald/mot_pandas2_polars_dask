# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

import dask.dataframe as dd
import dask_expr as dx
import pandas as pd
from distributed import Client, progress, wait

client = Client(n_workers=6)
client

parquet_path = "../../test_result.parquet/*"

# ### For slide deck
#
# 2023-12-01 -- 26.5s

ddf = dx.read_parquet(parquet_path, dtype_backend="pyarrow",)
fuel_type_ddf = (
    ddf[(ddf['test_result'] == "P")]
    #.query('test_result == "P"') # not in dask-expr 0.2.4
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY",
                            "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .compute()
)

# ### Testing

# +
# 2023-11
# 3.11.5 wih dask-expr 26.7s

ddf = dx.read_parquet(parquet_path, dtype_backend="pyarrow",)

# Workaround for lack of .query support in dask-expr
# This partially defeats the purpose of testing dask-expr
# filters=[("test_result", "==", "P")],

fuel_type_ddf = (
    ddf[(ddf['test_result'] == "P")]
    #.query('test_result == "P"') # not supported in dask-expr 0.2.4
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY", "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .persist()
)

progress(fuel_type_ddf)

# +
fuel_type_df = fuel_type_ddf.compute().pivot_table(
    values="vehicle_count",
    index="Year",
    columns="fuel_type",
    aggfunc="sum",
    fill_value=0,
)

fuel_type_df.head()
# -

ax = (
    fuel_type_df.loc[2006:, ["PE", "DI", "HY", "EL"]]
    .div(1000000)
    .reset_index()
    .plot.bar(figsize=(12, 6), x="Year", stacked=True, title="Car Counts by Fuel Type")
)
ax.set_ylabel("Count (million)")


