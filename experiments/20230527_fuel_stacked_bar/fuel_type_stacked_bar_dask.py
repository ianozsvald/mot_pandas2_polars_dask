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
import dask
import pandas as pd
from distributed import Client, progress, wait

client = Client(n_workers=6)
client

parquet_path = "../../test_result.parquet/*"

# ### Unoptimised for slides
#
# 2023-12-01 - 42.34s

fuel_type_ddf = (
    dd.read_parquet(parquet_path, dtype_backend="pyarrow")
    .query('test_result == "P"')
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY",
                            "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .compute()
)

# ### Optimised for slides
#
# 2023-12-01 - 23s

fuel_type_ddf = (
    dd.read_parquet(parquet_path,
                    dtype_backend="pyarrow",
                    columns=["test_result", "test_date", "fuel_type"],
                    filters=[("test_result", "==", "P")],
    )
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY",
                            "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .compute()
)

# ### Variants with progress bars

# +
# Naive method ~3GB
# 2023-06
# 3.11.2 conda - 51s
# 2023-11
# 3.11.5 conda - 43.7s
# 3.11.6 - 44.1s
# 3.12.0 - 41s

fuel_type_ddf = (
    dd.read_parquet(
        "../../test_result.parquet",
        dtype_backend="pyarrow",
    )
    .query('test_result == "P"')
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY", "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .persist()
)

progress(fuel_type_ddf)

# +
# PyArrow part optimised method ~1.7GB
# 34s
# Less RAM, less time
# 24.6s

fuel_type_ddf = (
    dd.read_parquet(
        "../../test_result.parquet",
        dtype_backend="pyarrow",
        columns=["test_result", "test_date", "fuel_type"],
        # filters=[("test_result", "==", "P")],
    )
    .query('test_result == "P"')
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY", "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
    .persist()
)

progress(fuel_type_ddf)
# -

# ### Shuffle method performance testing

# No real difference between these
dask.config.set({"dataframe.shuffle.method": "p2p"})
#dask.config.set({"dataframe.shuffle.method": "tasks"})

del fuel_type_ddf

# +
# PyArrow optimised method ~1.7GB
# 29-32s
# Less RAM, less time
# 22.2s

fuel_type_ddf = (
    dd.read_parquet(
        "../../test_result.parquet",
        dtype_backend="pyarrow",
        columns=["test_result", "test_date", "fuel_type"],
        filters=[("test_result", "==", "P")],
    )
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


