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

import dask.dataframe as dd
import pandas as pd
from distributed import Client, progress, wait

client = Client()
client

# +
# Naive method ~3GB

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
# PyArrow optimised method ~1.7GB

fuel_type_ddf = (
    dd.read_parquet(
        "../../test_result.parquet",
        columns=["test_result", "test_date", "fuel_type"],
        dtype_backend="pyarrow",
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
