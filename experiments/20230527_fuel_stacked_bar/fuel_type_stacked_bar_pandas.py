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

# # Failed experiment
# (OOM - kernel dies unless filtering by date)

import pandas as pd

# +
# %%time

fuel_type_df = (
    pd.read_parquet(
        "../../test_result.parquet",
        columns=["test_result", "test_date", "fuel_type"],
        dtype_backend="pyarrow",
        filters=[
            ("test_result", "==", "P"),
            ("test_date", ">=", pd.Timestamp("2018-01-01")),
        ],
    )
    .replace({"fuel_type": {"Hybrid Electric (Clean)": "HY", "Electric": "EL"}})
    .assign(Year=lambda x: x.test_date.dt.year)
    .groupby(["Year", "fuel_type"])
    .agg({"test_result": "count"})
    .rename(columns={"test_result": "vehicle_count"})
)

# +
fuel_type_df = fuel_type_df.pivot_table(
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
