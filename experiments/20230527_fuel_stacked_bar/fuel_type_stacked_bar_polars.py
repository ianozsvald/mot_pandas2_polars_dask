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

import polars as pl

# %%time
fuel_type_edf = (
    pl.scan_parquet("../../test_result.parquet/*")
    .select(["test_result", "test_date", "fuel_type"])
    .filter(pl.col("test_result") == "P")
    .with_columns(
        pl.col("fuel_type")
        .replace({"Hybrid Electric (Clean)": "HY", "Electric": "EL"}, default=pl.first())
        .cast(str),
        pl.col("test_date").dt.year().alias("Year"),
    )
    .group_by(["Year", "fuel_type"])
    .agg(pl.col("test_result").count().alias("vehicle_count"))
    .collect(streaming=True) # streaming required to prevent OOM
)

# +
fuel_type_df = (
    fuel_type_edf.pivot(
        index="Year",
        columns="fuel_type",
        values="vehicle_count",
        aggregate_function="sum",
        sort_columns=True,
    )
    .fill_null(0)
    .sort(by="Year")
    .to_pandas()
    .set_index("Year")
)

fuel_type_df.head()

# +
import matplotlib.pyplot as plt
from simpler_mpl import set_commas, set_common_mpl_styles

fig, ax = plt.subplots(constrained_layout=True, figsize=(8, 6))

fuel_type_df.loc[2006:, ["PE", "DI", "HY", "EL"]].div(1000000).reset_index().plot.bar(
    x="Year", stacked=True, title="Car Counts by Fuel Type", ax=ax
)

ax.set_ylabel("Count (million)")
set_common_mpl_styles(ax=ax)
plt.xticks(rotation=-30);
# -


