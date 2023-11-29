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

import polars as pl
import pandas as pd
import os
os.chdir("../..")

# %%time
edf = (
    pl.scan_parquet("test_result.parquet/*")
    .filter(pl.col("make").is_in(["VOLVO", "VOLKSWAGEN"]))
    .filter(pl.col("model").is_in(["V50", "PASSAT"]))
    .group_by("vehicle_id")
    .agg(
        pl.col(
            "make", "model", "fuel_type", "cylinder_capacity", "first_use_date"
        ).last(),
        pl.col("test_date").max().alias("last_test_date"),
        pl.col("test_mileage").max().alias("last_known_mileage"),
    )
    .collect()
)

edf.head()

len(edf)

edf.schema

ldf = (
    pl.scan_parquet("test_result.parquet/*")
    .filter(pl.col("make").is_in(["VOLVO", "VOLKSWAGEN"]))
    .filter(pl.col("model").is_in(["V50", "PASSAT"]))
    .group_by("vehicle_id")
    .agg(
        pl.col(
            "make", "model", "fuel_type", "cylinder_capacity", "first_use_date"
        ).last(),
        pl.col("test_date").max().alias("last_test_date"),
        pl.col("test_mileage").max().alias("last_known_mileage"),
    )
)

# %%time
# 5m22s
# 4m44s
edf1 = ldf.collect(streaming=True)

# %%time
# xmxxs
edf2 = ldf.collect()

edf = edf2

edf.head()

ldf.schema

#ldf.with_columns(('lifetime', pl.col('last_test_date') - pl.col('first_use_date')))
ldf.with_columns((pl.col('last_test_date') - pl.col('first_use_date')).alias('lifetime'))

# %%time
edf = ldf.collect()

edf.schema

edf.with_columns((pl.col('last_test_date') - pl.col('first_use_date')).alias('lifetime'))

# +
vehicle_summary_pdf = (
    edf
    .with_columns((pl.col('last_test_date') - pl.col('first_use_date')).alias('lifetime'))
    .to_pandas()
)

vehicle_summary_pdf['lifetime'] = vehicle_summary_pdf.lifetime.dt.days / 365
# -

#vehicle_summary_pdf["test_date"] = pd.to_datetime(vehicle_summary_pdf["test_date"])
#vehicle_summary_pdf["first_use_date"] = pd.to_datetime(
#    vehicle_summary_pdf["first_use_date"]
#)
vehicle_summary_pdf["surviving"] = vehicle_summary_pdf["last_test_date"] >= pd.Timestamp(
    "2022-01-01"
)
vehicle_summary_pdf["surviving_colour"] = vehicle_summary_pdf.surviving.map(
    {True: "blue", False: "red"}
)
vehicle_summary_pdf["fuel_colour"] = vehicle_summary_pdf.fuel_type.map(
    {"DI": "black", "PE": "blue", "HY": "green"}
)
vehicle_summary_pdf

# +
passats_df = (
    vehicle_summary_pdf.assign(year=vehicle_summary_pdf.first_use_date.dt.year)
    .query('model == "PASSAT" & fuel_type == "PE" & cylinder_capacity == 1798')
    .query("1997 <= year <= 2001")
)

passats_df

# +
from simpler_mpl import set_common_mpl_styles, set_commas
import matplotlib.pyplot as plt

fig, ax = plt.subplots(constrained_layout=True)

passats_df.dropna().plot.scatter(
    figsize=(8, 4),
    x="lifetime",
    y="last_known_mileage",
    marker=".",
    alpha=0.5,
    # s=0.5,
    c="surviving_colour",
    ax=ax
)
#ax.set_xlim(5, 26)
ax.set_ylim(0, 400000)
#ax.plot(result["test_date"], result["passed"])  # , marker='o')
ax.yaxis.set_major_formatter(lambda x, pos: f"{int(x/1000):,}k")
plt.xticks(rotation=-30)
set_common_mpl_styles(
    ax, title="Passat survival since registration", ymin=0, xlabel="Lifetime (years)" #ylabel="Passing Tests per Week"
)

