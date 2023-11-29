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

# Notes:
# * dask GroupBy tuning https://youtu.be/QY0zFsaO2j8

# +
import dask.dataframe as dd
import pandas as pd
import dask
import os

from distributed import Client, progress, wait

dask.config.set(temporary_directory=os.getcwd())
#dask.config.set(temporary_directory='/media/ian/data/mot_pandas2_polars_dask/data')

# +
os.chdir("../..")

client = Client(n_workers=16)
client
# -

# 2m15s Giles
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result.parquet",
        dtype_backend="pyarrow",
        columns=[
            "vehicle_id",
            "make",
            "model",
            "fuel_type",
            "cylinder_capacity",
            "first_use_date",
            "test_date",
            "test_mileage",
        ],
        filters=[
            ("make", "in", ["VOLVO", "VOLKSWAGEN"]),
            ("model", "in", ["V50", "PASSAT"]),
        ],
    )
    #.set_index('vehicle_id', npartitions=96, shuffle='tasks')
    #.repartition(npartitions=96)
    #.partitions[:3]
    .groupby("vehicle_id").agg(
        {
            "make": "last",
            "model": "last",
            "fuel_type": "last",
            "cylinder_capacity": "last",
            "first_use_date": "last",
            "test_date": "max",
            "test_mileage": "max",
        },
        shuffle='p2p',
        split_every=32,
        split_out=16,
    )
    .persist()
)
progress(vehicle_summary_ddf)


# +
def vehicle_grouper(df, groupby_sort=True, agg_shuffle=None):
    """Get summary data for each vehicle"""
    vehicle_df = df.groupby(
        "vehicle_id",
        sort=groupby_sort,  # True, False
    ).agg(
        {
            "make": "last",
            "model": "last",
            "fuel_type": "last",
            "cylinder_capacity": "last",
            "first_use_date": "last",
            "test_date": "max",
            "test_mileage": "max",
        },
        shuffle=agg_shuffle,  # None, 'disk', 'tasks', 'p2p'
    )
    return vehicle_df


def add_lifetime(df):
    """Get vehicle lifespan (vehicle may or may not still be in service)"""
    df["lifetime"] = (
        dd.to_timedelta((df["test_date"] - df["first_use_date"]))
        .astype('timedelta64[ns]')
        .dt.days / 365
    )
    return df


# -

# 30m16s Giles, 18min Ian
# 2023-11 3m9s
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result.parquet",
        dtype_backend='pyarrow',
        filters=[("make", "in", ["VOLVO", "VOLKSWAGEN"])],
        columns=['vehicle_id', 'make', 'model', 'fuel_type', 'cylinder_capacity', 'first_use_date', 'test_date', 'test_mileage'],
    )
    .set_index('vehicle_id')
    .query(
        'make in ["VOLVO", "VOLKSWAGEN"] & model in ["V50", "PASSAT"]'
    )
    .dropna()
    .pipe(vehicle_grouper)
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# 29m18s Giles, 34min to 854/854 aggregate-chunks and 120/123 agg-combines,then it seemlingly got stuck
# 2023-11 7m20s
vehicle_summary_ddf = (
    dd.read_parquet(path="test_result.parquet", dtype_backend="pyarrow")
    .query(
        'make in ["VOLVO", "VOLKSWAGEN", "ROVER"] & model in ["V50", "PASSAT", "200", "200 VI"]'
    )
    .dropna()
    .pipe(vehicle_grouper)
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# 6m55s Giles
# 2023-11 2m48s
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result_sorted.parquet",
        dtype_backend="pyarrow",
        index="vehicle_id",
        calculate_divisions=True,
    )
    .query(
        'make in ["VOLVO", "VOLKSWAGEN", "ROVER"] & model in ["V50", "PASSAT", "200", "200 VI"]'
    )
    .dropna()
    .pipe(vehicle_grouper, groupby_sort=True, agg_shuffle=None)
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# 11m02s Giles
# 2023-11 2m26s
vehicle_summary_ddf = (
    dd.read_parquet(path="test_result.parquet", dtype_backend="pyarrow")
    .query(
        'make in ["VOLVO", "VOLKSWAGEN", "ROVER"] & model in ["V50", "PASSAT", "200", "200 VI"]'
    )
    .dropna()
    .pipe(vehicle_grouper, agg_shuffle="p2p")
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# 2m19s Giles, Ian 3min
# 2023-11 50s
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result_sorted.parquet",
        dtype_backend="pyarrow",
        index="vehicle_id",
        calculate_divisions=True,
    )
    .query(
        'make in ["VOLVO", "VOLKSWAGEN", "ROVER"] & model in ["V50", "PASSAT", "200", "200 VI"]'
    )
    .dropna()
    .pipe(vehicle_grouper, groupby_sort=True, agg_shuffle="p2p")
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# 2m15s Giles
# 2023-11 42s
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result_sorted.parquet",
        dtype_backend="pyarrow",
        index="vehicle_id",
        calculate_divisions=True,
        columns=[
            "make",
            "model",
            "fuel_type",
            "cylinder_capacity",
            "first_use_date",
            "test_date",
            "test_mileage",
        ],
        filters=[
            ("make", "in", ["VOLVO", "VOLKSWAGEN", "ROVER"]),
            ("model", "in", ["V50", "PASSAT", "200", "200 VI"]),
        ],
    )
    .dropna()
    .pipe(vehicle_grouper, groupby_sort=False, agg_shuffle="p2p")
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

# Lifetime
vehicle_summary_ddf = (
    dd.read_parquet(
        path="test_result_sorted.parquet",
        dtype_backend="pyarrow",
        index="vehicle_id",
        calculate_divisions=True,
    )
    .query(
        'make in ["VOLVO", "VOLKSWAGEN", "ROVER"] & model in ["V50", "PASSAT", "200", "200 VI"]'
    )
    .dropna()
    .pipe(vehicle_grouper, groupby_sort=True, agg_shuffle="p2p")
    .pipe(add_lifetime)
    .persist()
)
progress(vehicle_summary_ddf)

vehicle_summary_ddf.head()

vehicle_summary_pdf = vehicle_summary_ddf.compute().sort_index()
vehicle_summary_pdf["test_date"] = pd.to_datetime(vehicle_summary_pdf["test_date"])
vehicle_summary_pdf["first_use_date"] = pd.to_datetime(
    vehicle_summary_pdf["first_use_date"]
)
vehicle_summary_pdf["surviving"] = vehicle_summary_pdf["test_date"] >= pd.Timestamp(
    "2022-01-01"
)
vehicle_summary_pdf["surviving_colour"] = vehicle_summary_pdf.surviving.map(
    {True: "blue", False: "red"}
)
vehicle_summary_pdf["fuel_colour"] = vehicle_summary_pdf.fuel_type.map(
    {"DI": "black", "PE": "blue", "HY": "green"}
)
vehicle_summary_pdf

vehicle_summary_pdf.groupby(["make", "surviving"])["model"].count()

vehicle_summary_pdf.model.value_counts()

vehicle_summary_pdf.fuel_type.value_counts()

vehicle_summary_pdf.query('model == "200" & test_mileage == 205404')

vehicle_summary_pdf.query('model == "V50" & test_mileage == 171443')

vehicle_summary_pdf.query('model == "PASSAT" & test_mileage == 142191')

# + active=""
# R487 JHJ 1796
# S61 UKM 1798
# CN05 HJC 1997
# -

vehicle_summary_pdf.query("vehicle_id in [1042513740, 1238787680, 1354367214]")

# +
v50s_df = (
    vehicle_summary_pdf.assign(year=vehicle_summary_pdf.first_use_date.dt.year).query(
        'model == "V50" & fuel_type == "DI" & cylinder_capacity == 1997'
    )
    # .query('1997 <= year <= 2001')
)

v50s_df.groupby("year").count()

# +
passats_df = (
    vehicle_summary_pdf.assign(year=vehicle_summary_pdf.first_use_date.dt.year)
    .query('model == "PASSAT" & fuel_type == "PE" & cylinder_capacity == 1798')
    .query("1997 <= year <= 2001")
)

passats_df  # .groupby('year').count()

# +
#rovers_df = vehicle_summary_pdf.query(
#    'model in ["200", "200 VI"] & cylinder_capacity == 1796'
#)#
#
#rovers_df

# +
#ax = rovers_df.dropna().plot.scatter(
#    figsize=(12, 6),
#    x="lifetime",
#    y="test_mileage",
#    marker=".",
#    alpha=0.5,
#    # s=0.5,
#    c="surviving_colour",
#)
#ax.set_xlim(5, 26)
#ax.set_ylim(0, 250000)

# +
from simpler_mpl import set_common_mpl_styles, set_commas
import matplotlib.pyplot as plt

fig, ax = plt.subplots(constrained_layout=True)

passats_df.dropna().plot.scatter(
    figsize=(8, 4),
    x="lifetime",
    y="test_mileage",
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

# -

ax = passats_df.dropna().plot.scatter(
    figsize=(12, 6),
    x="lifetime",
    y="test_mileage",
    marker=".",
    alpha=0.5,
    # s=0.5,
    c="surviving_colour",
)
ax.set_xlim(5, 26)
ax.set_ylim(0, 400000)

passats_df.info()

ax = (
    passats_df.resample(on="test_date", rule="1Y")
    .model.count()
    .loc[::-1]
    .cumsum()[::-1]
    .plot(marker=".")
)
ax.set_title("Passat Survival")

ax = v50s_df.dropna().plot.scatter(
    figsize=(12, 6),
    x="lifetime",
    y="test_mileage",
    marker=".",
    alpha=0.5,
    # s=0.5,
    c="surviving_colour",
)
ax.set_xlim(5, 20)
ax.set_ylim(0, 300000)

pd.concat([passats_df, v50s_df]).groupby(["make", "surviving"]).model.count()


