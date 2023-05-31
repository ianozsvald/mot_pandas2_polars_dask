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

# # Polars groupby on parquet data 2021+, eager vs lazy

# +
import pandas as pd
import polars as pl
from humanfriendly import format_size, format_number
from simpler_mpl import set_commas, set_common_mpl_styles
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats

# %load_ext autoreload
# %autoreload 2

display(f"Pandas {pd.__version__}, Polars {pl.__version__}")

from cell_profiler import cell_profiler as cp

# %start_cell_profiler


def show_rows_memory(df, deep=False):
    """
    Show rows and memory cost of a Pandas/Polars dataframe
    `deep=True` only has an impact on Pandas numpy-backed string columns, which otherwise are undercounted
    """
    num_bytes = 0
    df_type = "Unknown"
    try:
        num_bytes = df.estimated_size()  # try Polars
        df_type = "Polars"
    except AttributeError:
        pass
    try:
        num_bytes = df.memory_usage(deep=deep, index=False).sum()  # try Pandas
        df_type = "Pandas"
    except AttributeError:
        pass
    display(
        f"{df_type} df with {format_number(df.shape[0])} rows, {format_size(num_bytes)} bytes"
    )


# -

dfple = pl.read_parquet("../test_result_2021on.parquet")

# Notes from Ritchie via: https://pola-rs.github.io/polars-book/user-guide/lazy/query_plan/ via https://en.wikipedia.org/wiki/Relational_algebra
#
# ```
# Pi 2/14 means we only select 2/14 columns.
#
# Pi is relational algebra for projection (column selection)
#
# Sigma is relational algebra for selection (row filtering)
# ```

show_rows_memory(dfple)

dfple.lazy().groupby(by="make").agg("cylinder_capacity")

dfple.lazy().groupby(by="make").agg("cylinder_capacity").show_graph()

print(dfple.lazy().groupby(by="make").agg("cylinder_capacity").explain(optimized=False))

dfple.lazy().groupby(by="make").agg("cylinder_capacity").profile()

# %%time
result = (
    dfple.filter(pl.col("cylinder_capacity").is_not_null())
    .groupby(by="make")
    .agg(
        [
            pl.col("cylinder_capacity").median().alias("median"),
            pl.col("cylinder_capacity").count().alias("count"),
        ]
    )
    .filter(pl.col("count") > 10)
    .sort(by="median")
)
result[:3]

# %%time
# note collect(streaming=True) same result
result = (
    dfple.lazy()
    .filter(pl.col("cylinder_capacity").is_not_null())
    .groupby(by="make")
    .agg(
        [
            pl.col("cylinder_capacity").median().alias("median"),
            pl.col("cylinder_capacity").count().alias("count"),
        ]
    )
    .filter(pl.col("count") > 10)
    .sort(by="median")
    .collect()
)
result[:3]

result.filter(pl.col("make") == "ROLLS ROYCE")

assert result.filter(pl.col("make") == "ROLLS ROYCE")["median"].item() == 6749.0
assert result.filter(pl.col("make") == "ROLLS ROYCE")["count"].item() == 11741.0

# figsize in show_graph
dfple.lazy().filter(pl.col("cylinder_capacity").is_not_null()).groupby(by="make").agg(
    [
        pl.col("cylinder_capacity").median().alias("median"),
        pl.col("cylinder_capacity").count().alias("count"),
    ]
).filter(pl.col("count") > 10).sort(by="median").show_graph()

print(
    dfple.lazy()
    .filter(pl.col("cylinder_capacity").is_not_null())
    .groupby(by="make")
    .agg(
        [
            pl.col("cylinder_capacity").median().alias("median"),
            pl.col("cylinder_capacity").count().alias("count"),
        ]
    )
    .filter(pl.col("count") > 10)
    .sort(by="median")
    .explain(optimized=False)
)

print(
    dfple.lazy()
    .filter(pl.col("cylinder_capacity").is_not_null())
    .groupby(by="make")
    .agg(
        [
            pl.col("cylinder_capacity").median().alias("median"),
            pl.col("cylinder_capacity").count().alias("count"),
        ]
    )
    .filter(pl.col("count") > 10)
    .sort(by="median")
    .explain()
)

# .profile() only exists for Lazy dataframe
dfple.lazy().filter(pl.col("cylinder_capacity").is_not_null()).groupby(by="make").agg(
    [
        pl.col("cylinder_capacity").median().alias("median"),
        pl.col("cylinder_capacity").count().alias("count"),
    ]
).filter(pl.col("count") > 10).sort(by="median").profile()

# .profile() only exists for Lazy dataframe
dfple.lazy().filter(pl.col("cylinder_capacity").is_not_null()).groupby(by="make").agg(
    [
        pl.col("cylinder_capacity").median().alias("median"),
        pl.col("cylinder_capacity").count().alias("count"),
    ]
).filter(pl.col("count") > 10).sort(by="median").profile(
    type_coercion=False,
    predicate_pushdown=False,
    projection_pushdown=False,
    simplify_expression=False,
    slice_pushdown=False,
    common_subplan_elimination=False,
    show_plot=True,
)

dfple.lazy().filter(pl.col("cylinder_capacity").is_not_null()).groupby(by="make").agg(
    [
        pl.col("cylinder_capacity").median().alias("median"),
        pl.col("cylinder_capacity").count().alias("count"),
    ]
).filter(pl.col("count") > 10).sort(by="median").profile(show_plot=True)

# # Resample

dfple.head()

dfple = pl.read_parquet("../test_result_2021on.parquet")

# +
# dfple = dfple.with_columns(pl.col('test_date').dt.year().alias('test_year'))
#dfple = dfple.with_columns((pl.col("test_result") == pl.lit("P")).alias("passed"))
# dfple.sample(10000).with_columns((pl.col('cylinder_capacity')>1000).alias('other'))

# +
# dfple.head(10000).sort(pl.col('test_date')).groupby_dynamic('test_date', every='1w').agg(pl.col('test_type')).mean()
# -

dfple = dfple.with_columns((pl.col("test_result") == pl.lit("P")). \
                           alias("passed"))
result = (
    dfple.sort(pl.col("test_date"))
    .groupby_dynamic("test_date", every="1w")
    .agg(pl.col("passed").sum())
)

# +
from simpler_mpl import set_common_mpl_styles, set_commas

fig, ax = plt.subplots(constrained_layout=True)
ax.plot(result["test_date"], result["passed"])  # , marker='o')
ax.yaxis.set_major_formatter(lambda x, pos: f"{int(x/1000):,}k")
plt.xticks(rotation=-30)
set_common_mpl_styles(
    ax, title="Weekly Test Passes over 2 Years", ymin=0, ylabel="Passing Tests per Week"
)  # , xlabel="Weekly 2021-2022")
ax.axvline(pd.to_datetime("2020-12-25"), color="red")
ax.axvline(pd.to_datetime("2021-12-25"), color="red")
ax.annotate("Christmasses", (pd.to_datetime("2021-12-25"), 200000))
ax.axvline(pd.to_datetime("2022-12-25"), color="red")
# ax.annotate('Christmas', (pd.to_datetime('2022-09-10'), 200000)) # horrid offset for alignment
ax.axvline(pd.to_datetime("2021-03-26"), color="green", linestyle="--")
ax.annotate("April Lockdown\neffect from 2020?", (pd.to_datetime("2021-03-26"), 200000))
# -

# ## Resample but on larger dataset

dfpll = pl.scan_parquet("../test_result.parquet/*.parquet")
dfpll.select(pl.count()).collect().item()



# +
import datetime

# 82012876
dfpll.filter(pl.col("test_date") > datetime.datetime(2021, 1, 1)).select(
    pl.count()
).collect()
# -

result_lz = (
    dfpll.filter(pl.col("test_date") > datetime.datetime(2018, 1, 1))
    .with_columns((pl.col("test_result") == pl.lit("P")).alias("passed"))
    .sort(pl.col("test_date"))
    .groupby_dynamic("test_date", every="1w")
    .agg(pl.col("passed").sum())
    .collect()
)

# +
from simpler_mpl import set_common_mpl_styles, set_commas

fig, ax = plt.subplots(constrained_layout=True)
ax.plot(result_lz["test_date"], result_lz["passed"])  # , marker='o')
ax.yaxis.set_major_formatter(lambda x, pos: f"{int(x/1000):,}k")
plt.xticks(rotation=-30)
set_common_mpl_styles(
    ax, title="Weekly Test Passes over 5 Years", ymin=0, ylabel="Passing Tests per Week"
)  # , xlabel="Weekly 2021-2022")
for dt_str in ["2018-12-25", "2019-12-25", "2020-12-25", "2021-12-25", "2022-12-25"]:
    ax.axvline(pd.to_datetime(dt_str), color="red")
ax.annotate(
    "Christmasses", (pd.to_datetime("2018-12-25"), 200000)
)  # horrid offset for alignment
ax.annotate(
    "Lockdown\nknock-on",
    (pd.to_datetime("2022-06"), 300000),
    # textcoords='axes fraction',
    xytext=(pd.to_datetime("2020-04"), 100000),
    arrowprops=dict(facecolor="black", shrink=0.05),
    horizontalalignment="right",
    verticalalignment="top",
)
# -


