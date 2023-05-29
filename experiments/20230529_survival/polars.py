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

import pandas as pd
import polars as pl

manufacturers = [
    "FORD",
    "VAUXHALL",
    "VOLKSWAGEN",
    "RENAULT",
    "PEUGEOT",
    "TOYOTA",
    "HONDA",
    "CITROEN",
    "BMW",
    "NISSAN",
    "MERCEDES",
    "AUDI",
    "LAND ROVER",
    "MINI",
    "FIAT",
    "SUZUKI",
    "MAZDA",
    "VOLVO",
    "KIA",
    "MITSUBISHI",
    #'SKODA',
    #'HYUNDAI',
    #'SEAT',
    #'JAGUAR',
    #'SAAB'
]

# +
ldf = (
    pl.scan_parquet("../../test_result.parquet/*")
    .filter(pl.col("make").is_in(manufacturers))
    .filter(pl.col("first_use_date").dt.year() == 2005)
    .with_columns(pl.col("test_date").dt.year().alias("year"))
    .groupby(["year", "make", "test_result"])
    .count()
)

ldf.schema
# -

# %%time
edf = ldf.collect(streaming=True)

edf.head()

pdf = edf.to_pandas()
pdf

# +
summary_df = (
    pdf.query('test_result.isin(["F", "P"])')
    .query(f"make in {manufacturers[:10]}")
    .set_index(["year", "test_result", "make"])
    .unstack(["test_result", "make"])
    .sort_index(axis="columns")
    .droplevel(0, axis="columns")
)

summary_df
# -

summary_df[["P"]].droplevel(0, axis="columns")

(summary_df[["P"]].droplevel(0, axis="columns") + summary_df[["F"]]).droplevel(
    0, axis="columns"
)

summary_df[[("P", "TOYOTA")]].plot.bar(figsize=(12, 3))

pass_rate_df = summary_df[["P"]].droplevel(0, axis="columns") / (
    summary_df[["P"]].droplevel(0, axis="columns")
    + summary_df[["F"]].droplevel(0, axis="columns")
)
pass_rate_df

ax = pass_rate_df.loc[2008:].plot(title="Pass Rate for 2005 vehicles", marker=".")
ax.legend(bbox_to_anchor=(1, 1))

pdf.test_result.value_counts()
