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
# %%time

df = pd.read_parquet(
    "../../test_result.parquet",
    columns=["first_use_date", "test_date", "make", "test_result"],
    filters=[
        ("first_use_date", ">=", pd.Timestamp("2005-01-01")),
        ("first_use_date", "<", pd.Timestamp("2006-01-01")),
    ],
    dtype_backend="pyarrow",
)

# %time df['year'] = df['test_date'].dt.year
# -

manufacturers = df.make.value_counts().index.tolist()
df.make.value_counts()

# %%time
pdf = df.groupby(["year", "make", "test_result"]).agg(
    count=pd.NamedAgg("test_result", "count")
)
pdf

# +
summary_df = (
    pdf.query('test_result.isin(["F", "P"])')
    .query(f"make in {manufacturers[0:10]}")
    # .set_index(['year', 'test_result', 'make'])
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

pdf.reset_index().test_result.value_counts()
