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

# # OOM errors with scan_parquet > limit > collect
#
# See https://github.com/pola-rs/polars/issues/9001

import polars as pl
import pyarrow.dataset as ds

# 635m row dataset
scan_path = "../../test_result.parquet/*.parquet"

# ## Eager

df = pl.read_parquet(scan_path, n_rows=1000)
df.limit(5)

df = pl.read_parquet("../../test_result.parquet/part.0.parquet", use_pyarrow=True)
df.limit(5)

df.schema

# ## Lazy

lf = pl.scan_parquet(scan_path, n_rows=1000)
lf.schema

lf = pl.scan_pyarrow_dataset(ds.dataset("../../test_result.parquet", format="parquet"))
lf.schema

# ### Collect

# %%time
lf.limit(5).collect()

# %%time
# Streaming is faster
lf.limit(5).collect(streaming=True)

# ### Fetch

# + active=""
# %%time
# # OOMs
# lf.fetch(n_rows=5).limit(5)
# -

# %%time
lf.limit(5).fetch(n_rows=5)

# + active=""
# %%time
# # OOMs
# lf.fetch(streaming=True).limit(5)
# -

# %%time
# More speed!
lf.limit(5).fetch(streaming=True)

# ## Issue report examples

# %%time
# OOM
# pl.scan_parquet(scan_path).limit(5).collect()

# %%time
pl.scan_parquet(scan_path, n_rows=100).limit(5).collect()

# %%time
pl.scan_parquet(scan_path, n_rows=100).limit(5).collect(streaming=True)

# %%time
pl.scan_parquet(scan_path, n_rows=1000000000).limit(5).collect(streaming=True)

# %%time
# OOM
# pl.scan_parquet(scan_path).limit(5).collect(streaming=True)

# %%time
# OOM
# pl.scan_parquet(scan_path, n_rows=1000000000).limit(5).collect()
