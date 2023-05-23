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

import polars as pl

# 635m row dataset
scan_path = '../../test_result.parquet/*.parquet'

# ## Eager

df = pl.read_parquet(scan_path, n_rows=1000)
df.limit(5)

df = pl.read_parquet('../../test_result.parquet/part.0.parquet', use_pyarrow=True)
df.limit(5)

df.schema

# ## Lazy

lf = pl.scan_parquet(scan_path, n_rows=1000)
lf.schema

# ### Collect

# %%time
lf.limit(5).collect()

# %%time
# Streaming is faster
lf.limit(5).collect(streaming=True)

# ### Fetch

# %%time
lf.fetch(n_rows=5).limit(5)

# %%time
lf.limit(5).fetch(n_rows=5)

# %%time
lf.fetch(streaming=True).limit(5)

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

# ## OOM errors with scan_parquet > limit > collect

# I'm getting to grips with Polars, working towards a talk with @ianozsvald, using the UK MOT vehicle test dataset (~635m rows, 854 partitions on disk, dataset explained [here](https://github.com/pola-rs/polars/issues/8925)).
#
# When prototyping code with Pandas, Dask or PySpark I would often use `.head()` or `.limit()`,
# in order to eyeball data at different stages in a method chained process.
# With Dask & PySpark I usually expect to be able to do this with large data, without issue.
# With Polars, I'm finding that I'm getting bitten by OOM errors, and am trying to understand why.
#
# I started with
#
# `pl.scan_parquet(path).limit(5).collect()` - results in OOM
#
# Then I tried
#
# `pl.scan_parquet(path, n_rows=100).limit(5).collect()` - completes in 1.9s
#
# Having discovered streaming, I found that to be much faster
#
# `pl.scan_parquet(path, n_rows=100).limit(5).collect(streaming=True)` - completes in 85ms
#
# I then tried increasing n_rows on scan, eventually reaching a number larger than the row count of our dataset
#
# `pl.scan_parquet(path, n_rows=1000000000).limit(5).collect(streaming=True)` - completes in 110ms
#
# But without passing n_rows
#
# `pl.scan_parquet(path).limit(5).collect(streaming=True)` - results in OOM
#
# For completeness
#
# `pl.scan_parquet(path, n_rows=1000000000).limit(5).collect()` - results in OOM
#
# This raises a few questions:
#
# * Is OOM error expected with (large) `LazyFrame.limit(5)` ?
# * Why would I not want to use `streaming=True` ? (I'm aware that the functionality behind this may be incomplete)
# * Why does scanning n_rows larger than my dataset prevent OOM error in conjunction with `streaming=True` ?
# * Are there any plans for a `use_pyarrow` option to `scan_parquet`, as with `read_parquet`?
#
# I have discovered `LazyFrame.fetch()` and can see how that would be useful. I'm also aware that `df.limit` is an alias to `df.head`.

pl.show_versions()


