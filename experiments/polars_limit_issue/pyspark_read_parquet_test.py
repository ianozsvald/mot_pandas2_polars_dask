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

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = spark.read.parquet('../../test_result.parquet')
df.dtypes

df.rdd.getNumPartitions()

df.limit(10).show()

# +
dfp = (
    df.withColumns({'test_date': psf.unix_timestamp(psf.col('test_date')),
                    'first_use_date': psf.unix_timestamp(psf.col('first_use_date')),
                   })
    .limit(10)
    .toPandas()
)

dfp['test_date'] = pd.to_datetime(dfp['test_date'], unit='s')
dfp['first_use_date'] = pd.to_datetime(dfp['first_use_date'], unit='s')
dfp.info()
dfp.head()
# -


