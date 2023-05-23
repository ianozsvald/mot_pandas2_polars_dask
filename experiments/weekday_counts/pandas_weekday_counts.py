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

# + active=""
# %%time
#
# counts_df = (
#     pd.read_parquet('../../test_result.parquet',
#                     columns=['test_date', 'test_result'],
#                     filters=[('test_date', '>=', pd.Timestamp('2021-01-01')),
#                              ('test_result', "==", "P"),
#                             ],
#                     dtype_backend='pyarrow'
#                    )
#     .assign(weekday = lambda x: x.test_date.dt.weekday)
#     #.query('test_date >= "2021-01-01" & test_result == "P"')
#     .groupby('weekday')
#     .test_result
#     .count()
# )

# +
# %%time

counts_df = (
    pd.read_parquet('../../test_result.parquet',
                    columns=['test_date', 'test_result'],
                    filters=[('test_date', '>=', pd.Timestamp('2021-01-01')),
                             ('test_result', "==", "P"),
                            ],
                    dtype_backend='pyarrow'
                   )
    .test_date.dt.weekday.value_counts()
)
# -

counts_df#.unstack('test_result')


