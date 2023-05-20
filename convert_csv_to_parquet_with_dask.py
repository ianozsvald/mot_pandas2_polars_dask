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

# * https://www.gov.uk/guidance/mot-inspection-manual-for-private-passenger-and-light-commercial-vehicles
# * https://www.data.gov.uk/dataset/e3939ef8-30c7-4ca8-9c7c-ad9475cc9b2f/anonymised-mot-tests-and-results
# * https://www.gov.uk/government/news/mot-changes-20-may-2018

# #### Data separator cleanup code

# + language="bash" active=""
# # This identifies the lines with extra separators
# # We can probably fix these with sed
#
# for f in data/test_result_*.txt
#     do echo "Processing $f ..."
#     awk -F"|" 'NF > 14 {print $0}' < $f
#     done

# + magic_args="   " language="bash" active=""
# sed -i -e 's/TOYOTA|Estima |||/TOYOTA|Estima II|/g' test_result_2015.txt
# sed -i -e 's/TOYOTA|Estima |||/TOYOTA|Estima II|/g' test_result_2016.txt
# -

from dask.distributed import Client, wait
from distributed import progress
from glob import glob
from operator import itemgetter
import dask.dataframe as dd
import csv
import pandas as pd

import dask
dask.__version__

client = Client()
client

# +
txt_files = glob('data/test_*.txt')
csv_files = glob('data/unzipped/**/*.csv', recursive=True)

files = txt_files + csv_files
len(files)


# -

def sniff_dialect(file):
    with open(file) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(10000))
        dialect_info = dict(dialect.__dict__)
        dialect_info['file'] = file
    return dialect_info


# +
dialects = [sniff_dialect(file) for file in files]

csv_info_df = (
    pd.DataFrame(dialects)
    .drop(columns=['__module__', '_name', '__doc__'])
    .set_index('file')
    .sort_index()
    .reset_index()
)

csv_info_df
# -

csv_info_df.lineterminator.describe()

csv_info_df.skipinitialspace.describe()

csv_info_df.delimiter.unique()

results_files_df = csv_info_df[csv_info_df.file.str.contains('result')]
results_files_df


# ## Results

# +
def read_results(file, sep):
    df = dd.read_csv(file,
                     delimiter=sep,
                     doublequote=False,
                     on_bad_lines='warn',
                     #include_path_column=True, # DISABLED as Polars doesn't like the resulting categorical column
                     #parse_dates=['test_date'], # DISABLED as causes datetime to be numpy dtypes
                     dtype_backend='pyarrow',
                     # engine='pyarrow',
                    )
    return df

def parse_dates_pd(df):
    df['test_date'] = pd.to_datetime(df['test_date'], format='ISO8601', errors='coerce')
    df['first_use_date'] = pd.to_datetime(df['first_use_date'], format='ISO8601', errors='coerce')
    return df

def parse_dates_dd(df):
    df['test_date'] = dd.to_datetime(df['test_date'], format='ISO8601',
                                     # utc=True,
                                     errors='coerce')
    df['first_use_date'] = dd.to_datetime(df['first_use_date'], format='ISO8601',
                                          # utc=True,
                                          errors='coerce')
    return df

def make_dtypes_pyarrow(df):
    return df.convert_dtypes(dtype_backend='pyarrow')


# -

result_records = results_files_df[['file', 'delimiter']].to_records(index=False)
ddfs = [read_results(file, sep) for file, sep in result_records]

ddf = (
    dd.concat(ddfs[:])
    #.map_partitions(parse_dates_pd) # pandas version
    .pipe(parse_dates_dd) # dask version
    #.map_partitions(make_dtypes_pyarrow) # requires pandas, can't be done with dask
    .astype({'test_date': 'timestamp[us][pyarrow]', 'first_use_date': 'timestamp[us][pyarrow]'})
)

ddf.dtypes

f_result = (
    ddf
    .partitions[:]
    .to_parquet('test_result.parquet', write_index=False, overwrite=True, compute=False)
    .persist()
)
progress(f_result)

# + active=""
# # Test converting to pandas categories
#
# cat_cols = ['test_class_id', 'test_type', 'test_result', 'postcode_area', 'make', 'model', 'colour', 'fuel_type']
#
# f = (
#     dd.read_parquet('test_multiyear.parquet')
#     .categorize(columns=cat_cols)
#     .to_parquet('test_multiyear_cat.parquet', write_index=False, overwrite=True, compute=False)
#     .persist()
# )
#
# progress(f)
# -

# ### Roundtrip

# +
wait(f_result)

ddf_result = dd.read_parquet('test_result.parquet', 
                             # dtype_backend="pyarrow", # causes dates to be read as timestamp[us] rather than timestamp[ns]
                             # use_nullable_dtypes=True, # deprecated
                            )
# -

# Note the differing dtypes
# ddf_result.info(verbose=True)
ddf_result.dtypes

ddf_result.head()

ddf_result.npartitions

rover_df = ddf_result.query('vehicle_id == 1238787680').compute()

rover_df

rover_df.set_index('test_date').test_mileage.plot(marker='.')

# ## Items

item_files_df = csv_info_df[csv_info_df.file.str.contains('item')]
item_files_df


# +
def read_items(file, sep):
    df = dd.read_csv(file,
                     delimiter=sep,
                     dtype={'dangerous_mark': 'string[pyarrow]'},
                     dtype_backend='pyarrow',
                     # engine='pyarrow',
                    )
        
    return df

item_records = item_files_df[['file', 'delimiter']].to_records(index=False)
item_ddfs = [read_items(file, sep) for file, sep in item_records]
# -

item_ddf = dd.concat(item_ddfs[:])

item_ddf.dtypes

item_ddf.info()
item_ddf.head()

# +
f_item = (
    item_ddf
    .to_parquet('item.parquet', write_index=False, overwrite=True, compute=False)
    .persist()
)

progress(f_item)
# -

wait(f_item)
item_ddf = dd.read_parquet('item.parquet')

item_ddf.dtypes

rfr_counts = item_ddf.rfr_id.value_counts().compute()

rfr_counts

# ## Reasons for rejection (test fail)
#
# Not sure if this includes advisories

rfr_df = dd.read_csv('data/unzipped/dft_item_detail.csv', sep='|', dtype_backend='pyarrow')
rfr_df.info(verbose=True)
rfr_df.head()

rfr_df.query('rfr_id == 8394').compute()


