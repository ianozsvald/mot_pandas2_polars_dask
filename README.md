# PyDataLondon 2023 & ODSC 2023 talk on "Pandas 2, Polars or Dask"

Giles Weaver (https://www.linkedin.com/in/gilesweaver/) and I (https://www.linkedin.com/in/ianozsvald/) have written a talk comparing these 3 popular dataframe tools on a "slightly larger than RAM" data data. This repo contains the source used in the talks - this is a research repo, _it isn't aimed at you_, it is aimed at us, but you're welcome to look through the source.

The final presentations are here:

* https://speakerdeck.com/ianozsvald/pandas-2-polars-and-dask-pydatalondon-2023 (PyDataLondon 2023)
* https://speakerdeck.com/ianozsvald/odsc-pandas-2-dask-or-polars-quickly-tackling-larger-data-on-a-single-machine (ODSC 2023 - added a couple of extra slides)

On my newsletter (https://notanumber.email/) I've recently written on Polars (https://buttondown.email/NotANumber/archive/you-might-want-to-pay-attention-to-polars-our/) and the next issue should have more information on Pandas 2, Polars and Dask.

If you want to follow the following - the setup instructions are at the bottom, the Notebooks show most of our investigations, there's a couple of text files with our observations and notes.

**No pull requests please** this is just our researh repo for these conferences, we're unlikely to touch this after.


# `experiments/`

## `20230522_pandas_exploration`

* Pandas read_parquet 2021on as parquet costs 82M rows, 11GB and 7s 
* Pandas read_parquet 2021on as numpy nullable costs 82M rows 39GB and 27s 
* Pandas read_parquet 2018on as parquet costs 198M rows, 27GB 30s

EMPTY

## `20230521_sklearn_expt`

It seems that a Polars 2D float array (PyArrow) can be read by sklearn's LR, RF, SVC and LightGBM.

## `subselect_all_tests_to_few_years_polars` (in root)

Polars scan on full results dataset, export a 2021+ 82M row and 2018+ 198M row dataset as new Parquet files.

`f"{pl.scan_parquet(new_path).select(pl.count()).collect().item():,} rows"` count rows.

## `20230520_polars_queries`

Read 1 parquet file, passes vs failures on dt.day

## `20230519_polars_parquet`

Scratch notebook, trying to load many rows (e.g. 300M) into memory directly

## `20230511_pd_load_many`

Load in all the files for one year (e.g. 2021), using pandas, after loading convert to categorical and datetimes, then plot a subsample of mileage vs first use year

Observe that loading many files with pandas from csv is slow and takes circa 8GB 45s, then the concat is very slow 13GB total 30s, categorisation 15s, 400M rows, converting two date cols takes 15s each, pandera checks take 40s, 

In 2021 `df.query('vehicle_id==223981155')` shows 633 rows with DVSA as the model, beige vehicles. Maybe a test vehicle?
 
## `20230509_0|1_explore`

TODO get insights noted...

## `20230503_explore_ydataprofiling`

Use `ydata-profiling` to make unidimensional dumps of data from 2021 for several csv files. 

# DVLA data and documentation

## DVLA data sources

The compressed files need to be downloaded from https://www.data.gov.uk/dataset/e3939ef8-30c7-4ca8-9c7c-ad9475cc9b2f/anonymised-mot-tests-and-results

Documentation is available at:

```
dft_group_detail.csv (6 in total) from lookup.zip
mot-testing-data-user-guide postmay2018.odt
"MOT testing data user guide (pre-May 2018)" on above page has a 1 byte link - note no document found at that link
```

Other docs:

* https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/696292/mot-inspection-manual-for-classes-3-4-5-and-7-from-20-may-2018-draft.pdf
* https://www.gov.uk/guidance/mot-inspection-manual-for-private-passenger-and-light-commercial-vehicles



# Environment setup

## Main investigation

__Update 202311__ I'm using `pydataglobal2023` for the second iteration of this talk, using the same install notes as below.

The `pydatalondon2023` environment is for the main investigation including Pandas 2, Polars and Dask.

```
$ conda create -n pydatalondon2023 python=3.11
NOTE read https://github.com/andrix/python-snappy to install OS libraries (e.g. Ian `sudo apt install libsnappy-dev`)
pip install -r requirements.txt
# Dask optionals FYI: https://docs.dask.org/en/stable/install.html#optional-dependencies
```

From root (where README is) run:

```
# this assumes that mot_pandas2_polars_dask/ contains the checked out code
mot_pandas2_polars_dask$ python scripts/1_link_scraper.py 
# makes a ./data/ folder with 2 csv file
mot_pandas2_polars_dask$ bash scripts/2_acquire_data.sh 
# downloads all zip/tar.gz files, it took 2 hours for Ian leaving 40 files in total in ./data/ (`ls -lA | wc` minus one as total is a summary)
mot_pandas2_polars_dask$ bash scripts/3_decompress_data.sh # giving 65 files in ./data/ (`ls -lA | wc` minus one as the total is a summary)
bash scripts/4_fix_bad_data.sh 
# run convert_csv_to_parquet_with_dask.ipynb
# it generates ./test_result.parquet and ./item.parquet
# run subselect_all_tests_to_few_years_polars.ipynb
# NOTE BUG in subselect_ if does >2021,1,1 query losing circa 600 passing tests on 2022-01-01
# which generates ./test_result_2021on.parquet

```

## ydata-profiling secondary investigation

The `pydatalondon2023_pd15` environment is for Pandas Profiling which needs Pandas 1.5:

```
$ conda create -n pydatalondon2023_pd15 python=3.11
pip install -r requirements_pd15.txt
NOTE ydata_profiling and pandas_profiling don't work with pandas2!
```
