
# Notebooks

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

The `pydatalondon2023` environment is for the main investigation including Pandas 2, Polars and Dask.

```
$ conda create -n pydatalondon2023 python=3.11
pip install -r requirements.txt
```

The `pydatalondon2023_pd15` environment is for Pandas Profiling which needs Pandas 1.5:

```
$ conda create -n pydatalondon2023_pd15 python=3.11
pip install -r requirements_pd15.txt
NOTE ydata_profiling and pandas_profiling don't work with pandas2!
```
