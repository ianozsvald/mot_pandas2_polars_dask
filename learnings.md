# Questions we might ask

* What's the mileage given first-use date? DONE PD for 2021
* What's the mileage given first-use date? DO for 2007 and compare - does mileage seem to change?
* What's the valuecounts for mileages?
* Does petrol/diesel/hybrid/electric distribution change over time?
  * groupby fuel, count for 2007, 2021 - different counts?
* What's the mileage given cylinder capacity for petrol or diesel for e.g. 2010 registered cars?
* What's the mileage grouped by fuel and manufacturer?
* which test centres have the most weird results e.g. missing mileage or cylinders or STeam fuel?
* can we predict mileage given fuel, first use date, vehicle class? test conversion to numpy format

# Data

* possibly there's a relationship between fuel type and mileage (suggested by pandas profiling)

"Vehicles that have an unknown date of manufacture are allocated a first use date of 01/01/1971 by the DVLA. As a result of this, data for 1971 will show anomalies. "

NT normal test, RT retest, PL PV partial 

test_class_id 4 for cars, 7 for trucks, 1-2 bikes, 3 private passenger cars. 0 means pre-computeratisation.

2021 data includes likeamobile steam carriage, LYKAMOBILE 	OPEN TOP CARRIGE  https://www.youtube.com/watch?v=8nHo3NGqrlA

# polars

polars notes

.collect() will collect a lazy evaluation

use .lazy()...collect() to enforce lazy computation

LazyFrame https://towardsdatascience.com/understanding-lazy-evaluation-in-polars-b85ccb864d0c

use .fetch(5) like a pandas head

q.show_graph(optimized=True) is similar to dask

must use alias to rename cols in an agg, two of the same result in an error, no auto renaming as in pandas
df.groupby(by='make').agg([pl.col('cylinder_capacity').count().alias('cyl_size'), pl.col('test_mileage').median().alias('cyl_median')])\
presentation - we could contrast this with pandas behaviour?

.shape
.dtypes

pl.all() # expression for all columns https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.all.html

df[0] gets 0th row

https://pola-rs.github.io/polars-book/user-guide/expressions/numpy/#interoperability
.to_numpy() and .view() for no copy?

streaming would mean reading from e.g. a csv file that's too big for ram - but no docs yet

polars sort uses `descending` but pandas uses `ascending` dfp.select('test_class_id').to_series().value_counts().sort(by='counts', descending=True)

lazyframe filter then sink_parquet
dataframe filter then write_parquet

lazyframe read limit and head seem to read all
lazyframe count (?) not shape to get rows, dataframe has shape, ldf seems to want .fetch


scikit learn
https://github.com/scikit-learn/scikit-learn/issues/25896
" Support other dataframes like polars and pyarrow not just pandas #25896 "
currently only pandas supported, rather than the proposed __dataframe__ protocol
https://vegafusion.io/posts/2023/2023-03-25_Release_1.1.0.html
https://github.com/scikit-learn/scikit-learn/issues/25896#issuecomment-1486249625

getting numpy data out of polars
https://github.com/pola-rs/polars/issues/7961

READ: 

* https://pola-rs.github.io/polars-book/user-guide/migration/pandas/#selecting-data (pandas migration)
* https://pola-rs.github.io/polars-book/user-guide/expressions/user-defined-functions/#to-map-or-to-apply map/apply
