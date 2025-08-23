# duckreg

Very fast regressions on big datasets.

## What

**duckreg** is an R package that leverages the power of
[DuckDB](https://duckdb.org/) to run regressions on very large datasets, 
which may not fit into R's memory. Various acceleration strategies allow for 
highly efficient computation, while robust standard errors are computed from 
sufficient statistics.

The **duckreg** R package is inspired by, and has similar aims to, the
[Python package of the same name](https://github.com/py-econometrics/duckreg).
This implementation offers some idiomatic, R-focused features like a formula
interface and "pretty" print methods. But the two packages should otherwise
be very similar.

## Install

```r
# install.packages("remotes")
remotes::install_github("grantmcdermott/duckreg")
```

## Quickstart

### Small dataset

To get ourselves situated, we'll first demonstrate by using an in-memory R
dataset.

```r
library(duckreg)
library(fixest)   # for data and comparison

data("trade", package = "fixest")

duckreg(Euros ~ dist_km | Destination + Origin, data = trade, vcov = 'hc1')
#> [duckreg] Estimating compression ratio...
#> [duckreg] Data has 38,325 rows and 210 unique FE groups.
#> [duckreg] Using strategy: compress
#> [duckreg] Executing compress strategy SQL
#> 
#> Compressed OLS estimation, Dep. Var.: Euros 
#> Observations.: 38,325 (original) | 210 (compressed) 
#> Standard-errors: Heteroskedasticity-robust
#>         Estimate Std. Error t value  Pr(>|t|)    
#> dist_km -45709.8    1195.84 -38.224 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
```

Behind the scenes, **duckreg** has compressed the original dataset down from
nearly 40,000 observations to only 210, before running the final (weighted)
regression on this much smaller data object. This compression procedure trick
follows [Wang _et. al. (2021)](https://doi.org/10.48550/arXiv.2102.11297) and
effectively allows us to compute on a much lighter object, saving time and
memory. We can can confirm that it still gives the same result as running 
`fixest::feols` on the full dataset:

```r
feols(Euros ~ dist_km | Destination + Origin, data = trade, vcov = 'hc1')
#> OLS estimation, Dep. Var.: Euros
#> Observations: 38,325
#> Fixed-effects: Destination: 15,  Origin: 15
#> Standard-errors: Heteroskedasticity-robust 
#>         Estimate Std. Error t value  Pr(>|t|)    
#> dist_km -45709.8    1195.84 -38.224 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
#> RMSE: 124,221,786.3     Adj. R2: 0.215289
#>                       Within R2: 0.025914
```

### Big dataset

For a more appropriate **duckreg** use-case, let's run a regression on some NYC
taxi data. (Download instructions
[here](https://grantmcdermott.com/duckdb-polars/requirements.html).)
**duckreg** offers two basic ways to interact with, and analyse, data of this
size.

#### Option 1: "On-the-fly"

Use the `path` argument to read the data directly from disk and perform the
compression computation in an ephemeral DuckDB connection. This requires that
data are small enough to fit into RAM... but please note that "small enough" is
a very relative concept. Thanks to DuckDB's incredible efficiency, your RAM
should be able to handle very large datasets that would otherwise crash your R
session, and require only a fraction of the computation time.

```r
duckreg(
   tip_amount ~ fare_amount + passenger_count | month + vendor_name,
   path = "read_parquet('nyc-taxi/**/*.parquet')", ## path to hive-partitoned dataset
   vcov = "hc1"
)
#> [duckreg] Estimating compression ratio...
#> [duckreg] Data has 178,544,324 rows and 24 unique FE groups.
#> [duckreg] Using strategy: compress
#> [duckreg] Executing compress strategy SQL
#> 
#> Compressed OLS estimation, Dep. Var.: tip_amount 
#> Observations.: 178,544,324 (original) | 70,782 (compressed)
#> Standard Errors: Heteroskedasticity-robust
#>                  Estimate Std. Error  t value  Pr(>|t|)    
#> fare_amount      0.106744   0.000068 1564.742 < 2.2e-16 ***
#> passenger_count -0.029086   0.000106 -273.866 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
```

Note the size of the original dataset, which is nearly 180 million rows, versus
the compressed dataset, which is down to only 70k. On my laptop (M4 MacBook Pro)
this regression completes in **under 2 seconds**... and that includes the time
it took to determine an optimal estimation strategy, as well read the data from
disk![^1]

#### Option 2: Persistent database

While querying on-the-fly is both convenient and extremely performant, you can
of course also run regressions against existing tables in a persistent DuckDB
database. This latter approach requires that you specify appropriate `conn` and
`table` arguments. But note that querying against a persistent database also
means that you can run regressions against bigger than RAM data, since we will
automatically take advantage of DuckDB's
[out-of-core functionality](https://duckdb.org/2024/07/09/memory-management.html) 
(streaming, hash aggregation, etc.).

```r
## Explicitly load the duckdb (and thus also DBI) to create persistent database
## and create a table with our taxi data.
library(duckdb)
#> Loading required package: DBI

# create connection to persistent database
con = dbConnect(duckdb(), dbdir = "nyc.db")

# create a 'taxi' table in our new nyc.db database
dbExecute(
   con,
   "
   CREATE TABLE taxi AS
      FROM read_parquet('nyc-taxi/**/*.parquet')
      SELECT tip_amount, fare_amount, passenger_count, month, vendor_name
   "
)

# same result as earlier
duckreg(
   tip_amount ~ fare_amount + passenger_count | month + vendor_name,
   conn = con,     # database connection,
   table = "taxi", # table name
   vcov = "hc1"
)
#> [duckreg] Estimating compression ratio...
#> [duckreg] Data has 178,544,324 rows and 24 unique FE groups.
#> [duckreg] Using strategy: compress
#> [duckreg] Executing compress strategy SQL
#> 
#> Compressed OLS estimation, Dep. Var.: tip_amount 
#> Observations.: 178,544,324 (original) | 70,782 (compressed) 
#> Standard Errors: Heteroskedasticity-robust
#>                  Estimate Std. Error  t value  Pr(>|t|)    
#> fare_amount      0.106744   0.000068 1564.742 < 2.2e-16 ***
#> passenger_count -0.029086   0.000106 -273.866 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

## (Optional clean-up)
dbRemoveTable(con, "taxi")
dbDisconnect(con)
unlink("nyc.db") # remove from disk
```



[^1]: If we skipped the automatic strategy determination by providing an
   explict strategy, then the total computation time drops to
   _less than 1 second_...