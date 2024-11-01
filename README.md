# duckreg

Very fast out-of-memory regressions with DuckDB.

## What

R package that leverages the power of [DuckDB](https://duckdb.org/) to run
regressions on very large datasets, which may not fit into R's memory.
The core procedure follows
[Wong _et al_. (2021)](https://doi.org/10.48550/arXiv.2102.11297)
by reducing ("compressing") the data to a set of summary statistics and then
running frequency-weighted least squares on this smaller dataset. Robust
standard errors are computed from sufficient statistics.

The **duckreg** package is inspired by, and has similar aims to, the
[Python package of the same name](https://github.com/py-econometrics/duckreg).
Compared to the Python implementation, the functionality of this R version is
currently limited to compressed regressions only. But we plan to add support for
Mundlak regression, double-demeaning, etc. in the near future. On the other
hand, this R version does benefit from a significantly smaller dependency
footprint (<5 recursive dependencies vs. over 40), enabling faster and simpler
installs, as well as a lower long-term maintenance burden.[^1] 

[^1]: Remember kids:
    [_Lightweight is the right weight_](https://www.tinyverse.org/).

## Install

```r
# install.packages("remotes")
remotes::install_github("grantmcdermott/duckreg")
```

## Quickstart

### Small dataset

To get ourselves situated, we'll first demonstrate by using an in-memory R dataset.

``` r
library(duckreg)
library(fixest)   # for data and comparison

data("trade", package = "fixest")

duckreg(Euros ~ dist_km | Destination + Origin, data = trade, vcov = 'hc1')
#> Compressed OLS estimation, Dep. Var.: Euros 
#> Observations.: 38,325 (original) | 210 (compressed) 
#>         Estimate Std. Error t value  Pr(>|t|)    
#> dist_km -45709.8    1195.84 -38.224 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
```

Confirm that this gives us the same result as the `fixest::feols`:

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
**duckreg** offers two basic ways to interact with and analyse data of this size.

#### Option 1: "On-the-fly"

Use the `path` argument to read the data directly from disk and perform the
compression computation in an ephemeral DuckDB connection. This requires that
data are small enough to fit into RAM... but please be aware that this is a very
relative concept. Thanks to DuckDB's incredible efficiency, the "small enough"
limit covers very large datasets that would otherwise crash your R session, and
requires only a fraction of the computation time.

```r
duckreg(
    tip_amount ~ fare_amount + passenger_count | month + vendor_name,
    path = "read_parquet('nyc-taxi/**/*.parquet')" ## path to hive-partitoned dataset
)
#> Compressed OLS estimation, Dep. Var.: tip_amount 
#> Observations.: 178,544,324 (original) | 70,782 (compressed) 
#>                  Estimate Std. Error  t value  Pr(>|t|)    
#> fare_amount      0.106744   0.000068 1564.742 < 2.2e-16 ***
#> passenger_count -0.029086   0.000106 -273.866 < 2.2e-16 ***
#> ---
#> Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
```

Note the size of the original dataset, which is nearly 180 million rows, versus
the compressed dataset, which is down to only 70k. On my laptop this regression
completes in **under 4 seconds**... despite have to read and compress the dataset
from disk!

#### Option 2: Persistent database

While querying on-the-fly is both convenient and extremely performant, you can
of course also run regressions against existing tables in a persistent DuckDB
database. This latter approach requires that you specify appropriate `conn` and
`table` arguments. But note that querying against a persistent database also
means that you can run regressions against bigger than RAM data, since we will
automatically take advantage of DuckDB's
[out-of-core functionality](https://duckdb.org/2024/07/09/memory-management.html) 
(streaming, has aggregation, etc.).

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
      FROM read_parquet('~/Documents/Projects/duckdb-polars/nyc-taxi/**/*.parquet')
      SELECT tip_amount, fare_amount, passenger_count, month, vendor_name
   "
)

# same result as earlier
duckreg(
    tip_amount ~ fare_amount + passenger_count | month + vendor_name,
    conn = con,    # database connection,
    table = "taxi" # table name
)
#> Compressed OLS estimation, Dep. Var.: tip_amount 
#> Observations.: 178,544,324 (original) | 70,782 (compressed) 
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