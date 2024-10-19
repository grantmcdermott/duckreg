# duckreg

_tl;dr_ Very fast out-of-memory regressions with DuckDB.

R package for running stratified/saturated regressions out-of-memory with DuckDB. The core procedure follows
[Wong _et al_. (2021)](doi:10.48550/arXiv.2102.11297) by reducing
("compressing") the data to a set of summary statistics and then running
frequency-weighted least squares on this smaller dataset. Robust standard errors
are computed from sufficient statistics.

The **duckreg** package is inspired by, and has similar aims to, the
[Python package of the same name](https://github.com/py-econometrics/duckreg).
Compared to the Python implementation, the functionality of this R version is
currently limited to compressed regressions only. But we plan to add support for
Mundlak regression, double-demeaning, etc. in the near future. On the other
hand, this R version does benefit from a _significantly_ smaller dependency
footprint (<5 recursive dependencies vs. over 40), enabling faster and simpler
installs, as well as lower long-term maintenance burden. Remember kids:
[_Lightweight is the right weight_](https://www.tinyverse.org/).

## Install

```r
# install.packages("remotes")
remotes::install_github("grantmcdermott/duckreg")
```

## Use

``` r
library(duckreg)


```