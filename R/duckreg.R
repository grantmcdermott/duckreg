#' Run a compressed regression with a DuckDB backend.
#'
#' @md
#' @description
#' Leverages the power of DuckDB to run regressions on very large datasets,
#' which may not fit into R's memory. The core procedure follows Wong et al.
#' (2021) by reducing ("compressing") the data to a set of summary statistics
#' and then running frequency-weighted least squares on this smaller dataset.
#' Robust standard errors are computed from sufficient statistics.
#' 
#' @param fml A \code{\link[stats]{formula}} representing the relation to be
#' estimated. Fixed-effects should be included after a pipe, e.g
#' `fml = y ~ x1 + x2 | fe1 + f2`. Currently, only simple additive terms
#' are supported (i.e., no interaction terms, transformations or literals).
#' @param conn Connection to a DuckDB database, e.g. created with
#' \code{\link[DBI]{dbConnect}}. Can be either persistent (disk-backed) or
#' ephemeral (in-memory). If no connection is provided, then an ephemeral
#' connection will be created automatically and closed before the function
#' exits. Note that a persistent (disk-backed) database connection is
#' required for larger-than-RAM datasets in order to take advantage of DuckDB's
#' streaming functionality.
#' @param table,data,path Mututally exclusive arguments for specifying the data
#' table (object) to be queried. In order of precedence:
#' - `table`: Character string giving the name of the data table in an
#' existing (open) DuckDB connection.
#' - `data`: R dataframe that can be copied over to `conn` as a temporary
#' table for querying via the DuckDB query engine. Ignored if `table` is
#' provided.
#' - `path`: Character string giving a path to the data file(s) on disk, which
#' will be read into `conn`. Internally, this string is passed to the `FROM`
#' query statement, so could (should) include file globbing for
#' Hive-partitioned datasets, e.g. `"mydata/**/.*parquet"`. For more precision,
#' however, it is recommended to pass the desired DuckDB reader function as
#' part of this string, e.g. `"read_parquet('mydata/**/*.parquet')"`;
#' note the use of single quotes.
#' Ignored if either `table` or `data` is provided. 
#' @param vcov Character string denoting the desired type of variance-
#' covariance correction / standard errors. At present, only "hc1"
#' (heteroskedasticity-consistent) are supported, which is also thus
#' the default.
#' @param query_only Logical indicating whether only the underlying compression
#'   SQL query should be returned (i.e., no computation will be performed).
#'   Default is `FALSE`.
#' @param data_only Logical indicating whether only the compressed dataset
#'   should be returned (i.e., no regression is run). Default is `FALSE`.
#' 
#' @return A list of class "duckreg" containing various slots, including a table
#' of coefficients (which the associated print method will display).
#' @references
#' Wong, J., Forsell, E., Lewis, R., Mao, T., & Wardrop, M. (2021).
#' \cite{You Only Compress Once: Optimal Data Compression for Estimating Linear Models.} 
#' arXiv preprint arXiv:2102.11297.
#' Available: https://doi.org/10.48550/arXiv.2102.11297
#' 
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb duckdb_register
#' @importFrom Formula Formula
#' @importFrom Matrix chol2inv crossprod Diagonal sparse.model.matrix 
#' @importFrom stats reformulate
#' @importFrom glue glue glue_sql
#' 
#' @examples
#' 
#' # A not very compelling example using a small in-memory dataset:
#' (mod = duckreg(Temp ~ Wind | Month, data = airquality))
#' 
#' Same result as lm
#' summary(lm(Temp ~ Wind + factor(Month), data = airquality))
#' 
#' # Aside: duckreg's default print method hides the "nuisance" coefficients
#' # like the intercept and fixed effect(s). But we can grab them if we want.
#' print(mod, fes = TRUE)
#' 
#' # Note: for a more compelling and appropriate use-case, i.e. regression on a
#' # big (~180 million row) dataset of Hive-partioned parquet files, see the
#' # package website:
#' # https://github.com/grantmcdermott/duckreg?tab=readme-ov-file#quickstart
#' @export
duckreg = function(
   fml,
   conn = NULL,
   table = NULL,
   data = NULL,
   path = NULL,
   vcov = "hc1",
   query_only = FALSE,
   data_only = FALSE
   ) {
  
     # compress = match.arg(compress) 
       
     if (is.null(conn)) {
         conn = dbConnect(duckdb(), shutdown = TRUE)
         on.exit(dbDisconnect(conn), add = TRUE)
     }
     
     if (!is.null(table)) {
      if (!is.character(table)) stop("\nThe `table` argument must be a character string.\n")
      # from_statement = paste("FROM", table)
      from_statement = glue("FROM {table}")
     } else if (!is.null(data)) {
      if (!inherits(data, "data.frame")) stop("\nThe `data` argument must be a data.frame.\n")
      duckdb_register(conn, "tmp_table", data)
      # DBI::dbWriteTable(conn, "tmp_table", data)
      from_statement = "FROM tmp_table"
     } else if (!is.null(path)) {
      if (!is.character(path)) stop("\nThe `path` argument must be a character string.\n")
      if (!(grepl("^read|^scan", path) && grepl("'", path))) {
         path = gsub('"', "'", path)
         from_statement = glue("FROM '{path}'")
      } else {
         from_statement = glue("FROM {path}")
      }
     } else {
      stop("\nOne of of `table`, `data`, or `path` arguments is required.\n")
     }

     # vars of interest
     fml = Formula(fml)
     yvar = all.vars(formula(fml, lhs = 1, rhs = 0))
     xvars = all.vars(formula(fml, lhs = 0, rhs = 1))
     fes = if (length(fml)[2] > 1) all.vars(formula(fml, lhs = 0, rhs = 2)) else NULL
   #   vars = all.vars(fml)
   #   yvar = vars[1]
   #   xvars = vars[-1]
     
     # query string
     query_string = paste0(
         "
        WITH cte AS (
           SELECT
              ",
         paste(c(xvars, fes), collapse = ", "), ",
              COUNT(*) AS n,
              SUM(", yvar, ") as sum_Y,
              SUM(POW(", yvar, ", 2)) as sum_Y_sq,
           ", from_statement, "
           GROUP BY ALL
        )
        FROM cte
        SELECT
           *,
           sum_Y / n AS mean_Y,
           sqrt(n) AS wts
        "
     )
   
     if (isTRUE(query_only)) return(query_string)
     
     # fetch data
     compressed_dat = dbGetQuery(conn = conn, query_string)

     # turn FEs into factors
     for (f in fes) {
        compressed_dat[[f]] = factor(compressed_dat[[f]])
     }
     rm(f)
     
     if (isTRUE(data_only)) return(compressed_dat)

     # design and outcome matrices
     X = sparse.model.matrix(reformulate(c(xvars, fes)), compressed_dat)
     Y = compressed_dat[, "mean_Y"]
     Xw = X * compressed_dat[["wts"]]
     Yw = Y * compressed_dat[["wts"]]

     # beta values
     betahat = chol2inv(chol(crossprod(Xw))) %*% crossprod(Xw, Yw)

     # standard errors (currently only HC1)
     vcov_type = tolower(vcov)
     if (identical(vcov_type, "hc1")) {
        n = compressed_dat[["n"]]
        yprime = compressed_dat[["sum_Y"]]
        yprimeprime = compressed_dat[["sum_Y_sq"]]
        # Compute yhat
        yhat = X %*% betahat
        # Compute rss_g
        rss_g = (yhat^2) * n - 2 * yhat * yprime + yprimeprime
        # Compute vcov components
        bread = solve(crossprod(X, Diagonal(x = n) %*% X))
        meat = crossprod(X, Diagonal(x = as.vector(rss_g)) %*% X)
        n_nk = sum(n) / (sum(n) - ncol(X))
        vcov = n_nk * (bread %*% meat %*% bread)
        vcov = as.matrix(vcov)
        # grab SEs
        ses = sqrt(diag(vcov))
     }
     attr(vcov, "type") = vcov_type

     # return object
     coefs = betahat[, 1]
     zvalues = coefs / ses
     nparams = length(coefs)
     nobs = nrow(compressed_dat)
     nobs_orig = sum(compressed_dat$n)
     pvalues = 2*pt(-abs(zvalues), max(nobs - nparams, 1))
     coeftable = cbind(
        estimate = coefs,
        std.error = ses,
        statistic = zvalues,
        p.values = pvalues
     )

     ret = list(
      coeftable = coeftable,
      vcov = vcov,
      fml = fml,
      yvar = yvar,
      xvars = xvars,
      fes = fes,
      query_string = query_string,
      nobs = nobs,
      nobs_orig = nobs_orig
     )

     ## Overload class ----
     class(ret) = c("duckreg", class(ret))

     ret
}
