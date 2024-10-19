#' Run a compressed regression with a DuckDB backend.
#'
#' @md
#' @param fml Model \code{\link[stats]{formula}}.
#' @param conn Connection to a DuckDB database, e.g. created with
#' \code{\link[DBI]{dbConnect}}. Can be either persistent (disk-backed) or
#' ephemeral (in-memory). If no conection is provided, then an ephemeral
#' connection will automatically be created.
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
#' @return A two-column matrix containing the regression results:
#' - estimates: Point estimates
#' - std.error: Standard errors
#' 
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb duckdb_register
#' @importFrom Matrix Diagonal sparse.model.matrix
#' @importFrom stats reformulate
#' @importFrom glue glue glue_sql
#' 
#' @examples
#' 1+1
#' @export
duckreg = function(
   fml,
   conn = NULL,
   table = NULL,
   data = NULL,
   path = NULL,
   vcov = "hc1"
   ) {

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
     vars = all.vars(fml)
     yvar = vars[1]
     xvars = vars[-1]

     # query string
     query_string = paste0(
         "
        WITH cte AS (
           SELECT
              ",
         paste(xvars, collapse = ", "), ",
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

     # fetch data
     compressed_dat = dbGetQuery(conn = conn, query_string)

     # design and outcome matrices
     X = sparse.model.matrix(reformulate(c(xvars)), compressed_dat)
     Y = compressed_dat[, "mean_Y"]
     Xw = X * compressed_dat[["wts"]]
     Yw = Y * compressed_dat[["wts"]]

     # beta values
     betahat = chol2inv(chol(crossprod(Xw))) %*% crossprod(Xw, Yw)

     # standard errors (currently only HC1)
     if (vcov == tolower("hc1")) {
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

     # return object
     ret = cbind(estimate = betahat[, 1], std.error = ses)

     return(ret)
}
