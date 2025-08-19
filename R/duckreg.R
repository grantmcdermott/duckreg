
#' Run a compressed regression with a DuckDB backend.
#'
#' @md
#' @description
#' Leverages the power of DuckDB to run regressions on very large datasets,
#' which may not fit into R's memory. Various acceleration strategies allow for
#' efficient computation, while robust standard errors are computed from
#' sufficient statistics.
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
#' @param strategy Character string indicating the preferred acceleration
#'   strategy. The default `"auto"` will pick an optimal strategy based on
#'   internal heuristics. Users can also override with one of the following
#'   explicit strategies: `"compress"`, `"mundlak"`, or `"moments"`. See
#'   the Acceleration Strategies section below for details.
#' @param threshold Numeric. Threshold for determining the acceleration
#'   `strategy` under the default `"auto"` option. Maps to the ratio between
#'   (a) unique values of combined covariates and fixed effects, and (b) total
#'   rows in the dataset. If this ratio is below the given the given threshold
#'   (default = 0.1%), then the `"compress"` strategy is used, otherwise
#'   `"mundlak"` or `"moments"` depending on the number of fixed effects. 
#' @param verbose Logical. Print progress messages to the console? Defaults to
#'   `TRUE`. 
#' @param ridge_rel Relative ridge penalty to be used if the default Cholesky
#'   solve fails; see \code{\link[Matrix]{Cholesky}}. Technical note:
#'   `lambda = ridge_rel * mean(diag(X'X))`.
#' 
#' @return A list of class "duckreg" containing various slots, including a table
#' of coefficients (which the associated print method will display).
#' 
#' @section Acceleration strategies:
#' 
#' `duckreg` offers three primary acceleration (shortcut) strategies:
#' 
#' 1. `"compress"`: compress the size of data via a `GROUP BY` operation (using regressors + fixed effects) and then run frequency-weighted least squares on the smaller dataset. This procedure follows the "optimal data compression" strategy proposed by Wang et. al. (2021).
#' 2. `"moments"`: calculate sufficient statistics using global means ($X'X, X'y$). Limited to cases without fixed effects.
#' 3. `"mundlak"`: as per `"moments"`, but first subtract group-level means from the observations. Permits at most two fixed-effects (i.e., either demean or double-demean). This procedure follows the "generalized Mundlak estimator" proposed by Arkhangelsky & Imbens (2024).
#' 
#' The relative efficiency of each of these strategies depends on the size and
#' structure of the data, as well the number of unique regressors and
#' fixed-effects. While the compression approach can yield remarkable
#' performance gains for "standard" cases, it is less efficient for a true panel
#' (repeated cross-sections over time), where N >> T. In such cases, it is more
#' efficient to use a Mundlak-type representation that subtracts group means
#' first. (Reason: unit and time fixed effects are typically high dimensional, 
#' but covariate averages are not.)
#' 
#' If the user does not specify an explicit acceleration strategy, then
#' `duckreg` will invoke an `"auto"` heuristic behind the scenes. This requires
#' some additional overhead, but in most cases should be negligible next to the
#' overall time savings. The heuristic is as follows:
#' 
#' - IF no fixed-effects AND (any continuous regressor OR compression ratio > threshold) => `"moments"`.
#' - ELSE IF 1-2 fixed-effects AND estimated compression ratio high (i.e., > max(0.6, threshold)) => `"mundlak"`.
#' - ELSE => `"compress"`.
#' 
#' @references
#' Arkhangelsky, D. & Imbens, G. (2024)
#' \cite{Fixed Effects and the Generalized Mundlak Estimator}.
#' The Review of Economic Studies, 91(5), pp. 2545–2571.
#' Available: https://doi.org/10.1093/restud/rdad089
#' 
#' Wong, J., Forsell, E., Lewis, R., Mao, T., & Wardrop, M. (2021).
#' \cite{You Only Compress Once: Optimal Data Compression for Estimating Linear Models.} 
#' arXiv preprint arXiv:2102.11297.
#' Available: https://doi.org/10.48550/arXiv.2102.11297
#' 
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb duckdb_register
#' @importFrom Formula Formula
#' @importFrom Matrix chol2inv crossprod Diagonal sparse.model.matrix 
#' @importFrom stats reformulate pt
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
  data_only = FALSE,
  strategy = c("auto","compress","moments","mundlak"),
  threshold = 0.001,
  verbose = TRUE,
  ridge_rel = 1e-12
) {
  # Process and validate inputs
  inputs <- process_duckreg_inputs(
    fml, conn, table, data, path, vcov, strategy, 
    query_only, data_only, threshold, verbose, ridge_rel
  )
  
  # Choose strategy
  chosen_strategy <- choose_strategy(inputs)
  
  # Execute chosen strategy
  result <- switch(chosen_strategy,
    # sufficient statistics with no fixed effects
        "moments" = execute_moments_strategy(inputs), 
    # one or two-way fixed effects
        "mundlak" = execute_mundlak_strategy(inputs), 
    # group by regressors (+ fixed effects) -> frequency-weighted rows -> WLS
    # best when regressors are discrete and FE groups have many rows per unique value
        "compress" = execute_compress_strategy(inputs), 
    stop("Unknown strategy: ", chosen_strategy)
  )
  
  # Finalize result
  finalize_duckreg_result(result, inputs, chosen_strategy)
}

#' Process and validate duckreg inputs
#' @keywords internal
process_duckreg_inputs <- function(fml, conn, table, data, path, vcov, strategy, 
                                  query_only, data_only, threshold, verbose, ridge_rel) {
  strategy <- match.arg(strategy, c("auto","compress","moments","mundlak"))
  vcov_type_req <- tolower(vcov)  # Connection handling
  own_conn <- FALSE
  if (is.null(conn)) {
    conn <- dbConnect(duckdb::duckdb(), shutdown = TRUE)
    own_conn <- TRUE
   #  on.exit(try(dbDisconnect(conn), silent = TRUE), add = TRUE)
  }

  # FROM clause
  if (!is.null(table)) {
    if (is.character(table)) {
      # Original behavior: table name
      from_statement <- glue::glue("FROM {table}")
    } else if (inherits(table, "tbl_lazy")) {
      # lazy table: render SQL and try to extract connection
      rendered_sql <- tryCatch(dbplyr::sql_render(table), error = function(e) NULL)
      if (is.null(rendered_sql)) stop("Failed to render SQL for provided tbl_lazy.")
      from_statement <- paste0("FROM (", rendered_sql, ") AS lazy_subquery")
      if (is.null(conn)) {
        # try to extract DBI connection from the tbl_lazy (tbl_dbi stores it at src$con)
        if (!is.null(table$src) && !is.null(table$src$con)) {
          conn <- table$src$con
        } else {
          stop("`conn` is NULL and could not be extracted from the provided tbl_lazy. Provide `conn` explicitly.")
        }
      }
    } else {
      stop("`table` must be character or tbl_lazy object.")
    }
  } else if (!is.null(data)) {
    if (!inherits(data, "data.frame")) stop("`data` must be data.frame.")
    duckdb::duckdb_register(conn, "tmp_table_duckreg", data)
    from_statement <- "FROM tmp_table_duckreg"
  } else if (!is.null(path)) {
    if (!is.character(path)) stop("`path` must be character.")
    if (!(grepl("^read|^scan", path) && grepl("'", path))) {
      path <- gsub('"', "'", path)
      from_statement <- glue::glue("FROM '{path}'")
    } else {
      from_statement <- glue::glue("FROM {path}")
    }
  } else {
    stop("Provide one of `table`, `data`, or `path`.")
  }

  # Parse formula
  fml <- Formula(fml)
  yvar <- all.vars(formula(fml, lhs = 1, rhs = 0))
  if (length(yvar) != 1) stop("Exactly one outcome variable required.")
  
  xvars <- all.vars(formula(fml, lhs = 0, rhs = 1))
  fes <- if (length(fml)[2] > 1) all.vars(formula(fml, lhs = 0, rhs = 2)) else NULL
  if (!length(xvars)) stop("No regressors on RHS.")

  # Heuristic for continuous regressors (only if data passed)
  is_continuous <- function(v) {
    if (is.null(data)) return(NA)
    xv <- data[[v]]
    if (is.integer(xv)) return(FALSE)
    if (is.numeric(xv)) return(length(unique(xv)) > min(50, 0.2 * length(xv)))
    TRUE
  }
  any_continuous <- if (!is.null(data)) any(vapply(xvars, is_continuous, logical(1))) else FALSE

  list(
    fml = fml,
    yvar = yvar,
    xvars = xvars,
    fes = fes,
    conn = conn,
    from_statement = from_statement,
    data = data,
    vcov_type_req = vcov_type_req,
    strategy = strategy,
    query_only = query_only,
    data_only = data_only,
    threshold = threshold,
    verbose = verbose,
    ridge_rel = ridge_rel,
    any_continuous = any_continuous,
    own_conn = own_conn
  )
}

#' Check if the database backend supports COUNT_BIG
#'
#' This function checks whether the provided database connection is to a backend
#' that supports the `COUNT_BIG` function, such as SQL Server or Azure SQL.
#'
#' @param conn A DBI database connection object.
#'
#' @return Logical value: `TRUE` if the backend supports `COUNT_BIG`, `FALSE` otherwise.
#' @examples
#' \dontrun{
#'   con <- DBI::dbConnect(odbc::odbc(), ...)
#'   backend_supports_count_big(con)
#' }
#' @export
backend_supports_count_big <- function(conn){
  info <- try(DBI::dbGetInfo(conn), silent = TRUE)
  if (inherits(info, "try-error")) return(FALSE)
  dbms <- tolower(paste(info$dbms.name, collapse = " "))
  grepl("sql server|azure sql|microsoft sql server", dbms)
}

# detect SQL backend
detect_backend <- function(conn) {
  info <- try(DBI::dbGetInfo(conn), silent = TRUE)
  if (inherits(info, "try-error")) return(list(name = "unknown", supports_count_big = FALSE))
  dbms <- tolower(paste(info$dbms.name, collapse = " "))
  list(
    name = if (grepl("duckdb", dbms)) "duckdb" else if (grepl("sql server|azure sql|microsoft sql server", dbms)) "sqlserver" else "other",
    supports_count_big = grepl("sql server|azure sql|microsoft sql server", dbms)
  )
}

# sql_count: returns an expression fragment for use inside SELECT when possible.
sql_count <- function(conn, alias, expr = "*", distinct = FALSE) {
  bd <- detect_backend(conn)
  if (distinct) {
    glue::glue("{if (bd$supports_count_big) paste0('COUNT_BIG(DISTINCT ', expr, ')') else paste0('CAST(COUNT(DISTINCT ', expr, ') AS BIGINT)')} AS {alias}")
  } else {
    if (bd$supports_count_big) {
      glue::glue("COUNT_BIG({expr}) AS {alias}")
    } else {
      glue::glue("CAST(COUNT({expr}) AS BIGINT) AS {alias}")
    }
  }
}

#' Choose regression strategy based on inputs and auto logic
#' @keywords internal
choose_strategy <- function(inputs) {
  # Extract values
  strategy <- inputs$strategy
  fes <- inputs$fes
  verbose <- inputs$verbose
  any_continuous <- inputs$any_continuous
  threshold <- inputs$threshold
  conn <- inputs$conn
  from_statement <- inputs$from_statement
  xvars <- inputs$xvars
  
    # Compression ratio estimator
    estimate_compression <- function(inputs) {
    conn <- inputs$conn
    verbose <- inputs$verbose
    xvars <- inputs$xvars
    fes <- inputs$fes
    from_statement <- inputs$from_statement

    if (verbose) message("[duckreg] Estimating compression ratio...")
    key_cols <- c(xvars, fes)
    if (!length(key_cols)) return(1)

    # Total rows (safe: COUNT(*) is supported pretty much everywhere)
    total_sql <- glue::glue("SELECT CAST(COUNT(*) AS BIGINT) AS n FROM (SELECT * {from_statement}) t")
    total_n <- DBI::dbGetQuery(conn, total_sql)$n

    # Helper to count distinct tuples (works for single or multi-column)
    count_distinct_tuples <- function(cols) {
      cols_expr <- paste(cols, collapse = ", ")
      # Use subquery counting distinct tuples (portable and works in DuckDB/SQL Server/etc)
      sql <- glue::glue("SELECT CAST(COUNT(*) AS BIGINT) AS g FROM (SELECT DISTINCT {cols_expr} {from_statement}) t")
      DBI::dbGetQuery(conn, sql)$g
    }

    if (length(fes)) {
      # count unique FE groups (may be single or multi-column)
      n_groups_fe <- tryCatch(count_distinct_tuples(fes), error = function(e) NA_integer_)
    } else {
      n_groups_fe <- NA_integer_
    }

    # count unique keys over regressors + FEs (may be multi-column)
    n_groups_total <- tryCatch(count_distinct_tuples(key_cols), error = function(e) NA_integer_)

    if (verbose && length(fes) && !is.na(n_groups_fe)) {
      message("[duckreg] Data has ", format(total_n, big.mark = ","), 
              " rows and ", format(n_groups_fe, big.mark = ","), " unique FE groups.")
    }

    n_groups_total / max(total_n, 1)
  }

  chosen_strategy <- strategy
  est_cr <- NA_real_

  # Auto logic
  if (strategy == "auto") {
    if (length(fes) == 0) {
      est_cr <- tryCatch(estimate_compression(inputs), error = function(e) NA_real_)
      if (any_continuous || (!is.na(est_cr) && est_cr > threshold)) {
        chosen_strategy <- "moments"
      } else {
        chosen_strategy <- "compress"
      }
    } else if (length(fes) %in% c(1, 2)) {  
      est_cr <- tryCatch(estimate_compression(inputs), error = function(e) NA_real_)
      if (!is.na(est_cr) && est_cr > max(0.6, threshold)) {
        chosen_strategy <- "mundlak"
        if (verbose) message("[duckreg] Auto: selecting mundlak (estimated compression ratio ",
                             sprintf("%.2f", est_cr), ").")
      } else {
        chosen_strategy <- "compress"
      }
    } else {
      est_cr <- tryCatch(estimate_compression(inputs), error = function(e) NA_real_)
      chosen_strategy <- "compress"
      if (!is.na(est_cr) && est_cr > 0.8 && verbose) {
        message(sprintf("[duckreg] Auto: high compression ratio (%.4f). Group compression preferred for this FE structure.", est_cr))
      }
    }
    if (verbose && (strategy !="auto")) {message("Compression ratio: ", ifelse(is.na(est_cr), "unknown", sprintf("%.2f", est_cr)))}
  } else {
    chosen_strategy <- strategy
  }

  if (verbose) {message("[duckreg] Using strategy: ", chosen_strategy)}

  # Guard unsupported combos
  if (chosen_strategy == "moments" && length(fes) > 0) {
    if (verbose) message("[duckreg] FE present; moments (no-FE) not applicable. Using compress.")
    chosen_strategy <- "compress"
  }
  if (chosen_strategy == "mundlak" && !(length(fes) %in% c(1, 2))) {
    if (verbose) message("[duckreg] mundlak requires one or two FEs. Using compress.")
    chosen_strategy <- "compress"
  }
  
  # Store compression ratio estimate for later use
  inputs$compression_ratio_est <- est_cr
  
  chosen_strategy
}

#' Execute moments strategy (no fixed effects)
#' @keywords internal
execute_moments_strategy <- function(inputs) {
  if (inputs$query_only) stop("query_only unsupported for moments.")
  if (inputs$data_only) warning("data_only ignored for moments.")
  
  pair_exprs <- c(
    "COUNT(*) AS n_total",
    glue("SUM({inputs$yvar}) AS sum_y"),
    glue("SUM({inputs$yvar}*{inputs$yvar}) AS sum_y_sq")
  )
  for (x in inputs$xvars) {
    pair_exprs <- c(pair_exprs,
                    glue("SUM({x}) AS sum_{x}"),
                    glue("SUM({x}*{inputs$yvar}) AS sum_{x}_y"),
                    glue("SUM({x}*{x}) AS sum_{x}_{x}"))
  }
  if (length(inputs$xvars) > 1) {
    for (i in seq_along(inputs$xvars)) {
      if (i == 1) next
      for (j in seq_len(i - 1)) {
        xi <- inputs$xvars[i]; xj <- inputs$xvars[j]
        pair_exprs <- c(pair_exprs, glue("SUM({xi}*{xj}) AS sum_{xi}_{xj}"))
      }
    }
  }
  moments_sql <- paste0(
    "SELECT\n  ",
    paste(pair_exprs, collapse = ",\n  "),
    "\n", inputs$from_statement
  )
  if (inputs$verbose) {message("[duckreg] Executing moments SQL \n")}
  moments_df <- dbGetQuery(inputs$conn, moments_sql)
  n_total <- moments_df$n_total

  vars_all <- c("(Intercept)", inputs$xvars)
  p <- length(vars_all)
  XtX <- matrix(0, p, p, dimnames = list(vars_all, vars_all))
  Xty <- matrix(0, p, 1, dimnames = list(vars_all, ""))

  XtX["(Intercept)","(Intercept)"] <- n_total
  Xty["(Intercept)",] <- moments_df$sum_y
  for (x in inputs$xvars) {
    sx  <- moments_df[[paste0("sum_", x)]]
    sxx <- moments_df[[paste0("sum_", x, "_", x)]]
    sxy <- moments_df[[paste0("sum_", x, "_y")]]
    XtX["(Intercept)", x] <- XtX[x,"(Intercept)"] <- sx
    XtX[x, x] <- sxx
    Xty[x,] <- sxy
  }
  if (length(inputs$xvars) > 1) {
    for (i in seq_along(inputs$xvars)) {
      if (i == 1) next
      for (j in seq_len(i - 1)) {
        xi <- inputs$xvars[i]; xj <- inputs$xvars[j]
        val <- moments_df[[paste0("sum_", xi, "_", xj)]]
        XtX[xi, xj] <- XtX[xj, xi] <- val
      }
    }
  }

  Rch <- tryCatch(chol(XtX), error = function(e) {
    ridge <- inputs$ridge_rel * mean(diag(XtX))
    chol(XtX + diag(ridge, p))
  })
  betahat <- backsolve(Rch, forwardsolve(Matrix::t(Rch), Xty))
  rownames(betahat) <- vars_all

  rss <- as.numeric(moments_df$sum_y_sq - 2 * t(betahat) %*% Xty + t(betahat) %*% XtX %*% betahat)
  df_res <- max(n_total - p, 1)
  sigma2 <- rss / df_res
  XtX_inv <- chol2inv(Rch)
  vcov_mat <- sigma2 * XtX_inv
  if (inputs$vcov_type_req == "hc1") {
    vcov_mat <- vcov_mat * (n_total / df_res)
    attr(vcov_mat, "type") <- "hc1"
  } else attr(vcov_mat, "type") <- "ols"

  coefs <- as.numeric(betahat); names(coefs) <- vars_all
  ses <- sqrt(Matrix::diag(vcov_mat))
  tstats <- coefs / ses
  pvals <- 2 * pt(-abs(tstats), df_res)
  coeftable <- cbind(estimate = coefs, std.error = ses, statistic = tstats, p.values = pvals)

  list(
    coeftable = coeftable,
    vcov = vcov_mat,
    fml = inputs$fml,
    yvar = inputs$yvar,
    xvars = inputs$xvars,
    fes = NULL,
    query_string = moments_sql,
    nobs = 1L,
    nobs_orig = n_total,
    strategy = "moments",
    compression_ratio_est = inputs$compression_ratio_est,
    df_residual = df_res
  )
}

#' Execute mundlak strategy (1-2 fixed effects)
#' @keywords internal
execute_mundlak_strategy <- function(inputs) {
  if (inputs$query_only) stop("query_only unsupported for mundlak.")
  if (inputs$data_only) warning("data_only ignored for mundlak.")

  if (length(inputs$fes) == 1) {
    # Single FE: simple within-group demeaning
    fe1 <- inputs$fes[1]
    all_vars <- c(inputs$yvar, inputs$xvars)
    
    means_cols <- paste(sprintf("AVG(%s) AS %s_mean", all_vars, all_vars), collapse = ", ")
    tilde_exprs <- paste(
      sprintf("(b.%s - gm.%s_mean) AS %s_tilde", all_vars, all_vars, all_vars),
      collapse = ",\n       "
    )
    
    moment_terms <- c(
      sql_count(inputs$conn, "n_total"),
      sql_count(inputs$conn, "n_fe1", fe1, distinct = TRUE),
      "1 AS n_fe2",
      sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_y_sq", inputs$yvar, inputs$yvar)
    )
    for (x in inputs$xvars) {
      moment_terms <- c(moment_terms,
                        sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", x, inputs$yvar, x, inputs$yvar),
                        sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", x, x, x, x))
    }
    if (length(inputs$xvars) > 1) {
      for (i in seq_along(inputs$xvars)) {
        if (i == 1) next
        for (j in seq_len(i - 1)) {
          xi <- inputs$xvars[i]; xj <- inputs$xvars[j]
          moment_terms <- c(moment_terms,
                            sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", xi, xj, xi, xj))
        }
      }
    }

    mundlak_sql <- paste0(
      "WITH base AS (
      SELECT * ", inputs$from_statement, "
      ),
      group_means AS (
      SELECT ", fe1, ", ", means_cols, " FROM base GROUP BY ", fe1, "
      ),
      demeaned AS (
      SELECT
          b.", fe1, ",
          ", tilde_exprs, "
      FROM base b
      JOIN group_means gm ON b.", fe1, " = gm.", fe1, "
      ),
      moments AS (
      SELECT
          ", paste(moment_terms, collapse = ",\n    "), "
      FROM demeaned
      )
      SELECT * FROM moments"
    )
    
  } else {
    # Two FE: double demeaning
    fe1 <- inputs$fes[1]; fe2 <- inputs$fes[2]
    all_vars <- c(inputs$yvar, inputs$xvars)

    unit_means_cols <- paste(sprintf("AVG(%s) AS %s_u", all_vars, all_vars), collapse = ", ")
    time_means_cols <- paste(sprintf("AVG(%s) AS %s_t", all_vars, all_vars), collapse = ", ")
    overall_cols    <- paste(sprintf("AVG(%s) AS %s_o", all_vars, all_vars), collapse = ", ")
    tilde_exprs <- paste(
      sprintf("(b.%s - um.%s_u - tm.%s_t + o.%s_o) AS %s_tilde", all_vars, all_vars, all_vars, all_vars, all_vars),
      collapse = ",\n       "
    )

    moment_terms <- c(
      sql_count(inputs$conn, "n_total"),
      sql_count(inputs$conn, "n_fe1", fe1, distinct = TRUE),
      sql_count(inputs$conn, "n_fe2", fe2, distinct = TRUE),
      sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_y_sq", inputs$yvar, inputs$yvar)
    )
    for (x in inputs$xvars) {
      moment_terms <- c(moment_terms,
                        sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", x, inputs$yvar, x, inputs$yvar),
                        sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", x, x, x, x))
    }
    if (length(inputs$xvars) > 1) {
      for (i in seq_along(inputs$xvars)) {
        if (i == 1) next
        for (j in seq_len(i - 1)) {
          xi <- inputs$xvars[i]; xj <- inputs$xvars[j]
          moment_terms <- c(moment_terms,
                            sprintf("SUM(CAST(%s_tilde AS FLOAT) * CAST(%s_tilde AS FLOAT)) AS sum_%s_%s", xi, xj, xi, xj))
        }
      }
    }

    mundlak_sql <- paste0(
      "WITH base AS (
      SELECT * ", inputs$from_statement, "
      ),
      unit_means AS (
      SELECT ", fe1, ", ", unit_means_cols, " FROM base GROUP BY ", fe1, "
      ),
      time_means AS (
      SELECT ", fe2, ", ", time_means_cols, " FROM base GROUP BY ", fe2, "
      ),
      overall AS (
      SELECT ", overall_cols, " FROM base
      ),
      demeaned AS (
      SELECT
          b.", fe1, ",
          b.", fe2, ",
          ", tilde_exprs, "
      FROM base b
      JOIN unit_means um ON b.", fe1, " = um.", fe1, "
      JOIN time_means tm ON b.", fe2, " = tm.", fe2, "
      CROSS JOIN overall o
      ),
      moments AS (
      SELECT
          ", paste(moment_terms, collapse = ",\n    "), "
      FROM demeaned
      )
      SELECT * FROM moments"
    )
  }
  
  # Execute SQL and build matrices
  if (inputs$verbose) message("[duckreg] Executing mundlak SQL")
  moments_df <- dbGetQuery(inputs$conn, mundlak_sql)
  n_total <- moments_df$n_total
  n_fe1 <- moments_df$n_fe1
  n_fe2 <- moments_df$n_fe2

  vars_all <- inputs$xvars  # No intercept for FE models
  p <- length(vars_all)
  XtX <- matrix(0, p, p, dimnames = list(vars_all, vars_all))
  Xty <- matrix(0, p, 1, dimnames = list(vars_all, ""))

  for (x in inputs$xvars) {
    XtX[x, x] <- moments_df[[sprintf("sum_%s_%s", x, x)]]
    Xty[x, ]  <- moments_df[[sprintf("sum_%s_%s", x, inputs$yvar)]]
  }
  if (length(inputs$xvars) > 1) {
    for (i in seq_along(inputs$xvars)) {
      if (i == 1) next
      for (j in seq_len(i - 1)) {
        xi <- inputs$xvars[i]; xj <- inputs$xvars[j]
        XtX[xi, xj] <- XtX[xj, xi] <- moments_df[[sprintf("sum_%s_%s", xi, xj)]]
      }
    }
  }

  Rch <- tryCatch(chol(XtX), error = function(e) {
    ridge <- inputs$ridge_rel * mean(diag(XtX))
    chol(XtX + diag(ridge, p))
  })
  betahat <- backsolve(Rch, forwardsolve(Matrix::t(Rch), Xty))
  rownames(betahat) <- vars_all

  rss <- as.numeric(moments_df$sum_y_sq - 2 * t(betahat) %*% Xty + t(betahat) %*% XtX %*% betahat)
  df_fe <- n_fe1 + n_fe2 - 1
  df_res <- max(n_total - p - df_fe, 1)
  sigma2 <- rss / df_res
  XtX_inv <- chol2inv(Rch)
  vcov_mat <- sigma2 * XtX_inv
  
  if (inputs$vcov_type_req == "hc1") {
    vcov_mat <- vcov_mat * (n_total / df_res)
    attr(vcov_mat, "type") <- "hc1"
  } else {
    attr(vcov_mat, "type") <- "ols"
  }

  coefs <- as.numeric(betahat); names(coefs) <- vars_all
  ses <- sqrt(Matrix::diag(vcov_mat))
  tstats <- coefs / ses
  pvals <- 2 * pt(-abs(tstats), df_res)
  coeftable <- cbind(estimate = coefs, std.error = ses, statistic = tstats, p.values = pvals)

  list(
    coeftable = coeftable,
    vcov = vcov_mat,
    fml = inputs$fml,
    yvar = inputs$yvar,
    xvars = inputs$xvars,
    fes = inputs$fes,
    query_string = mundlak_sql,
    nobs = 1L,
    nobs_orig = n_total,
    strategy = "mundlak",
    compression_ratio_est = inputs$compression_ratio_est,
    df_residual = df_res,
    n_fe1 = n_fe1,
    n_fe2 = n_fe2
  )
}

#' Execute compress strategy (groupby compression)
#' @keywords internal
execute_compress_strategy <- function(inputs) {
  group_cols <- c(inputs$xvars, inputs$fes)
  group_cols_sql <- paste(group_cols, collapse = ", ")
  query_string <- paste0(
    "WITH cte AS (
    SELECT
        ", group_cols_sql, ",
        COUNT(*) AS n,
        SUM(", inputs$yvar, ") AS sum_Y,
        SUM(POWER(", inputs$yvar, ", 2)) AS sum_Y_sq
    ", inputs$from_statement, "
    GROUP BY ", group_cols_sql, "
    )
    SELECT
    *,
    sum_Y / n AS mean_Y,
    sqrt(n) AS wts
    FROM cte"
    )
    
  if (inputs$query_only) return(query_string)

  if (inputs$verbose) message("[duckreg] Executing compress strategy SQL")
  compressed_dat <- dbGetQuery(inputs$conn, query_string)
  nobs_orig <- sum(compressed_dat$n)
  nobs_comp <- nrow(compressed_dat)
  compression_ratio <- nobs_comp / max(nobs_orig, 1)
  
  if (inputs$verbose && compression_ratio > 0.8) {
    message(sprintf("[duckreg] Warning: compression ineffective (%.1f%% of original rows).",
                    100 * compression_ratio))
  }

  if (length(inputs$fes)) for (f in inputs$fes) compressed_dat[[f]] <- factor(compressed_dat[[f]])
  if (inputs$data_only) return(compressed_dat)

  X <- sparse.model.matrix(reformulate(c(inputs$xvars, inputs$fes)), compressed_dat)
  if (ncol(X) == 0) stop("Design matrix has zero columns.")
  Y <- compressed_dat[,"mean_Y"]
  wts <- compressed_dat[["wts"]]
  Xw <- X * wts
  Yw <- Y * wts
  XtX <- crossprod(Xw)
  XtY <- crossprod(Xw, Yw)

  Rch <- tryCatch(chol(XtX), error = function(e) {
    ridge <- inputs$ridge_rel * mean(diag(XtX))
    chol(XtX + diag(ridge, ncol(XtX)))
  })
  betahat <- backsolve(Rch, forwardsolve(Matrix::t(Rch), XtY))
  if (is.null(dim(betahat))) betahat <- matrix(betahat, ncol = 1)
  rownames(betahat) <- colnames(X)
  yhat <- as.numeric(X %*% betahat)

  if (inputs$vcov_type_req == "hc1") {
    n_vec <- compressed_dat$n
    sum_Y <- compressed_dat$sum_Y
    sum_Y_sq <- compressed_dat$sum_Y_sq
    rss_g <- sum_Y_sq - 2 * yhat * sum_Y + n_vec * (yhat^2)
    XtX_inv <- chol2inv(Rch)
    meat <- crossprod(X, Diagonal(x = as.numeric(rss_g)) %*% X)
    df_res <- max(nobs_orig - ncol(X), 1)
    scale_hc1 <- nobs_orig / df_res
    vcov_mat <- scale_hc1 * (XtX_inv %*% meat %*% XtX_inv)
    attr(vcov_mat, "type") <- "hc1"
  } else {
    sum_Y <- compressed_dat$sum_Y
    sum_Y_sq <- compressed_dat$sum_Y_sq
    rss_g <- sum_Y_sq - 2 * yhat * sum_Y + compressed_dat$n * (yhat^2)
    rss_total <- sum(rss_g)
    df_res <- max(nobs_orig - ncol(X), 1)
    sigma2 <- rss_total / df_res
    XtX_inv <- chol2inv(Rch)
    vcov_mat <- sigma2 * XtX_inv
    attr(vcov_mat, "type") <- "ols"
  }

  coefs <- as.numeric(betahat); names(coefs) <- rownames(betahat)
  ses <- sqrt(Matrix::diag(vcov_mat))
  tstats <- coefs / ses
  pvals <- 2 * pt(-abs(tstats), max(nobs_orig - length(coefs), 1))
  coeftable <- cbind(
    estimate = coefs,
    std.error = ses,
    statistic = tstats,
    p.values = pvals
  )

  list(
    coeftable = coeftable,
    vcov = vcov_mat,
    fml = inputs$fml,
    yvar = inputs$yvar,
    xvars = inputs$xvars,
    fes = inputs$fes,
    query_string = query_string,
    nobs = nobs_comp,
    nobs_orig = nobs_orig,
    strategy = "compress",
    compression_ratio = compression_ratio,
    compression_ratio_est = inputs$compression_ratio_est,
    df_residual = max(nobs_orig - length(coefs), 1)
  )
}

#' Finalize duckreg result object
#' @keywords internal
finalize_duckreg_result <- function(result, inputs, chosen_strategy) {
  result$strategy <- chosen_strategy
  class(result) <- c("duckreg", class(result))
  result
}