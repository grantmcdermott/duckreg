#' Print method for duckreg objects
#' @param x `duckreg` object.
#' @param fes Should the fixed effects be displayed? Default is `FALSE`.
#' @export
print.duckreg = function(x, fes = FALSE, ...) {
    ct = x[["coeftable"]]
    colnames(ct) = c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
    if (!isTRUE(fes)) {
        xvars = x[["xvars"]]
        ct = ct[xvars, , drop = FALSE]
    }
    cat("Compressed OLS estimation, Dep. Var.:", x$yvar, "\n")
    cat("Observations.:", prettyNum(x$nobs_orig, big.mark = ","), "(original) |", prettyNum(x$nobs, big.mark = ","), "(compressed)", "\n")
    print_coeftable(ct)
    invisible(ct)
}

# stolen from Laurent here:
# https://github.com/lrberge/fixest/blob/5523d48ef4a430fa2e82815ca589fc8a47168fe7/R/miscfuns.R#L3758
print_coeftable = function(coeftable, lastLine = "", show_signif = TRUE){
  # Simple function that does as the function coeftable but handles special cases
  # => to take care of the case when the coefficient is bounded

  if(!is.data.frame(coeftable)){
    class(coeftable) = NULL
    ct = as.data.frame(coeftable)
  } else {
    ct = coeftable
  }

  signifCode = c("***"=0.001, "** "=0.01, "*  "=0.05, ".  "=0.1)

  pvalues = ct[, 4]

  stars = cut(pvalues, breaks = c(-1, signifCode, 100), labels = c(names(signifCode), ""))
  stars[is.na(stars)] = ""

  whoIsLow = !is.na(pvalues) & pvalues < 2.2e-16

  # Note that it's a bit different than format => I don't like xxe-yy numbers, very hard to read: you can't see large/small nbers at first sight
  for(i in 1:3){
    ct[, i] = decimalFormat(ct[, i])
  }

  ct[!whoIsLow, 4] = format(ct[!whoIsLow, 4], digits = 5)

  ct[whoIsLow, 4] = "< 2.2e-16"
  ct[is.na(ct[, 4]), 4] = "NA"

  ct[, 5] = stars
  names(ct)[5] = ""

  print(ct)
  
  cat(lastLine)

  if(show_signif){
    cat("---\nSignif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1\n")
  }
}

decimalFormat = function(x){

  who_valid = which(!is.na(x) & is.numeric(x))
  if(length(who_valid) == 0) return(x)

  res = x

  x_valid = x[who_valid]
  xPower = log10(abs(x_valid))

  if(min(xPower) > 0){
    pow_round = max(1, 6 - ceiling(xPower))
  } else if(min(xPower) < -5){
    pow_round = ceiling(abs(min(xPower))) + 2
  } else {
    pow_round = 6
  }

  res[who_valid] = round(x_valid, pow_round)

  res
}
