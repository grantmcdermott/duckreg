# This file compares the performance of the new Mundlak/sufficient statistics
# approach to the former pure-group-by approach.

# ------------------------------------------
# Simulation functions
# ------------------------------------------
library(dplyr)
library(tidyr)
library(ggplot2)
library(fixest) # to compare to feols

# True regression coefficients for the simulation
beta_true <- c(x1 = 0.5, x2 = -0.3)

# Simulation of discrete regressors
# Produces integer-valued x1, x2 (0..K1-1, 0..K2-1) so duckreg without FE can compress.
generate_panel_discrete <- function(N_units, T_periods,
                                    beta = beta_true,
                                    K1 = 8L, K2 = 6L,
                                    sd_fe_unit = 1.0, sd_fe_time = 0.5,
                                    sd_err = 0.8) {
  unit_fe <- rnorm(N_units, sd = sd_fe_unit)
  time_fe <- rnorm(T_periods, sd = sd_fe_time)
  base_df <- crossing(unit = 1:N_units, time = 1:T_periods)
  n_tot <- nrow(base_df)
  # Latent continuous draws correlated with FEs
  latent_x1 <- 0.6 * unit_fe[base_df$unit] + 0.3 * time_fe[base_df$time] + rnorm(n_tot, sd = 0.7)
  latent_x2 <- -0.4 * unit_fe[base_df$unit] + 0.5 * time_fe[base_df$time] + rnorm(n_tot, sd = 0.7)
  # Bin into discrete categories (roughly equal-frequency)
  cuts1 <- quantile(latent_x1, probs = seq(0, 1, length.out = K1 + 1))
  cuts2 <- quantile(latent_x2, probs = seq(0, 1, length.out = K2 + 1))
  # Make unique breakpoints (jitter if ties)
  cuts1 <- unique(cuts1); while(length(cuts1) < K1 + 1) cuts1 <- sort(unique(c(cuts1, jitter(cuts1[length(cuts1)],1e-6))))
  cuts2 <- unique(cuts2); while(length(cuts2) < K2 + 1) cuts2 <- sort(unique(c(cuts2, jitter(cuts2[length(cuts2)],1e-6))))
  x1_disc <- as.integer(cut(latent_x1, breaks = cuts1, include.lowest = TRUE, labels = FALSE)) - 1L
  x2_disc <- as.integer(cut(latent_x2, breaks = cuts2, include.lowest = TRUE, labels = FALSE)) - 1L
  # Outcome with linear effect of discrete codes
  y <- unit_fe[base_df$unit] + time_fe[base_df$time] +
       beta["x1"] * x1_disc + beta["x2"] * x2_disc + rnorm(n_tot, sd = sd_err)
  mutate(base_df,
         x1 = x1_disc,
         x2 = x2_disc,
         y = y)
}

# Generate more compressible data by repeating the base data
generate_panel_discrete_compressible <- function(N_units, T_periods,
                                                 beta = beta_true,
                                                 K1 = 8L,
                                                 K2 = 6L,
                                                 rep = 10L,
                                                 sd_fe_unit = 1.0, 
                                                 sd_fe_time = 0.5,
                                                 sd_err = 0.8,
                                                 sd_dup_noise = 1e-4) {
  # Generate the original non-compressed data
  base <- generate_panel_discrete(
    N_units = N_units,
    T_periods = T_periods,
    beta = beta,
    K1 = K1,
    K2 = K2,
    sd_fe_unit = sd_fe_unit,
    sd_fe_time = sd_fe_time,
    sd_err = sd_err
  )

  # Repeat the data 'repeat' times, stacking vertically
  expanded <- base[rep(seq_len(nrow(base)), times = rep), ]
  # Add small noise to y only, so it's not a literal duplication
  expanded$y <- expanded$y + rnorm(nrow(expanded), sd = sd_dup_noise)

  expanded
}

# ------------------------------------------
# Benchmarking code
# ------------------------------------------
# Grids
N_units_grid <- c(1e3, 1e4, 1e5, 1e6, 3e6) # Number of units
T_periods_grid <- c(10L)
rep_grid <- c(100L) # Number of repetitions for more compressible data
K1 <- 4L # Number of discrete categories for x1
K2 <- 4L # Number of discrete categories for x2

results <- list()
counter <- 0
total <- length(N_units_grid) * length(T_periods_grid) * length(rep_grid)

# number of iterations to average over
n_iters <- 3

set.seed(123)  # For reproducibility
for (iter in 1:n_iters) {
    cat(sprintf("Iteration %d/%d\n", iter, n_iters))
    # Reset counter for each iteration
    counter <- 0
    for (N_units in N_units_grid) {
        for (T_periods in T_periods_grid) {
            for (rep in rep_grid) {
                counter <- counter + 1
                cat(sprintf("[Run %d/%d] N_units=%d, T_periods=%d, rep=%d\n", 
                                        counter, total, N_units, T_periods, rep))
                # Compressible data
                dfc <- generate_panel_discrete_compressible(
                    as.integer(N_units / rep), T_periods, 
                    K1 = K1, K2 = K2, rep = rep
                )
                # Non-compressible data
                df <- generate_panel_discrete(
                    N_units, T_periods, K1 = K1, K2 = K2
                )
                # Timings and fits
                t_new_comp <- system.time(
                    fit_new_comp <- duckreg(y ~ x1 + x2 | unit_fe + time_fe,
                        data = dfc %>% dplyr::rename(unit_fe = unit, time_fe = time), 
                        strategy = "moments_fe")
                )["elapsed"]
                t_new_noncomp <- system.time(
                    fit_new_noncomp <- duckreg(y ~ x1 + x2 | unit_fe + time_fe,
                        data = df %>% dplyr::rename(unit_fe = unit, time_fe = time), 
                        strategy = "moments_fe")
                )["elapsed"]

                # only run "group" if not too many rows 
                if (nrow(dfc) < 3e6 && nrow(df) < 3e6) {                  
                    t_orig_comp <- system.time(
                        fit_orig_comp <- duckreg(y ~ x1 + x2 | unit_fe + time_fe,
                            data = dfc %>% dplyr::rename(unit_fe = unit, time_fe = time), 
                            strategy = "group")
                    )["elapsed"]
                } else {
                    t_orig_comp <- NA
                    fit_orig_comp <- NULL
                }
                # even lower threshold for non-compressible data
                if (nrow(dfc) < 3e5 && nrow(df) < 3e5) {                  
                    t_orig_noncomp <- system.time(
                        fit_orig_noncomp <- duckreg(y ~ x1 + x2 | unit_fe + time_fe,
                            data = df %>% dplyr::rename(unit_fe = unit, time_fe = time), 
                            strategy = "group")
                    )["elapsed"]
                } else {
                    t_orig_noncomp <- NA
                    fit_orig_noncomp <- NULL
                }

                # compare to feols 
                t_feols_comp <- system.time(
                    fit_feols_comp <- feols(y ~ x1 + x2 | unit_fe + time_fe,
                        data = dfc %>% dplyr::rename(unit_fe = unit, time_fe = time))
                )["elapsed"]
                t_feols_noncomp <- system.time(
                    fit_feols_noncomp <- feols(y ~ x1 + x2 | unit_fe + time_fe,
                        data = df %>% dplyr::rename(unit_fe = unit, time_fe = time))
                )["elapsed"]    

                # Save results
                results[[length(results) + 1]] <- tibble::tibble(
                    N_units = N_units,
                    T_periods = T_periods,
                    rep = rep,
                    nrow_compressible = nrow(dfc),
                    nrow_noncompressible = nrow(df),
                    elapsed_orig_compressible = t_orig_comp,
                    elapsed_new_compressible = t_new_comp,
                    elapsed_orig_noncompressible = t_orig_noncomp,
                    elapsed_new_noncompressible = t_new_noncomp,
                    elapsed_feols_compressible = t_feols_comp,
                    elapsed_feols_noncompressible = t_feols_noncomp,
                    x1_feols_compressible = fit_feols_comp$coeftable["x1", "estimate"],
                    x2_feols_compressible = fit_feols_comp$coeftable["x2", "estimate"],
                    x1_feols_noncompressible = fit_feols_noncomp$coeftable["x1", "estimate"],
                    x2_feols_noncompressible = fit_feols_noncomp$coeftable["x2", "estimate"],
                    x1_orig_compressible = fit_orig_comp$coeftable["x1", "estimate"],
                    x2_orig_compressible = fit_orig_comp$coeftable["x2", "estimate"],
                    x1_new_compressible = fit_new_comp$coeftable["x1", "estimate"],
                    x2_new_compressible = fit_new_comp$coeftable["x2", "estimate"],
                    x1_orig_noncompressible = fit_orig_noncomp$coeftable["x1", "estimate"],
                    x2_orig_noncompressible = fit_orig_noncomp$coeftable["x2", "estimate"],
                    x1_new_noncompressible = fit_new_noncomp$coeftable["x1", "estimate"],
                    x2_new_noncompressible = fit_new_noncomp$coeftable["x2", "estimate"]
                )
                rm(dfc, df, fit_orig_comp, fit_new_comp, fit_orig_noncomp, fit_new_noncomp)
                gc(verbose = FALSE)
                message(sprintf("Completed %d/%d runs", counter, total))
            }
        }
    }
}

gc()
benchmark_results <- dplyr::bind_rows(results)

# make table of times to compare
benchmark_summary <- benchmark_results %>%
    tidyr::pivot_longer(
        cols = c(elapsed_orig_compressible, elapsed_new_compressible,
                 elapsed_orig_noncompressible, elapsed_new_noncompressible,
                 elapsed_feols_compressible, elapsed_feols_noncompressible),
        names_to = c("method", "type"),
        names_pattern = "elapsed_(orig|new|feols)_(compressible|noncompressible)",
        values_to = "elapsed"
    ) %>%
    mutate(
        method = recode(method, orig = "Original", new = "New", feols = "FEOLS"),
        type = recode(type, compressible = "Compressible", noncompressible = "Non-compressible")
    ) %>%
    group_by(N_units, T_periods, rep, method, type) %>%
    summarise(
        avg_time = mean(elapsed),
        .groups = "drop"
    ) %>%
    mutate(
        nrows = N_units * T_periods
    ) %>%
    arrange(N_units, T_periods, rep, method, type)

# -------------------------------------------
# Plotting 
# ------------------------------------------- 
# Plot timing results 
benchmark_summary %>%
    mutate(
        avg_time_plot = ifelse(is.na(avg_time), 0, avg_time),
        label = ifelse(is.na(avg_time), "N/A", sprintf("%.2f", avg_time))
    ) %>%
    ggplot(aes(x = factor(N_units * T_periods),
                         y = avg_time_plot, fill = method)) +
    geom_bar(stat = "identity", position = "dodge") +
    geom_text(aes(label = label), 
                        position = position_dodge(width = 0.9), 
                        vjust = -0.5, size = 3) +
    facet_wrap(~ type, ncol = 1, scale = "free_y") +
    labs(
        title = "Benchmark Results: Original vs New Algorithm",
        x = "N_units * T_periods",
        y = "Average Time (seconds)"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_x_discrete(labels = function(x) format(as.numeric(x), big.mark = ","))

# Scatter to compare the point estimates across methods
benchmark_results %>%
    # divide point estimates by the true value
    mutate(
        x1_orig_compressible = x1_orig_compressible / beta_true["x1"],
        x1_new_compressible = x1_new_compressible / beta_true["x1"],
        x2_orig_compressible = x2_orig_compressible / beta_true["x2"],
        x2_new_compressible = x2_new_compressible / beta_true["x2"],
        x1_orig_noncompressible = x1_orig_noncompressible / beta_true["x1"],
        x1_new_noncompressible = x1_new_noncompressible / beta_true["x1"],
        x2_orig_noncompressible = x2_orig_noncompressible / beta_true["x2"],
        x2_new_noncompressible = x2_new_noncompressible / beta_true["x2"]
    ) %>%
    ggplot() +
    geom_point(aes(x = x1_orig_compressible, y = x1_new_compressible)) +
    geom_point(aes(x = x1_orig_noncompressible, y = x1_new_noncompressible), color = "blue") + 
    geom_point(aes(x = x2_orig_compressible, y = x2_new_compressible), shape = 1) +
    geom_point(aes(x = x2_orig_noncompressible, y = x2_new_noncompressible), shape = 1, color = "blue") +
    geom_abline(slope = 1, intercept = 0, color = "red") +
    labs(
        title = "Comparison of x1 Estimates: Original vs New (Compressible)",
        x = "Original x1 Estimate",
        y = "New x1 Estimate"
    ) +
    theme_minimal()

