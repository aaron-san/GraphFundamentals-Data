
suppressWarnings({
# suppressPackageStartupMessages({
    library(arrow)
    library(tidyverse)
    library(lubridate)
    library(xts)
    library(data.table)
    library(quantmod)
    # library(RcppRoll) # roll_mean, roll_sd, roll_max, roll_min, roll_var, roll_sum, roll_prod, roll_median
    library(slider) # slide_dbl()
})

dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/"



read_tibble <- function(x, date_format = "%Y-%m-%d", ...) {
    
  ####
  # x <- input_data_profiles_file
  # x <- input_data_file
  # x <- files_control[1]
  # date_format <- "%m/%d/%Y"
  # x <- file_yhoo_profiles[1]
  # date_format <- "%Y_%m_%d"
  # date_format <- "%Y-%m-%d"
      
  # x <-
  #   list.files("C:/Users/user/Desktop/Aaron/R/Projects/Yahoo-Data/data/cleaned",
  #              pattern = "yhoo_profiles", full.names = TRUE) %>% max()
  ####
  
  x %>%
    fread(fill = TRUE, integer64 = "double", data.table = FALSE) %>%
    as_tibble() %>% 
    # Some objects have multiple date classes, so coerce it to "date"    
    mutate(across(where(is.Date), ~as.Date(.x))) %>% 
    mutate(across(which(sapply(., class) == "integer64"), as.numeric)) %>% 
    mutate(across(where(is.integer), ~as.double(.x))) %>% 
    mutate(across(contains("date"), ~as.Date(.x, date_format))) %>%
    # Remove empty columns
    janitor::remove_empty(which = "cols")
}



# Round report date to nearest month-end
get_rounded_date <- . %>% 
    # as_tibble() %>% 
    mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>%
    select(-report_date) %>% 
    select(ticker, rounded_date, everything())
    
# get_rounded_date(tibble(ticker = "MSFT", report_date = ymd("2021-12-29")))


# Some tickers have multiple instances of a date but unequal values, so
#  remove NA values for all tickers and then remove any duplicate 
#  instances of a given ticker and date, keeping the most recent value

# Choose the most recent value where two values or more exist for a given rounded_date
#  - Arrange descending so the most recent value for two nearby dates appears
#  - first and then round them both to the nearest month end, then drop the second (more dated value)

remove_duplicates <- function(x) {
  #####
  # x <- total_liabilities
  #####
  
  x %>% 
  as.data.table() %>% 
  setorder(report_date) %>%
  # Remove rows that have all NAs
  filter(rowSums(is.na(select(., -c(ticker, report_date)))) != 
           ncol(select(., -c(ticker, report_date)))) %>%
  setorder(ticker, -report_date) %>% 
  add_column(n_vals = rowSums(!is.na(select(., -c(ticker, report_date))))) %>%
  get_rounded_date() %>%
  setorder(rounded_date, -n_vals) %>%
  # Remove duplicate instances of a date for a given ticker
  unique(by = c("ticker", "rounded_date")) %>%   
  select(-n_vals) %>% 
  setorder(ticker, rounded_date) %>% 
  as_tibble() %>% 
  filter(!is.na(ticker))
}


# Fill NAs up to two periods (months)
fill_na_two_periods_max <- function(x) {
    #--------#
    # x <- fundamentals_full_dates %>%
    #     filter(ticker == "AAL") %>% pull(research_development)
    #--------#
    non_NA <- which(!is.na(x))
    # If there are no NAs, return x
    if(length(non_NA) == 0) return(x)
    
    diffs <- diff(non_NA)
    for(i in seq_along(diffs)) {
        # i <- 1
        if(diffs[i] <= 3) {
            x[non_NA[i] + seq_len(diffs[i] - 1)] <- x[non_NA[i]]
        } else if(diffs[i] > 3) {
            x[non_NA[i] + 1:2] <- x[non_NA[i]]
        }
    }
    if(last(non_NA) < length(x)) {
        x[last(non_NA) + 1:(min(2, length(x) - last(non_NA)))] <- x[last(non_NA)]
    }
    return(x)
}


read_and_clean <- function(file) {
    ######
    # file <- files_prices[1]
    ######
    
    read_tibble(file) %>%
    select(ticker, date = ref.date, close = price.close, 
           adjusted = price.adjusted) %>% 
    group_by(ticker) %>% 
    fill(close, adjusted, .direction = "down") %>% 
    slice(endpoints(date, on = "months")) %>% 
    as.data.table() %>% 
    setorder(ticker, date) %>%
    as_tibble()
}




# Given tickers and fields, find a complete series of data, if it exists
# Reduce range of tickers and dates to get a complete series for a given field
get_complete_series <- function(data, fields, years = 5) {
    
  #######
  # data <- data_partially_complete
  # years <- 8
  # fields <- fields_to_use
  #######
    
  # Find tickers that have at least the required years
  tickers_to_keep <-
    data %>%
    count(ticker) %>% 
    filter(n >= years*4) %>% 
    pull(ticker)
  
  # Choose the columns specified
  data_subset <-
    data %>% 
    select(ticker, report_date, any_of(fields)) %>% 
    filter(ticker %in% tickers_to_keep)
  
    
  # Search through all possible time spans of length "years" and
  #  find the date range with the fewest NAs
  date_range_w_least_nas <-
    data_subset %>%
    select(-ticker) %>% 
    group_by(report_date) %>% 
    nest() %>% 
    mutate(report_date, data_points = 
             map_dbl(data, ~ncol(.x)*nrow(.x))) %>% 
    mutate(na_perc = sum(is.na(unlist(data))) / data_points) %>% 
    ungroup() %>% 
    as.data.table() %>% setorder(report_date) %>% as_tibble() %>% 
    mutate(p_na_trailing_n_yrs = 
             slider::slide_dbl(na_perc, sum, .before = 4*years-1,
                               .complete = TRUE)) %>% 
    slice_min(p_na_trailing_n_yrs) %>% 
    tail(1) %>% 
    {tibble(min_date = .$report_date - lubridate::years(years), 
            max_date = .$report_date)}
  
  # Get most complete date range
  data_cl_dates <-
      data_subset %>%
      filter(between(report_date, 
                     date_range_w_least_nas$min_date, 
                     date_range_w_least_nas$max_date))
  
  # Get tickers with no NAs in most complete date range
  tickers_to_use <-
    data_cl_dates %>%
    select(-report_date) %>% 
    group_by(ticker) %>% 
    nest() %>% 
    transmute(ticker,
              na_count = sum(is.na(unlist(data)))) %>% 
    filter(na_count == 0) %>% 
    pull(ticker)
  
  if(length(tickers_to_use) == 0) 
      warning("Not enough data available for chosen inputs.")
  
  
  # Clean the sector_yhoo variable by setting sector_yhoo equal to the
  #  first observed value for each ticker
  # data_cl_sector <-
  #   # Use "data_subset" if you want to include data that might exist outside the 
  #   # date range
  #   # data_subset %>% 
  #   data_cl_dates %>%
  #   filter(ticker %in% tickers_to_use) %>%
  #   # Set the sector of the ticker to the first observed sector value
  #   # (ticker MGPI had two unique sectors)
  #   group_by(ticker) %>% 
  #   mutate(sector_yhoo = first(sector_yhoo)) %>% 
  #   ungroup() %>% 
  #   as.data.table() %>% 
  #   unique() %>% 
  #   setorder(ticker, report_date) %>% 
  #   as_tibble()
  
  # If there are any NAs remaining, raise a warning.
  if(data_cl_dates %>% is.na() %>% any()) 
      warning("There are some NAs in the data.")
  
  date_diffs <-
    data_cl_dates %>% 
    # as.data.table() %>% 
    # setorder(report_date) %>% 
    # as_tibble() %>% 
    select(ticker, report_date) %>% 
    group_by(ticker) %>%
    mutate(date_diff = as.numeric(report_date - lag(report_date))) %>% 
    # Replace each initial NA with a safe number, 30, 
    #  so that the row isn't dropped later
    mutate(date_diff = replace(date_diff, row_number() == 1, 90))
  
  
  # If any of the date differences are not between 27 and 32, then stop
  if(!date_diffs %>% pull(date_diff) %>% between(., 80, 100) %>% all())
      warning("Some date sequences are incomplete.")
  
  return(data_cl_dates)
}




get_recent_price_dirs <- function(pattern = "prices",
                                  period = "daily", 
                                  dir = "data/cleaned data",
                                  group_pattern = "[0-9]{1,9}_[0-9]{1,9}") {
  #####
  # pattern <- "prices"
  # freq <- "daily"
  # group_pattern <- "[0-9]{1,9}_[0-9]{1,9}"
  # dir <- "data/cleaned data"
  #####
  files_prices <- 
    list.files(dir, 
               pattern = paste0("^", pattern, ".*_", period, "_", group_pattern),
               full.names = TRUE)
  
  file_dates <- files_prices %>%
    str_extract_all("[0-9]{4} [0-9]{2} [0-9]{2}") %>% flatten_chr()
  
  max_date <- file_dates %>% max()
  prior_date <- file_dates[file_dates < max_date] %>% max()
  files_max_date <- files_prices %>% str_subset(max_date)
  files_prior_date <- files_prices %>% str_subset(prior_date)
  groups_in_max_date <-
    files_prices %>% str_subset(max_date) %>% 
    str_extract(group_pattern)
  groups_in_prior_date <- files_prior_date %>% str_extract(group_pattern)
  
  files_prior_date_to_use <- files_prior_date %>% 
    .[!groups_in_prior_date %in% groups_in_max_date]
  
  files_prices <-
    files_max_date %>% 
    c(files_prior_date_to_use) %>% 
    sort() %>% unique()
  
  return(files_prices)
}




append_derived_values <- function(x) {
  #####
  # x <- combined_fundamentals_merged
  #####
  x %>%
  group_by(ticker) %>%
  mutate(
      
      
    #   x %>% 
    #     select(ticker, 
    #            short_long_term_debt, 
    #            total_current_liabilities, 
    #            total_liabilities, 
    #            accounts_payable) %>% 
    #   group_by(ticker) %>%     
    #   summarize(across(everything(), ~mean(is.na(.x)))) %>% 
    # pivot_longer(-ticker, values_to = "pct_miss") %>% 
    # group_by(ticker) %>% 
    # summarize(some_of_each_field = all(pct_miss < 1))
        
  
  # x %>% 
  #   filter(ticker == "AAC") %>% 
  #   select(ticker, 
  #          short_long_term_debt, 
  #          total_current_liabilities, 
  #          total_liabilities, 
  #          accounts_payable) %>% View()
  
      
      
      # !!!!!!!!!!!!!!!
      # 
      # total_assets = ifelse(is.na(total_assets), 
      #                       cash_and_short_term_investments +
      #                         net_receivables + property_plant_equipment)
      # !!!!!!!!!!!!!!!!
      
      
      
      total_stockholder_equity = total_assets - total_liabilities,
      market_cap = close * shares_basic,
      capital = property_plant_equipment + total_current_assets -
        total_current_liabilities - cash_and_short_term_investments,
      working_cap_ex_cash = total_current_assets - 
        cash_and_short_term_investments - total_current_liabilities,
      
      working_capital = total_current_assets - cash_and_short_term_investments -
        total_current_liabilities,
      free_cash_flow_1Y = net_income_1Y + depreciation_amortization_1Y - 
        c(NA, diff(working_capital)) + cash_from_investing_activities_1Y,
      excess_cash = cash_and_short_term_investments + total_current_assets -
        total_current_liabilities,
      total_debt = total_liabilities - total_current_liabilities +
          short_long_term_debt,
      enterprise_value = market_cap + total_debt - excess_cash,
      market_value_of_total_assets = total_liabilities + market_cap
    ) %>% 
    ungroup()
}

append_price_stats <- function(x) {
  x %>% 
    group_by(ticker) %>%
    mutate(
      adj_return_1M = slide_dbl(adjusted, ~log(.x[2]/.x[1]), 
                                .before = 2-1, .complete = TRUE),
      adj_return_3M = slide_dbl(adjusted, ~log(.x[4]/.x[1]), 
                                .before = 4-1, .complete = TRUE),
      adj_return_6M = slide_dbl(adjusted, ~log(.x[7]/.x[1]), .before = 7-1, 
                                .complete = TRUE),
      adj_return_1Y = slide_dbl(adjusted, ~log(.x[13]/.x[1]), .before = 12-1, 
                                .complete = TRUE),
      adj_return_3Y = slide_dbl(adjusted, ~log(.x[37]/.x[1]), .before = 3*12-1,
                                .complete = TRUE)
    ) %>% 
    ungroup()
}

append_filled_values <- function(x) {
  x %>% 
    group_by(ticker) %>% 
    mutate(
      revenue_1Y_filled = 
        fill_na_two_periods_max(revenue_1Y),
      revenue_1Q_filled = 
        fill_na_two_periods_max(revenue_1Q),
      operating_income_loss_1Y_filled = 
        fill_na_two_periods_max(operating_income_loss_1Y),
      operating_income_loss_1Q_filled = 
        fill_na_two_periods_max(operating_income_loss_1Q),
      gross_profit_1Y_filled = 
        fill_na_two_periods_max(gross_profit_1Y),
      gross_profit_1Q_filled = 
        fill_na_two_periods_max(gross_profit_1Q),
      cash_from_operating_activities_1Y_filled = 
        fill_na_two_periods_max(cash_from_operating_activities_1Y),
      cash_from_operating_activities_1Q_filled = 
        fill_na_two_periods_max(cash_from_operating_activities_1Q),
      cash_from_investing_activities_1Q_filled =
        fill_na_two_periods_max(cash_from_investing_activities_1Q),
      cash_from_financing_activities_1Q_filled = 
        fill_na_two_periods_max(cash_from_financing_activities_1Q),
      net_income_common_1Y_filled =  
        fill_na_two_periods_max(net_income_common_1Y),
      net_income_common_1Q_filled =
        fill_na_two_periods_max(net_income_common_1Q),
      net_income_1Y_filled = 
        fill_na_two_periods_max(net_income_1Y), 
      net_income_1Q_filled = 
        fill_na_two_periods_max(net_income_1Q),
      total_assets_filled =
        fill_na_two_periods_max(total_assets),
      total_liabilities_filled = 
        fill_na_two_periods_max(total_liabilities),
      total_stockholder_equity_filled =
        fill_na_two_periods_max(total_stockholder_equity),
      capital_filled = 
        fill_na_two_periods_max(capital)
    ) %>% 
    ungroup()
}

append_cleaned_values <- function(x) {
  x %>% 
    group_by(ticker) %>% 
    mutate(
      # Estimate 4th quarter's value from yearly data and data from previous
      # 3 quarters
      revenue_1Q_est =
        slide2_dbl(revenue_1Y, 
                   revenue_1Q_filled,
                   ~ .x[10] - sum(.y[c(1, 4, 7)]),
                   .before = 9, .complete = TRUE
        ),
      operating_income_loss_1Q_est =
        slide2_dbl(operating_income_loss_1Y, 
                   operating_income_loss_1Q_filled,
                   ~ .x[10] - sum(.y[c(1, 4, 7)]),
                   .before = 9, .complete = TRUE
        ),
      gross_profit_1Q_est =
        slide2_dbl(gross_profit_1Y, 
                   gross_profit_1Q_filled,
                   ~ .x[10] - sum(.y[c(1, 4, 7)]),
                   .before = 9, .complete = TRUE
        ),
      cash_from_operating_activities_1Q_est =
        slide2_dbl(
          cash_from_operating_activities_1Y,
          cash_from_operating_activities_1Q_filled,
          ~ .x[10] - sum(.y[c(1, 4, 7)]),
          .before = 9, .complete = TRUE
        ),
      net_income_common_1Q_est =
        slide2_dbl(net_income_common_1Y, 
                   net_income_common_1Q_filled,
                   ~ .x[10] - sum(.y[c(1, 4, 7)]),
                   .before = 9, .complete = TRUE
        ),
      net_income_1Q_est =
        slide2_dbl(net_income_1Y, 
                   net_income_1Q_filled,
                   ~ .x[10] - sum(.y[c(1, 4, 7)]),
                   .before = 9, .complete = TRUE
        )
    ) %>% 
    # Consolidate columns (Choose first non-NA value from two columns)
    mutate(
      revenue_1Q = 
        coalesce(
          revenue_1Q, 
          revenue_1Q_est
        ),
      operating_income_loss_1Q = 
        coalesce(
          operating_income_loss_1Q,
          operating_income_loss_1Q_est
        ),
      gross_profit_1Q = coalesce(
        gross_profit_1Q, 
        gross_profit_1Q_est
      ),
      cash_from_operating_activities_1Q =
        coalesce(
          cash_from_operating_activities_1Q,
          cash_from_operating_activities_1Q_est
        ),
      net_income_common_1Q = coalesce(
        net_income_common_1Q,
        net_income_common_1Q_est
      ),
      net_income_1Q = 
        coalesce(
          net_income_1Q, 
          net_income_1Q_est
        )
    ) %>% 
    # Estimate yearly (1Y) values by adding previous 4 quarters of data,
    #  using back-filled (estimated) values from above
    mutate(
      revenue_1Y_est =
        slide_dbl(revenue_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      gross_profit_1Y_est =
        slide_dbl(gross_profit_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      operating_income_loss_1Y_est =
        slide_dbl(operating_income_loss_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      net_income_common_1Y_est =
        slide_dbl(net_income_common_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      net_income_1Y_est =
        slide_dbl(net_income_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      cash_from_operating_activities_1Y_est =
        slide_dbl(cash_from_operating_activities_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      cash_from_investing_activities_1Y_est =
        slide_dbl(cash_from_investing_activities_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        ),
      cash_from_financing_activities_1Y_est =
        slide_dbl(cash_from_financing_activities_1Q,
                  ~ sum(.x[c(1, 4, 7, 10)]),
                  .before = 10 - 1, .complete = TRUE
        )
    ) %>% 
    # Consolidate columns (Choose first non-NA value from two columns)
    mutate(
      revenue_1Y = 
        coalesce(
          revenue_1Y, 
          revenue_1Y_est
        ),
      
      gross_profit_1Y = 
        coalesce(
          gross_profit_1Y, 
          gross_profit_1Y_est
        ),
      operating_income_loss_1Y = 
        coalesce(
          operating_income_loss_1Y,
          operating_income_loss_1Y_est
        ),
      net_income_common_1Y = 
        coalesce(
          net_income_common_1Y,
          net_income_common_1Y_est
        ),
      net_income_1Y = 
        coalesce(
          net_income_1Y, 
          net_income_1Y_est
        ),
      cash_from_operating_activities_1Y =
        coalesce(
          cash_from_operating_activities_1Y,
          cash_from_operating_activities_1Y_est
        ),
      cash_from_investing_activities_1Y =
        coalesce(
          cash_from_investing_activities_1Y,
          cash_from_investing_activities_1Y_est
        ),
      cash_from_financing_activities_1Y =
        coalesce(
          cash_from_financing_activities_1Y,
          cash_from_financing_activities_1Y_est
        )
    ) %>%
    ungroup() %>% 
    select(!ends_with("_est"))
}

append_profitability_ratios <- function(x) {
  x %>% 
    group_by(ticker) %>% 
    mutate(
      gross_margin_1Q = ifelse(gross_profit_1Q == revenue_1Q, NA, 
                               gross_profit_1Q / revenue_1Q),
      gross_margin_1Y = ifelse(gross_profit_1Y == revenue_1Y, NA, 
                               gross_profit_1Y / revenue_1Y),
      operating_profit_margin_1Y = 
        operating_income_loss_1Y / revenue_1Y,
      net_profit_margin = net_income_common_1Y / revenue_1Y,
      roe = ifelse(total_stockholder_equity > 0, 
                   net_income_1Y / 
                     lag(total_stockholder_equity_filled, 12), NA),
      roa = net_income_common_1Y / lag(total_assets_filled, 12),
      roc = operating_income_loss_1Y / lag(capital_filled, 12),
      roc_mean_3Y = 
        slide_dbl(roc, 
                  ~prod(1 + .x[c(1, 13, 25)])^(1/(3*12))-1, 
                  .before = 3*12-12, .complete = TRUE),
      roe_mean_3Y = 
        slide_dbl(roe, 
                  ~prod(1 + .x[c(1, 13, 25)])^(1/(3*12))-1, 
                  .before = 3*12-12, .complete = TRUE),
      roa_mean_3Y = 
        slide_dbl(roa, 
                  ~prod(1 + .x[c(1, 13, 25)])^(1/(3*12))-1, 
                  .before = 3*12-12, .complete = TRUE),
      roc_mean_8Y = 
        slide_dbl(roc, 
                  ~prod(1 + .x[c(1, 13, 25, 37, 49, 61, 73, 85)])^(1/(8*12))-1,
                  .before = 8*12-12, .complete = TRUE),
      roe_mean_8Y = 
        slide_dbl(roe, 
                  ~prod(1 + .x[c(1, 13, 25, 37, 49, 61, 73, 85)])^(1/(8*12))-1,
                  .before = 8*12-12, .complete = TRUE),
      roa_mean_8Y = 
        slide_dbl(roa, 
                  ~prod(1 + .x[c(1, 13, 25, 37, 49, 61, 73, 85)])^(1/(8*12))-1,
                  .before = 8*12-12, .complete = TRUE),
      pe = market_cap / net_income_1Y_filled,
      accruals_pct_chg_1Y = 
        (net_income_1Y - cash_from_operating_activities_1Y) / 
        lag((net_income_1Y_filled - cash_from_operating_activities_1Y_filled), 12),
      gross_margin_avg_3Y = 
        slide_dbl(gross_margin_1Y, 
                  ~mean(.x[c(1, 13, 25)]), 
                  .before = 3*6-12, .complete = TRUE),
      gross_margin_sd_3Y = 
        slide_dbl(gross_margin_1Y, 
                  ~sd(.x[c(1, 13, 25)]), 
                  .before = 3*6-12, .complete = TRUE), 
      gross_margin_stability_3Y = gross_margin_avg_3Y / gross_margin_sd_3Y,
      gross_margin_avg_8Y = 
        slide_dbl(gross_margin_1Y, 
                  ~mean(.x[c(1, 13, 25, 37, 49, 61, 73, 85)]),
                  .before = 8*12-12, .complete = TRUE),
      gross_margin_sd_8Y = 
        slide_dbl(gross_margin_1Y, 
                  ~sd(.x[c(1, 13, 25, 37, 49, 61, 73, 85)]), 
                  .before = 8*12-12, .complete = TRUE),
      gross_margin_stability_8Y = gross_margin_avg_8Y / gross_margin_sd_8Y,
      operating_profit_margin_sd_3Y = 
        slide_dbl(operating_profit_margin_1Y, 
                  ~sd(.x[c(1, 13, 25)]), 
                  .before = 3*6-12, .complete = TRUE),
      gross_margin_pct_chg_1Y = 
        gross_margin_1Y / lag(gross_margin_1Y, 12) - 1,
      gross_margin_mean_growth_8Y = 
        slide_dbl(gross_margin_pct_chg_1Y, 
                  ~prod(1 + .x[c(1, 13, 25, 37, 49, 61, 73, 85)])^(1/(8*12))-1,
                  .before = 8*12-12, .complete = TRUE),
      gross_margin_index = 
        lag(gross_margin_1Y, 12) / gross_margin_1Y
    ) %>% 
    ungroup() %>% 
    select(!ends_with("_filled"))
}

append_liquidity_solvency_ratios <- function(x) {
  x %>%
    group_by(ticker) %>%
    mutate(
      cash_ratio = cash_and_short_term_investments / total_current_liabilities,
      current_ratio = total_current_assets / total_current_liabilities,
      debt_ratio = total_liabilities / total_assets,
      lt_debt_ratio = (total_liabilities - total_current_liabilities) /
        total_assets,
      debt_to_equity = total_liabilities / total_stockholder_equity,
      # interest_coverage_ratio = EBIT / net_interest_expense,
      lt_debt_ratio_pct_chg_1Y = lt_debt_ratio / lag(lt_debt_ratio, 12) - 1,
      total_liabilities_to_market_value_of_total_assets =
        total_liabilities / market_value_of_total_assets,
      cash_to_market_value_of_total_assets =
        cash_and_short_term_investments /
        market_value_of_total_assets,
      book_value_adj = total_stockholder_equity +
        .1 * (market_cap - total_stockholder_equity),
      leverage_index = debt_ratio / lag(debt_ratio, 12)
    ) %>%
    ungroup()
}

append_manipulation_metrics <- function(x) {
  x %>% 
    group_by(ticker) %>% 
    mutate(
      scaled_total_accruals = 
        ((total_current_assets - cash_and_short_term_investments) -
           (total_current_liabilities - 
              (short_long_term_debt - lag(short_long_term_debt, 12))) - 
           depreciation_amortization_1Y) / total_assets,
      scaled_net_operating_assets = ((total_assets - 
                                        cash_and_short_term_investments) - 
                                       (total_assets - short_long_term_debt -
                                          (total_liabilities -
                                             total_current_liabilities) - 
                                          (total_assets - total_liabilities))) /
        total_assets,
      total_accruals_to_total_assets = (working_cap_ex_cash - 
                                          lag(working_cap_ex_cash, 12) -
                                          depreciation_amortization_1Y) /
        total_assets,
      asset_quality_index = (total_assets - total_current_assets -
                               property_plant_equipment) / total_assets
    ) %>% 
    ungroup()
}

append_activity_ratios <- function(x) {
  ######
  # x <- ratios
  ######
  
  x %>% 
    group_by(ticker) %>% 
    mutate(
      asset_turnover = revenue_1Y / total_assets,
      working_capital_pct_chg_1Y = working_capital / 
        lag(working_capital, n = 12) - 1,
      sales_growth_index = revenue_1Y / lag(revenue_1Y, 12),
      depreciation_index = ifelse(depreciation_amortization_1Y == 0, NA, 
                                  lag(depreciation_amortization_1Y, 12) /
                                    depreciation_amortization_1Y),
      sga_index = selling_general_administrative_1Y / 
        lag(selling_general_administrative_1Y, 12),
    # select(ticker, report_date, net_receivables, revenue_1Y) %>% View(),
      # Assumes all sales are credit sales
      days_sales_outstanding =
        (net_receivables + lag(net_receivables, 12)) / 2 / revenue_1Y * 365,
      days_sales_outstanding_index = days_sales_outstanding / 
        lag(days_sales_outstanding, 12),
      # Assumes all COGS are short-term accruals, not immediate cash costs
      days_payables_outstanding = 
        (accounts_payable / lag(accounts_payable, 12)) /2 / 
        (revenue_1Y - gross_profit_1Y) * 365
    ) %>% 
    ungroup()
}

# Trim leading and laggind NAs
trim_nas <- function(df, x) { 
  # x <- c(NA, NA, 23, 43, NA, 2, NA, NA, NA)
  df %>%
    filter(
      # Trim trailing NAs
      cumsum(!is.na({{ x }})) != 0 & 
        # Trim leading NAs
        rev(cumsum(!is.na(rev({{ x }})))) != 0
    )
}



has_complete_dates <- function(x, lower_bound_days = 20, upper_bound_days = 40) {
  has_complete_series <-
    x %>% 
    group_by(ticker) %>% 
    mutate(diff_date = as.numeric(report_date - lag(report_date))) %>% 
    select(ticker, report_date, diff_date) %>% 
    drop_na(diff_date) %>% 
    summarize(complete_dates = all(diff_date > lower_bound_days & 
                                     diff_date < upper_bound_days)) %>% 
    ungroup() %>% 
    pull(complete_dates)
  
  if(any(has_complete_series == FALSE)) {
    stop("Some tickers don't have a complete monthly date series")
  } else {
    "All tickers have complete dates!"
  }
}



delete_old_files <- function(pattern, dir, nth_to_keep = 4) {
  ######
  # pattern <- "profile_data_consolidated"
  # dir <- "data/cleaned data"
  # dir <- "data"
  ###########
  # Keep every 'step'th file
  ###########
  # step <- 4
  ######
  
  files_raw_v1 <- 
    list.files(dir, pattern = paste0({{ pattern }}, " \\d{4}_\\d{2}_\\d{2}"), 
                                                  full.names = TRUE)
  
  files_raw_v2 <- 
    list.files(dir, pattern = paste0({{ pattern }}, " \\(\\d{4} \\d{2} \\d{2}\\)"), 
                             full.names = TRUE)
  
  files_raw <- c(files_raw_v1, files_raw_v2) %>% sort()

  file_dates_p1 <- files_raw %>% str_extract("\\d{4} \\d{2} \\d{2}")
  file_dates_p2 <- files_raw %>% str_extract("\\d{4}_\\d{2}_\\d{2}") %>%
    str_replace_all("_", " ")
  
  file_dates_p1[is.na(file_dates_p1)] <- file_dates_p2[!is.na(file_dates_p2)]
  
  file_dates <- 
    unique(file_dates_p1) %>% 
    as.Date("%Y %m %d") %>% 
    sort()
  
  if(length(file_dates) < 10)
    return("Cancelled - Less than 10 unique dates exist.")
  
  date_seq_keep <-
    seq(from = 1, to = length(file_dates), by = step) %>% 
    c(., length(file_dates) - 0:1) %>% unique() %>% sort()
  
  date_seq_del <-
    setdiff(seq_along(file_dates), date_seq_keep)
  
  dates_to_del <- 
    file_dates[date_seq_del] %>% 
    str_replace_all("-", " ")
  
  files_to_del <- files_raw[which(file_dates_p1 %in% dates_to_del)] %>% sort()
  
  file.copy(from = files_to_del, to = "C:/Users/user/Documents/Temporary Recycle Bin")
}






