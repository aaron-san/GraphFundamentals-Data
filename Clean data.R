
# Clean fundamentals

source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")


bs_files <- list.files("data", pattern = "balance_sheet", full.names = TRUE)
bs_data <- map(bs_files, read_tibble)
clean_stmt <- . %>% 
    mutate(across(-c(ticker, date), ~str_remove_all(.x, ","))) %>% 
    mutate(across(-c(ticker, date), as.numeric))
bs_cleaned <- bs_data %>% map(clean_stmt)
bs_joined <- reduce(bs_cleaned, full_join)



is_files <- list.files("data", pattern = "income_statement", full.names = TRUE)
is_data <- map(is_files, read_tibble)
clean_stmt <- . %>% 
    mutate(across(-c(ticker, date), ~str_remove_all(.x, ","))) %>% 
    mutate(across(-c(ticker, date), as.numeric))
is_cleaned <- is_data %>% map(clean_stmt)
is_joined <- reduce(is_cleaned, full_join)


cf_files <- list.files("data", pattern = "cash_flows", full.names = TRUE)
cf_data <- map(cf_files, read_tibble)
clean_stmt <- . %>% 
    mutate(across(-c(ticker, date), ~str_remove_all(.x, ","))) %>% 
    mutate(across(-c(ticker, date), as.numeric))
cf_cleaned <- cf_data %>% map(clean_stmt)
cf_joined <- reduce(cf_cleaned, full_join)


today <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")
# Save
fwrite(bs_joined, paste0("data/cleaned data/balance_sheets_cleaned ", today, ".csv" ))
fwrite(is_joined, paste0("data/cleaned data/income_statements_cleaned ", today, ".csv" ))
fwrite(cf_joined, paste0("data/cleaned data/cash_flows_cleaned ", today, ".csv" ))






