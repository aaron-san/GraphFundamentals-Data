#######################
# Clean fundamentals
#######################
# - Add download_date
# - Reformat dates, numbers, etc.
# - Combine multiple tickers into big files

library(dplyr)
library(stringr)
library(data.table)

read_tibble <- function(x, date_format = "%Y-%m-%d", ...) {
    
    x %>%
        fread(fill = TRUE, integer64 = "double", data.table = FALSE) %>%
        # Some objects have multiple date classes, so coerce it to "date"    
        mutate(across(where(is.Date), ~as.Date(.x))) %>% 
        mutate(across(any_of("date"), ~as.Date(.x, date_format)))
}




today <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")

read_and_format <- function(x) {
    #####
    # x <- bs_files[1497]
    #####
    
    download_date <- x %>% 
        str_extract("(?<=\\()[0-9]{4} [0-9]{1,2} [0-9]{1,2}(?=\\))") %>% 
        as.Date("%Y %m %d")
    
    read_tibble(x) %>% 
        add_column(download_date = download_date) %>% 
        mutate(across(-c(ticker, date, download_date), 
                      ~str_remove_all(.x, ","))) %>%
        mutate(across(-c(ticker, date, download_date), as.numeric)) %>% 
        mutate(ticker = as.character(ticker)) %>% 
        mutate(across(contains("date"), as.Date)) %>% 
        drop_na(ticker, date, download_date)
}



# Clean balance sheets 
# ~20 mins!

bs_raw_files <- list.files("data", pattern = "balance_sheet", full.names = TRUE)
# Always include the most recent cleaned balance_sheet files (will have many     duplicates)
bs_cleaned_files <- list.files("data/cleaned data", pattern = "balance_sheet", full.names = TRUE)


bs_data <- map_df(bs_cleaned_files, ~fread(.x, fill = TRUE, data.table = FALSE, integer64 = "double")) %>% as_tibble()

bs_data_cleaned <-
    bs_data %>% colnames()
    group_by(ticker, date) %>% 
    # Fill NAs
    fill(-c(ticker, date), .direction = "downup") %>% 
    ungroup() %>% 
    # Remove duplicates
    filter(!duplicated(select(., ticker, date)))







max_date_bs_files <- bs_cleaned_files %>% 
    str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
bs_cleaned_files <- bs_cleaned_files %>% str_subset(max_date_bs_files)
bs_files <- sort(unique(c(bs_raw_files, bs_cleaned_files)), decreasing = TRUE)


step <- 500
start <- seq(from = 1, to = length(bs_files), by = step)
end <- start + step - 1
end[length(end)] <- min(length(bs_files), end[length(end)])

start_bs <- Sys.time()
for(i in seq_along(start)) {
    
    bs_data <- 
        ####
        # i <- 1
        ####
        map_df(bs_files[start[i]:end[i]], read_and_format) %>% 
        arrange(ticker, date, desc(download_date)) %>%
        group_by(ticker, date) %>% 
        # The data is sorted with the most recent download_date first.
        # If the most recent data is missing, fill missing values down,
        # then pull them up from the prior download dates (fill "downup")
        fill(!c(ticker, date, download_date), .direction = "downup") %>% 
        filter(!duplicated(ticker, date)) %>% 
        ungroup()

    # Save
    fwrite(bs_data, paste0("data/cleaned data/balance_sheets_cleaned ",
                           start[i], "_", end[i], " ", today, ".csv" ))
}
end_bs <- Sys.time() - start_bs; end_bs


# Clean income statements
# is_files <- list.files("data", pattern = "income_statement", full.names = TRUE)
is_raw_files <- list.files("data", pattern = "income_statement", full.names = TRUE)
# Always include the most recent cleaned income_statement files (will have many duplicates)
is_cleaned_files <- list.files("data/cleaned data", pattern = "income_statement", full.names = TRUE)
max_date_is_files <- is_cleaned_files %>% 
    str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
is_cleaned_files <- is_cleaned_files %>% str_subset(max_date_is_files)

is_files <- sort(unique(c(is_raw_files, is_cleaned_files)), decreasing = TRUE)
# is_files[which.max(map(is_files, nchar))]

step <- 500
start <- seq(from = 1, to = length(is_files), by = step)
end <- start + step - 1
end[length(end)] <- min(length(is_files), end[length(end)])


start <- start[8:11]
end <- end[8:11]


start_is <- Sys.time()
for(i in seq_along(start)) {
    
    is_data <- 
        ####
        # i <- 1
        ####
        map_df(is_files[start[i]:end[i]], read_and_format) %>% 
        arrange(ticker, date, desc(download_date)) %>% 
        drop_na(ticker, date, download_date) %>% 
        group_by(ticker, date) %>% 
        # The data is sorted with the most recent download_date first.
        # If the most recent data is missing, pull it from the prior
        # download dates (fill up)
        fill(!c(ticker, date, download_date), .direction = "up") %>% 
        filter(!duplicated(ticker, date)) %>% 
        ungroup()
    
    # Save
    fwrite(is_data, paste0("data/cleaned data/income_statements_cleaned ",
                           start[i], "_", end[i], " ", today, ".csv" ))
}
end_is <- Sys.time() - start_is; end_is



# Clean cash flows
# ~17mins
# cf_files <- list.files("data", pattern = "cash_flows", full.names = TRUE)
cf_raw_files <- list.files("data", pattern = "cash_flow", full.names = TRUE)
# Always include the most recent cleaned cash_flow files (will have many duplicates)
cf_cleaned_files <- list.files("data/cleaned data", pattern = "cash_flow", full.names = TRUE)
max_date_cf_files <- cf_cleaned_files %>% 
    str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
cf_cleaned_files <- cf_cleaned_files %>% str_subset(max_date_cf_files)

cf_files <- sort(unique(c(cf_raw_files, cf_cleaned_files)), decreasing = TRUE)
# cf_files[which.max(map(cf_files, nchar))]

step <- 500
start <- seq(from = 1, to = length(cf_files), by = step)
end <- start + step - 1
end[length(end)] <- min(length(cf_files), end[length(end)])

start_cf <- Sys.time()
for(i in seq_along(start)) {
    
    cf_data <- 
        ####
        # i <- 1
        ####
        map_df(cf_files[start[i]:end[i]], read_and_format) %>% 
        arrange(ticker, date, desc(download_date)) %>% 
        drop_na(ticker, date, download_date) %>% 
        group_by(ticker, date) %>% 
        # The data is sorted with the most recent download_date first.
        # If the most recent data is missing, pull it from the prior
        # download dates (fill up)
        fill(!c(ticker, date, download_date), .direction = "up") %>% 
        filter(!duplicated(ticker, date)) %>% 
        ungroup()
    
    # Save
    fwrite(cf_data, paste0("data/cleaned data/cash_flows_cleaned ",
                           start[i], "_", end[i], " ", today, ".csv" ))
}
end_cf <- Sys.time() - start_cf; end_cf



