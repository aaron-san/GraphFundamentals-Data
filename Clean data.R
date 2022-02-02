#######################
# Clean fundamentals
#######################
# - Add download_date
# - Reformat dates, numbers, etc.
# - Combine multiple tickers into big files

library(tidyverse)
library(data.table)

dir_proj <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data"

today <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")

# Add download date
# Clean up formatting
read_and_format <- function(x) {
    #####
    # x <- bs_files[1]
    #####
    
    tbl <- x %>% fread(fill = TRUE, integer64 = "double", data.table = FALSE)
    
    if(!"download_date" %in% colnames(tbl)) {
        download_date <- x %>% 
            str_extract("(?<=\\()[0-9]{4} [0-9]{1,2} [0-9]{1,2}(?=\\))") %>% 
            as.Date("%Y %m %d")
        tbl <- tbl %>% 
            add_column(download_date = download_date)
    }
    
    tbl %>% 
        mutate(across(-c(ticker, date, download_date), 
                      ~str_remove_all(.x, ","))) %>%
        mutate(across(-c(ticker, date, download_date), as.numeric)) %>% 
        mutate(ticker = as.character(ticker)) %>% 
        mutate(across(contains("date"), as.Date)) %>% 
        drop_na(ticker, date, download_date)
}


get_cleaned_data <- function(files) {
    map_df(files, read_and_format) %>% 
        arrange(ticker, date, desc(download_date)) %>% 
        drop_na(ticker, date, download_date) %>% 
        group_by(ticker, date) %>% 
        # The data is sorted with the most recent download_date first.
        # If the most recent data is missing, pull it from the prior
        # download dates (fill up)
        fill(!c(ticker, date, download_date), .direction = "downup") %>% 
        slice(1) %>% 
        # filter(!duplicated(ticker, date)) %>% 
        ungroup()
}




# List files
#  - Include the most recent cleaned income_statement files (will have many 
#    duplicates)
raw_files <- 
    list.files(paste0(dir_proj, "/data/raw data"), 
               pattern = "income_statement|balance_sheet|cash_flow",
               full.names = TRUE)
cleaned_files <- 
    list.files(paste0(dir_proj, "/data/cleaned data"), 
               pattern = "income_statement|balance_sheet|cash_flow",
               full.names = TRUE)
combined_files <- sort(unique(c(raw_files, cleaned_files)), decreasing = TRUE)


# Subset files (SLOW!!)
is_files <- combined_files %>% str_subset("income_statement")
bs_files <- combined_files %>% str_subset("balance_sheet")
cf_files <- combined_files %>% str_subset("cash_flow")


# Clean data
is_data <- get_cleaned_data(is_files)
bs_data <- get_cleaned_data(bs_files)
cf_data <- get_cleaned_data(cf_files)

# Save
fwrite(is_data, paste0(dir_proj, "/data/cleaned data/income_statements_cleaned ",
                       today, ".csv" ))
fwrite(bs_data, paste0(dir_proj, "/data/cleaned data/balance_sheets_cleaned ",
                       today, ".csv" ))
fwrite(cf_data, paste0(dir_proj, "/data/cleaned data/cash_flows_cleaned ",
                       today, ".csv" ))




# Backup data -------------------------------------------------------------

# Remove old cleaned files
remove_old_cleaned_files <- function() {
    cleaned_files <- 
        list.files(paste0(dir_proj, "/data/cleaned data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    latest_date <- 
        cleaned_files %>% 
        str_extract("(?<= \\()\\d{4} \\d{2} \\d{2}(?=\\)\\.csv)") %>% 
        max()
    
    files_to_remove <- 
        cleaned_files %>% 
        .[{!cleaned_files %>% str_detect(latest_date)}]
    
    file.remove(files_to_remove)
}
remove_old_cleaned_files()



# Zip remaining cleaned files
zip_remaining_files <- function() {
    cleaned_files <- 
        list.files(paste0(dir_proj, "/data/cleaned data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    
    zip::zipr(zipfile = paste0(dir_proj, "/data/backup/cleaned data - backup ", 
                              today, ".zip"), files = cleaned_files)
}

zip <- try(zip_remaining_files())

# Remove old raw files
remove_old_raw_files <- function() {
    raw_files <- 
        list.files(paste0(dir_proj, "/data/raw data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    
    file.remove(raw_files)
}

if(!"try-error" %in% class(zip)) {
    remove_old_raw_files()
}


