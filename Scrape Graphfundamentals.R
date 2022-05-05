
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)
library(httr)

source("helper functions.R")
source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")

# library(wdman)
library(RSelenium)
# selServ <- selenium(jvmargs = c("-Dwebdriver.chrome.verboseLogging=true"))
# remDr <- remoteDriver(port = 4567L, browserName = "firefox")
# remDr$open()
# selServ$log()
# remDr$getStatus()

# Get login credentials
login_details <- read_tibble("private/wsm_pw.csv")


# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

get_fundamentals <- function(driver, ticker) {
    
    # driver <- remote_driver
    
    #####
    # ticker <- "AE"
    # ticker <- "ACGLP"
    # ticker <- "ACHC"
    # ticker <- "AAN"
    # ticker <- "SOFI"
    # ticker <- "AAAA"
    # ticker <- "AAC"
    # ticker <- "AACQ"
    # ticker <- "AAON"
    # ticker <- "SOHU"
    # ticker <- "sdfddsfd"
    #####
    
    base_url <- "https://wallstreetmillennial.com/graphfundamentals?ticker="
    ticker_url <- paste0(base_url, str_to_upper(ticker))
    
    is_url <- paste0(ticker_url, "&stmt=IS")
    bs_url <- paste0(ticker_url, "&stmt=BS")
    cf_url <- paste0(ticker_url, "&stmt=CF")
    
    # If the webpage has an error 500 message, return NA
    # is_url_get <- GET(is_url); if(status_code(is_url_get) == 500) return(NA)
    # bs_url_get <- GET(bs_url); if(status_code(bs_url_get) == 500) return(NA)
    # cf_url_get <- GET(cf_url); if(status_code(cf_url_get) == 500) return(NA)
    
    # driver$setTimeout(type = "page load", milliseconds = 50000)
    Sys.sleep(3)
    
    
    # Navigate to the ticker's page
    driver$navigate(is_url)
    # driver$setTimeout(type = "page load", milliseconds = 50000)
    Sys.sleep(3)
    
    # Check for invalid ticker message
    warning_invalid_ticker <- 
        suppressMessages(try(find_element_selector(driver, '.notification'),
                             silent = TRUE))
    
    invalid_ticker <-
        suppressMessages(try(warning_invalid_ticker$getElementAttribute('innerHTML')[[1]],
             silent = TRUE)) %>% 
        str_detect("Invalid ticker submitted")
    
    if(invalid_ticker) return(NA)
    
    
    # Check that ticker matches url ticker
    validate_ticker(driver = driver, ticker = ticker)

    save_data <- function(df, ticker, name) {
        # df <- profile_data
        # ticker <- "MSFT"
        # name <- "profile"
        
        dir_w <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/data/raw data/"
        download_date <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")
        
        dir_w_date <- paste0(dir_w, "/", paste0(str_replace_all(Sys.Date(), "-", " ")))
        
        if(!file.exists("dir_w_date")) {
            dir.create(dir_w_date, showWarnings = FALSE)    
        }
        
        if(!all(is.na(df))) {
            fwrite(df, 
                   paste0(dir_w_date, "/", ticker, " - ", name, " ",
                          download_date, ".csv"))
        }    
    }
    
    
    # Get profile data
    profile_data <- get_profile_data(driver = driver, url = is_url)
    # Save
    save_data(df = profile_data, ticker = ticker, name = "profile")
    
    is_quarterly <- get_table_data(
        driver = driver,
        url = is_url,
        statement = "income_statement",
        period = "quarterly")
    is_quarterly_cleaned <- clean_df(is_quarterly, ticker = ticker)
    
    # Save
    save_data(df = is_quarterly_cleaned, ticker = ticker, name = "income_statement_quarterly")
    
    is_yearly <- get_table_data(
        driver = driver,
        url = is_url,
        statement = "income_statement",
        period = "yearly")
    is_yearly_cleaned <- clean_df(is_yearly, ticker = ticker)
    
    # Save
    save_data(df = is_yearly_cleaned, ticker = ticker, name = "income_statement_yearly")
    
    bs_quarterly <- get_table_data(
        driver = driver,
        url = bs_url,
        statement = "balance_sheet",
        period = "quarterly"
    )
    bs_quarterly_cleaned <- clean_df(bs_quarterly, ticker = ticker)
    
    # Save
    save_data(df = bs_quarterly_cleaned, ticker = ticker, name = "balance_sheet_quarterly")
    
    bs_yearly <- get_table_data(
        driver = driver,
        url = bs_url,
        statement = "balance_sheet",
        period = "yearly"
    )
    bs_yearly_cleaned <- clean_df(bs_yearly, ticker = ticker)
    
    # Save
    save_data(df = bs_yearly_cleaned, ticker = ticker, name = "balance_sheet_yearly")
        
    cf_quarterly <- get_table_data(
        driver = driver,
        url = cf_url,
        statement = "cash_flows",
        period = "quarterly"
    )
    cf_quarterly_cleaned <- clean_df(cf_quarterly, ticker = ticker)
    
    # Save
    save_data(df = cf_quarterly_cleaned, ticker = ticker, name = "cash_flows_quarterly")
    
    cf_yearly <- get_table_data(
        driver = driver,
        url = cf_url,
        statement = "cash_flows",
        period = "yearly"
    )
    cf_yearly_cleaned <- clean_df(cf_yearly, ticker = ticker)
    
    # Save
    save_data(df = cf_yearly_cleaned, ticker = ticker, name = "cash_flows_yearly")
}

# Connect to remote server
# remote_driver <- get_remote_driver()
# prepare_webpage(driver = remote_driver)

# get_fundamentals(driver = remote_driver, ticker = "GM")





# remove("ticker_data_1")
# ticker_data_1 <- scrape_fundamentals(driver = remote_driver, ticker = "MSFT")
# ticker_data_2 <- scrape_fundamentals(driver = remote_driver, ticker = "GM")
# ticker_data_3 <- scrape_fundamentals(driver = remote_driver, ticker = "GE")
# ticker_data_4 <- scrape_fundamentals(driver = remote_driver, ticker = "LTCM")
# ticker_data_5 <- scrape_fundamentals(driver = remote_driver, ticker = "DLTR")
# ticker_data_6 <- scrape_fundamentals(driver = remote_driver, ticker = "COP")












tickers_with_clean_prices <- 
    read_lines("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/cleaned data/tickers_with_clean_prices.txt")


# Get tickers from cleaned downloaded data
cleaned_data_files <- list.files("data/cleaned data", 
                         pattern = "balance_sheets_quarterly_cleaned|income_statements_quarterly_cleaned|cash_flows_quarterly_cleaned", 
                         full.names = TRUE)
# if(length(cleaned_data_files) == 0) {
#     cleaned_data_files <- 
#         unzip("data/backup/backup - 2021 11 11.zip", list = TRUE)$Name
# }

tickers_in_files <- 
    cleaned_data_files %>% #str_extract("(?<=\\/)[A-Za-z]{1,20}(?= -)")
    map_df(~read_tibble(.x)) %>% 
    distinct(ticker) %>% 
    pull()

# Proportion of tickers downloaded (at least once)
length(tickers_in_files) / length(tickers_with_clean_prices)


download_fundamentals <- function(ff_driver, start_id=NULL, start_ticker=NULL, tickers=NULL) {
    #######
    # driver <- remote_driver
    # ff_driver <- ff_driver
    # start_ticker <- NULL #"CCL"
    # start_ticker <- "AE"
    # start_id <- 1 # NULL
    # tickers <- tickers_with_clean_prices
    #######
    
    if(is.null(tickers)) 
        stop("Provide vector of tickers")
    if(is.null(start_id) & is.null(start_ticker))
        stop("Provide a start_id or start_ticker")
    if(!is.null(start_id) & !is.null(start_ticker))
        stop("Provide one of either a start_id or a start_ticker")
    
    if(!is.null(start_ticker)) {
        start_ticker <- paste0("^", start_ticker, "$")
        start_id <- tickers %>% str_which(start_ticker)
    }

    driver <- ff_driver[["client"]]
    
    # Download fundamentals
    # for(i in seq_along(start)) {
    for(i in start_id:length(tickers)) {
        
        print(tickers[i])
        
        # If disconnected, reconnect to remote server
        # ff_driver$server$stop()
        server_status <- tryCatch(
            ff_driver$server$process$get_status(),
            error = function(e) {
                ff_driver <- get_driver()
                remote_driver <- ff_driver[["client"]]
                prepare_webpage(driver = remote_driver)
                driver <- remote_driver    
            })
        
        # i <- 1
        get_fundamentals(driver = driver, ticker = tickers[i])
        
        # print(paste0(start[i], "-", end[i], " done!"))
    }
}



# DOWNLOAD ----------------------------------------------------------------


# Connect to remote server
ff_driver <- get_driver()
# ff_driver$server$process
# ff_driver$server$stop()
remote_driver <- ff_driver[["client"]]
prepare_webpage(driver = remote_driver)

download_fundamentals(ff_driver = ff_driver,
                      # start_id = 1,
                      start_ticker = "AEP",
                      tickers = tickers_in_files)



# BAD!!!!!!!!!!!!!!!!!
ADXN

- Can run multiple docker containers simultaneously?
- Docker containers have unique IP addresses?




# ------------------------------------------------------------------------- #
#Include the parallel library. If the next line does not work, run install.packages(“parallel”) first
library(parallel)

# Use the detectCores() function to find the number of cores in system
no_cores <- detectCores()

# Setup cluster
clust <- makeCluster(no_cores - 1) #This line will take time

#Setting a base variable 
# base <- 4
#Note that this line is required so that all cores in cluster have this variable available
clusterExport(clust, c("save_fundamentals", "str_replace_all",
                       "scrape_fundamentals", "str_to_upper",
                       "status_code", "GET", "read_html",
                       "%>%", "str_extract", "html_text",
                       "html_elements", "html_table",
                       "as_tibble", "flatten", "select",
                       "add_column", "mutate", "rename",
                       "str_remove_all", "fwrite"))

# parLapply(clust, 1:5, function(x) c(x^2,x^3))
parLapply(clust, tickers_with_clean_prices,
          function(x) save_fundamentals(tickers = x))

# 7,548 files (2022-02-17)

stopCluster(clust)


