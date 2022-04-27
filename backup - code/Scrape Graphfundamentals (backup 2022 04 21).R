
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)
library(httr)

source("helper functions.R")
source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")


library(tidyverse)    
library(RSelenium)

# Connect to the Chrome driver
remote_driver <- get_chrome_driver()


# Navigate to the login page
remote_driver$navigate("https://wallstreetmillennial.com/login")
suppressMessages(tryCatch(click_modal(), silent = TRUE, error=function(e){}))
login()



# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

scrape_fundamentals <- function(ticker) {
    
    #####
    ticker <- "MSFT"
    # ticker <- "COP"
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
    
    bs_url <- paste0(ticker_url, "&stmt=BS$frequency")
    is_url <- paste0(ticker_url, "&stmt=IS$frequency")
    cf_url <- paste0(ticker_url, "&stmt=CF$frequency")
    
    
    # If the webpage has an error 500 message, return NA
    if(status_code(GET(bs_url)) == 500) return(NA)
    if(status_code(GET(is_url)) == 500) return(NA)
    if(status_code(GET(cf_url)) == 500) return(NA)
    
    
    # Navigate to the ticker's page
    remote_driver$navigate(is_url)
    
    # Get ticker printed on webpage
    ticker_element <- remote_driver$findElement(using = "xpath",
                                                value = '//*[@id="tableTitle"]')
    page_ticker <- 
        ticker_element$getElementText() %>% 
        unlist() %>% 
        str_extract("(?<=\\()[A-Z]+(?=\\))")
    
    
    if(!exists("page_ticker")) return (NA)
    if(is.na(page_ticker)) return(NA)
    
    # If the loop ticker doesn't equal the ticker on the webpage, return NA
    #  The webpage sometimes forwards to AAPL's page if the ticker link 
    #  isn't available, without raising an error
    if(ticker != page_ticker) return(NA)
    
    
    
    
    
    
    
    
    bs_link_element <- find_element_xpath(remote_driver, '/html/body/section/div[2]/div[1]/div/div[1]/div/aside/ul/li[2]/a')
    bs_link_element$clickElement()
    
    quarterly_link <- find_element_xpath(remote_driver, '//*[@id="quarterlyFilingFrequencyTabA"]')
    quarterly_link$clickElement()
    
    # remote_driver$navigate(paste0(bs_url, "=annual"))
    annual_link <- find_element_xpath(remote_driver, '//*[@id="annualFilingFrequencyTabA"]')
    annual_link$clickElement()
    
    
    data_table <- find_element_xpath(remote_driver, '/html/body/section/div[2]/div[2]/div/div[3]')
    
    is_q <-
        data_table$getElementAttribute('innerHTML')[[1]] %>% 
        read_html() %>% 
        html_table() %>% .[[1]]
    
    is_q
    
    
    
    
    # test_url <- read_html(bs_url)
    
    
    if(bs_tibble %>% nrow() == 0 | bs_tibble %>% ncol() <= 2) {
        bs_df <- NA 
    } else {
        bs_df <- 
            bs_tibble %>% 
            select(-any_of("X")) %>% 
            janitor::remove_empty(which = "cols") %>% 
            rename(field = Fields) %>% 
            # append_number_dups() %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>%
            mutate(across(-field, as.numeric)) %>% 
            janitor::clean_names() %>% 
            add_column(ticker = ticker) %>%
            select(ticker, field, everything())
    }
    
    
    is_tibble <- is_page %>% flatten() %>% as_tibble(.name_repair=make.names)
    
    if(is_tibble %>% nrow() == 0 | is_tibble %>% ncol() <= 2) {
        is_df <- NA
    } else {
        is_df <- 
            is_tibble %>% 
            select(-any_of("X")) %>% 
            janitor::remove_empty(which = "cols") %>% 
            rename(field = Fields) %>% 
            # append_number_dups() %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>% 
            mutate(across(-field, as.numeric)) %>% 
            janitor::clean_names() %>% 
            add_column(ticker = ticker) %>% 
            select(ticker, field, everything())
        }
    
    
    cf_tibble <- cf_page %>% flatten() %>% as_tibble(.name_repair=make.names)
    
    if(cf_tibble %>% nrow() == 0 | cf_tibble %>% ncol() <= 2) {
        cf_df <- NA 
    } else {
        cf_df <- 
            cf_tibble %>% 
            select(-any_of("X")) %>% 
            janitor::remove_empty(which = "cols") %>% 
            rename(field = Fields) %>% 
            # append_number_dups() %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>% 
            mutate(across(-field, as.numeric)) %>% 
            janitor::clean_names() %>% 
            add_column(ticker = ticker) %>% 
            select(ticker, field, everything())
        }
    
    return(list(balance_sheet = bs_df,
                income_statement = is_df,
                cash_flows = cf_df))
    
}


# scrape_fundamentals(ticker = "MSFT")
# scrape_fundamentals(ticker = "GM")
# scrape_fundamentals(ticker = "DLTR")
# scrape_fundamentals(ticker = "COP")


save_fundamentals <- function(tickers) {
    #####
    # tickers <- "HDdfd"
    # tickers <- "SOHU"
    #####
    
    dir_w <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/data/raw data/"
    download_date <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")
    
    for(ticker in tickers) {
        
        #####
        # ticker <- "SOHU"
        #####
        print(ticker)
        
        fundamentals <- scrape_fundamentals(ticker)
        
        if(!is.na(fundamentals)) {
            if(!is.na(fundamentals$balance_sheet)) {
                fwrite(fundamentals$balance_sheet, 
                       paste0(dir_w, ticker, " - balance_sheet ", 
                              download_date, ".csv"))
            }
            if(!is.na(fundamentals$income_statement)) {
                fwrite(fundamentals$income_statement, 
                       paste0(dir_w, ticker, " - income_statement ", 
                              download_date, ".csv"))
            }
            if(!is.na(fundamentals$cash_flows)) {
                fwrite(fundamentals$cash_flows, 
                       paste0(dir_w, ticker, " - cash_flows ", 
                              download_date, ".csv"))
            }
            
            Sys.sleep(1)
        }
    }
}



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


download_fundamentals <- function(start_id=NULL, start_ticker=NULL, tickers=NULL) {
    #######
    # start_ticker <- NULL #"CCL"
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
        start_id <- tickers_with_clean_prices %>% str_which(start_ticker)
    }

    # Download fundamentals
    # for(i in seq_along(start)) {
    for(i in start_id:length(tickers_with_clean_prices)) {
        # i <- 1
        save_fundamentals(tickers = tickers_with_clean_prices[i])
        # print(paste0(start[i], "-", end[i], " done!"))
    }
}


# DOWNLOAD ----------------------------------------------------------------

# download_fundamentals(start_id = 1,
#                       # start_ticker = "VLDR",
#                       tickers = tickers_in_files)


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


