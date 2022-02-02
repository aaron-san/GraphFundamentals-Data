
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)
library(httr)

source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")


# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

scrape_fundamentals <- function(ticker) {
    
    #####
    # ticker <- "A"
    # ticker <- "SOFI"
    # ticker <- "AAAA"
    # ticker <- "AAC"
    # ticker <- "AACQ"
    # ticker <- "AAON"
    # ticker <- "SOHU"
    # ticker <- "sdfddsfd"
    #####
    
    base_url <- "https://graphfundamentals.com/graphfundamentals?ticker="
    ticker_url <- paste0(base_url, str_to_upper(ticker))
    bs_url <- paste0(ticker_url, "&sheettype=BS#")
    is_url <- paste0(ticker_url, "&sheettype=IS#")
    cf_url <- paste0(ticker_url, "&sheettype=CF#")
    
    # If the webpage has an error 500 message, return NA
    if(status_code(GET(bs_url)) == 500) return(NA)
    if(status_code(GET(is_url)) == 500) return(NA)
    if(status_code(GET(cf_url)) == 500) return(NA)
    
    test_url <- read_html(bs_url)
    # Get ticker printed on webpage
    page_ticker <- test_url %>% 
        xml_nodes("h1.h2") %>% html_text() %>% 
        str_extract("(?<=\\()[A-Z]{1,15}(?=\\))")
    if(is.na(page_ticker)) return(NA)
    # If the loop ticker doesn't equal the ticker on the webpage, return NA
    #  The webpage sometimes forwards to AAPL's page if the ticker link 
    #  isn't available, without raising an error
    if(ticker != page_ticker) return(NA)
    
    bs_page <- test_url %>% html_table()
    is_page <- read_html(is_url) %>% html_table()
    if(bs_page %>% flatten() %>% 
       as_tibble(.name_repair=make.names) %>% nrow() == 0) return(NA)
    cf_page <- read_html(cf_url) %>% html_table()
    
    
    bs_tibble <- bs_page %>% flatten() %>% as_tibble(.name_repair=make.names)
    if(bs_tibble %>% nrow() == 0 | bs_tibble %>% ncol() <= 2) {
        bs_df <- NA 
    } else {
        bs_df <- 
            bs_page %>% 
            flatten() %>% 
            as_tibble(.name_repair = make.names) %>% 
            .[, colSums(is.na(.)) < nrow(.)] %>% 
            rename(field = X) %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>% 
            mutate(across(-field, as.numeric)) %>% 
            pivot_longer(-field, names_to = "date", values_to = "value") %>% 
            pivot_wider(names_from = field, values_from = value) %>% 
            janitor::clean_names() %>% 
            mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
            add_column(ticker = ticker) %>% 
            select(ticker, date, everything())
    }
    
    
    is_tibble <- is_page %>% flatten() %>% as_tibble(.name_repair=make.names)
    if(is_tibble %>% nrow() == 0 | is_tibble %>% ncol() <= 2) {
        is_df <- NA
    } else {
        is_df <- 
            is_page %>% 
            flatten() %>% 
            as_tibble(.name_repair = make.names) %>% 
            .[, colSums(is.na(.)) < nrow(.)] %>% 
            rename(field = X) %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>% 
            mutate(across(-field, as.numeric)) %>% 
            pivot_longer(-field, names_to = "date", values_to = "value") %>% 
            pivot_wider(names_from = field, values_from = value) %>% 
            janitor::clean_names() %>% 
            mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
            add_column(ticker = ticker) %>% 
            select(ticker, date, everything())
        }
    
    
    cf_tibble <- cf_page %>% flatten() %>% as_tibble(.name_repair=make.names)
    if(cf_tibble %>% nrow() == 0 | cf_tibble %>% ncol() <= 2) {
        cf_df <- NA 
    } else {
        cf_df <- cf_page %>% 
            flatten() %>% 
            as_tibble(.name_repair = make.names) %>% 
            .[, colSums(is.na(.)) < nrow(.)] %>% 
            rename(field = X) %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>% 
            mutate(across(-field, as.numeric)) %>% 
            pivot_longer(-field, names_to = "date", values_to = "value") %>% 
            pivot_wider(names_from = field, values_from = value) %>% 
            janitor::clean_names() %>% 
            mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
            add_column(ticker = ticker) %>% 
            select(ticker, date, everything())
        }
    
    return(list(balance_sheet = bs_df,
                income_statement = is_df,
                cash_flows = cf_df))
    
}


# scrape_fundamentals(ticker = "erWEFDF")


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
            
            Sys.sleep(3)
        }
    }
}



tickers_with_clean_prices <- 
    read_lines("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/cleaned data/tickers_with_clean_prices.txt")


# Get tickers from cleaned downloaded data
cleaned_data_files <- list.files("data/cleaned data", 
                         pattern = "balance_sheet|income_statement|cash_flow", 
                         full.names = TRUE)
if(length(cleaned_data_files) == 0) {
    cleaned_data_files <- 
        unzip("data/backup/backup - 2021 11 11.zip", list = TRUE)$Name
}

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

download_fundamentals(start_id = 1,
                      # start_ticker = "VLDR",
                      tickers = tickers_in_files)




    
    

