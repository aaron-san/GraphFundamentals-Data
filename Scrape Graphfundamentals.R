
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)


# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

scrape_fundamentals <- function(ticker) {
    
    #####
    # ticker <- "SOFI"
    # ticker <- "sdfddsfd"
    #####
    
    bs_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                  str_to_upper(ticker), "/BS#")
    
    is_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/IS#")
    
    cf_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/CF#")
    
    test_url <- read_html(bs_url)
    page_ticker <- test_url %>% 
        xml_nodes("h1.h2") %>% html_text() %>% 
        str_extract("(?<=\\()[A-Z]{1,10}(?=\\))")
    if(ticker != page_ticker) return(NA)
    
    
    bs_page <- test_url %>% html_table()
    is_page <- read_html(is_url) %>% html_table()
    cf_page <- read_html(cf_url) %>% html_table()
    
    bs_df <- bs_page %>% 
        flatten() %>% 
        as_tibble(.name_repair = make.names) %>% 
        .[, colSums(is.na(.)) < nrow(.)] %>% 
        rename(field = X) %>% 
        pivot_longer(-field, names_to = "date", values_to = "value") %>% 
        pivot_wider(names_from = field, values_from = value) %>% 
        janitor::clean_names() %>% 
        mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
        add_column(ticker = ticker) %>% 
        select(ticker, date, everything())
    
    
    is_df <- is_page %>% 
        flatten() %>% 
        as_tibble(.name_repair = make.names) %>% 
        .[, colSums(is.na(.)) < nrow(.)] %>% 
        rename(field = X) %>% 
        pivot_longer(-field, names_to = "date", values_to = "value") %>% 
        pivot_wider(names_from = field, values_from = value) %>% 
        janitor::clean_names() %>% 
        mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
        add_column(ticker = ticker) %>% 
        select(ticker, date, everything())
    
    cf_df <- cf_page %>% 
        flatten() %>% 
        as_tibble(.name_repair = make.names) %>% 
        .[, colSums(is.na(.)) < nrow(.)] %>% 
        rename(field = X) %>% 
        pivot_longer(-field, names_to = "date", values_to = "value") %>% 
        pivot_wider(names_from = field, values_from = value) %>% 
        janitor::clean_names() %>% 
        mutate(date = as.Date(date, "X%Y.%m.%d")) %>% 
        add_column(ticker = ticker) %>% 
        select(ticker, date, everything())
    
    return(list(balance_sheet = bs_df,
                income_statement = is_df,
                cash_flows = cf_df))
    
}


# scrape_fundamentals(ticker = "erWEFDF")


save_fundamentals <- function(tickers) {
    #####
    # tickers <- "HDdfd"
    #####
    
    dir_w <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/data/"
    download_date <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")
    
    for(ticker in tickers) {
        
        #####
        # ticker <- "DFDSE"
        #####
        
        fundamentals <- scrape_fundamentals(ticker)
        
        fwrite(fundamentals$balance_sheet, 
               paste0(dir_w, ticker, " - balance_sheet ", download_date, ".csv"))
        fwrite(fundamentals$income_statement, 
               paste0(dir_w, ticker, " - income_statement ", download_date, ".csv"))
        fwrite(fundamentals$cash_flows, 
               paste0(dir_w, ticker, " - cash_flows ", download_date, ".csv"))        
    }
}



tickers_with_clean_prices <- 
    read_lines("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/cleaned data/tickers_with_clean_prices.txt")

tickers <- tickers_with_clean_prices[1:100]


tickers <- c("AAPL", "HD", "BBY")






