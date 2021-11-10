
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)
library(httr)



# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

scrape_fundamentals <- function(ticker) {
    
    #####
    # ticker <- "SOFI"
    # ticker <- "AAAA"
    # ticker <- "AAC"
    # ticker <- "AACQ"
    # ticker <- "AAON"
    # ticker <- "BZUN"
    # ticker <- "sdfddsfd"
    #####
    
    bs_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                  str_to_upper(ticker), "/BS#")
    
    is_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/IS#")
    
    cf_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/CF#")
    
    # If the webpage has an error 500 message, return NA
    if(status_code(GET(bs_url)) == 500) return(NA)
    
    test_url <- read_html(bs_url)
    # Get ticker printed on webpage
    page_ticker <- test_url %>% 
        xml_nodes("h1.h2") %>% html_text() %>% 
        str_extract("(?<=\\()[A-Z]{1,10}(?=\\))")
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
    #####
    
    dir_w <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/data/"
    download_date <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")
    
    for(ticker in tickers) {
        
        #####
        # ticker <- "DFDSE"
        # ticker <- "BZUN"
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


# Get downloaded files
fund_files <- list.files("data", 
                         pattern = "balance_sheet|income_statement|cash_flow", 
                         full.names = TRUE)

tickers_in_files <- fund_files %>% str_extract("(?<=\\/)[A-Za-z]{1,20}(?= -)")
# Proportion of tickers downloaded (at least once)
length(tickers_in_files) / length(tickers_with_clean_prices)


# length(tickers_with_clean_prices) # 13,763
tickers_with_clean_prices %>% str_detect("ARPO") %>% which()
tickers_with_clean_prices %>% str_detect("BILI") %>% which()
tickers_with_clean_prices %>% str_detect("^H$") %>% which()
tickers_with_clean_prices %>% str_detect("^OFIX$") %>% which()

# Stops at "AEHL", "ARPO" and "BILI", "OFIX", "PDD", "REDU"

# [1] "OFIX"
# Error: `cols` must select at least one column.
# Run `rlang::last_error()` to see where the error occurred.
# In addition: There were 50 or more warnings (use warnings() to see the first 50)


step <- 1000
start <- seq(from = 1, to = length(tickers_with_clean_prices), by = step)
end <- start + step - 1
end[length(end)] <- min(length(tickers_with_clean_prices), end[length(end)])


start <- start[c(3, 5, 6, 7)]
end <- end[c(3, 5, 6, 7)]

# Download fundamentals
for(i in seq_along(start)) {
    save_fundamentals(tickers = tickers_with_clean_prices[start[i]:end[i]])
    print(paste0(start[i], "-", end[i], " done!"))
} 


# Get percent downloaded per group
tbl <- tibble(group = integer(), perc_compl = numeric())
for(i in seq_along(start)) {
    tickers <- tickers_with_clean_prices[start[i]:end[i]]
    tbl_i <- tibble(group = i, perc_compl = mean(tickers %in% tickers_in_files))
    tbl <- bind_rows(tbl, tbl_i)
}
tbl









