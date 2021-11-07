
# Graphfundamentals.com

library(rvest)

# https://graphfundamentals.com/graphfundamentals/AAPL/IS#




tickers <- c("AAPL", "HD", "BBY")

scrape_fundamentals <- function(ticker) {
    
    #####
    # ticker <- "AAPL"
    #####
    
    bs_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                  str_to_upper(ticker), "/BS#")
    
    is_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/IS#")
    
    cf_url <- paste0("https://graphfundamentals.com/graphfundamentals/",
                     str_to_upper(ticker), "/CF#")
    
    bs_page <- read_html(bs_url) %>% html_table()
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
        mutate(date = as.Date(date, "X%Y.%m.%d"))    
    
    
    is_df <- is_page %>% 
        flatten() %>% 
        as_tibble(.name_repair = make.names) %>% 
        .[, colSums(is.na(.)) < nrow(.)] %>% 
        rename(field = X) %>% 
        pivot_longer(-field, names_to = "date", values_to = "value") %>% 
        pivot_wider(names_from = field, values_from = value) %>% 
        janitor::clean_names() %>% 
        mutate(date = as.Date(date, "X%Y.%m.%d"))    
    
    cf_df <- cf_page %>% 
        flatten() %>% 
        as_tibble(.name_repair = make.names) %>% 
        .[, colSums(is.na(.)) < nrow(.)] %>% 
        rename(field = X) %>% 
        pivot_longer(-field, names_to = "date", values_to = "value") %>% 
        pivot_wider(names_from = field, values_from = value) %>% 
        janitor::clean_names() %>% 
        mutate(date = as.Date(date, "X%Y.%m.%d"))    
    
    return(list(balance_sheet = bs_df,
                income_statement = is_df,
                cash_flows = cf_df))
    
}

fundamentals <- scrape_fundamentals("HD")






