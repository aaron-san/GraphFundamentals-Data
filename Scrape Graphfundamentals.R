
# Graphfundamentals.com

library(tidyverse)
library(rvest)
library(data.table)
library(RCurl)
library(httr)

source("helper functions.R")
source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")
# 
# library(wdman)
# library(RSelenium)
# selServ <- selenium(jvmargs = c("-Dwebdriver.chrome.verboseLogging=true"))
# remDr <- remoteDriver(port = 4567L, browserName = "chrome")
# remDr$open()
# selServ$log()
# 
# 
# 
# library(RSelenium);library("httr");library(wdman)
# selCommand<-wdman::selenium(jvmargs = c("-Dwebdriver.chrome.verboseLogging=true"),
#                             retcommand = TRUE)
# remDr <- remoteDriver(port = 4567L, browserName = "chrome")
# remDr$open()
# 
# # remotes::install_github("ropensci/RSelenium#237")
# 
# binman::list_versions("chromedriver")
# driver <- rsDriver(browser=c("chrome"), 
#                    chromever="100.0.4896.127")
# 
# 
# RSelenium::rsDriver(browser = "chrome",
#                     chromever = "latest_compatible")
# 
# available.versions <- binman::list_versions("chromedriver") 
# latest.version <- available.versions$win32[length(available.versions)]
# rsDriver(chromever = binman::list_versions("chromedriver")$win32[4])
# rsDriver(chromever = binman::list_versions("chromedriver")$win32[length(available.versions$win32)])
# 
# wdman::selenium(port = 4444L,
#                 check=FALSE, retcommand = TRUE) %>%
#     system(wait=FALSE, invisible=FALSE)
# 
# rmDrv = remoteDriver(extraCapabilities = list(marionette = TRUE),
#                      browserName="chrome", port = 4444L)
# rmDrv$open()
# 
# rmDrv$navigate("https://www.google.com")
# 
# rmDrv$close()




# library(tidyverse)    
library(RSelenium)

# Get login credentials
login_details <- read_tibble("private/wsm_pw.csv")


# Connect to the FireFox driver
ff_driver <- rsDriver(browser = "firefox") #, extraCapabilities = cprof)
remote_driver <- ff_driver[["client"]]


# html <- remote_driver$getPageSource()[[1]]




# Navigate to the login page
remote_driver$navigate("https://wallstreetmillennial.com/login")
Sys.sleep(3)
# click_modal()
# Sys.sleep(3)
login(login_details = login_details)


# https://graphfundamentals.com/graphfundamentals/AAPL/IS#

scrape_fundamentals <- function(ticker, driver) {
    
    # driver <- remote_driver
    
    #####
    # ticker <- "MSFT"
    # ticker <- "F"
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
    
    bs_url <- paste0(ticker_url, "&stmt=BS")
    is_url <- paste0(ticker_url, "&stmt=IS")
    cf_url <- paste0(ticker_url, "&stmt=CF")
    
    # install.packages("RSelenium")
    # If the webpage has an error 500 message, return NA
    if(status_code(GET(bs_url)) == 500) return(NA)
    if(status_code(GET(is_url)) == 500) return(NA)
    if(status_code(GET(cf_url)) == 500) return(NA)
    
    
    # Navigate to the ticker's page
    driver$navigate(is_url)
    driver$setTimeout(type = "page load", milliseconds = 10000)
    # Sys.sleep(2.5)
    
    # Get ticker printed on webpage
    ticker_element <- driver$findElement(using = "xpath",
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
    
    #########################
    # Get profile data
    #########################
    # profile_data <- get_profile_data()
    
    get_profile_data <- function(driver) {
        
        # driver <- remote_driver
        
        driver$navigate(is_url)
        
        # Get company name
        name_element <- driver$findElement(using = "xpath",
                                                  value = '//*[@id="tableTitle"]')
        business_name <- name_element$getElementText() %>% .[[1]]
        
        # Get SIC industry
        SIC_element <- find_element_xpath(driver, '//*[@id="sicClassification"]')
        SIC <- 
            SIC_element$getElementText() %>% 
            unlist() %>% 
            str_extract("(?<=ion\\: ).*")
        
        ind_group_element <- find_element_xpath(driver, '//*[@id="industryGroup"]')
        industry_group <- 
            ind_group_element$getElementText() %>% 
            unlist() %>% 
            str_extract("(?<=Group\\: ).*")
        
        industry_element <- find_element_xpath(driver, '//*[@id="industry"]')
        industry <- 
            industry_element$getElementText() %>% 
            unlist() %>% 
            str_extract("(?<=Industry\\: ).*")
        
        sector_element <- find_element_xpath(driver, '//*[@id="division"]')
        sector <- 
            sector_element$getElementText() %>% 
            unlist() %>% 
            str_extract("(?<=Sector\\: ).*")    
        
        return(tibble(business_name = business_name,
                      SIC = SIC,
                      industry_group = industry_group,
                      industry = industry,
                      sector = sector))
    }
    
    profile_data <- get_profile_data(driver = driver)

    
    get_table_data <- function(driver, statement, period) {
        
        # driver <- remote_driver
        # statement <- "income_statement"
        # period <- "quarterly"
        
        driver$navigate(is_url)
        
        statement_xpath <- switch(statement,
                                  "income_statement" = '/html/body/section/div[2]/div[1]/div/div[1]/div/aside/ul/li[1]/a',
                                  "balance_sheet" = '/html/body/section/div[2]/div[1]/div/div[1]/div/aside/ul/li[2]/a',
                                  "cash_flows" = '/html/body/section/div[2]/div[1]/div/div[1]/div/aside/ul/li[3]/a')
        
        period_selector <- switch(period,
                             "quarterly" = '#quarterlyFilingFrequencyTabLi',
                             "yearly" = '#annualFilingFrequencyTabLi')
        
        is_link_element <- find_element_xpath(driver, statement_xpath)
        Sys.sleep(1)
        is_link_element$clickElement()

        # Function to get current url and see if corresponds to "statement" input
        
        validate_url <- function(driver) {
        
            # driver <- remote_driver
            
            driver$setTimeout(type = "page load", milliseconds = 10000)
            stmt <- switch(statement,
                           "income_statement" = "IS",
                           "balance_sheet" = "BS",
                           "cash_flows" = "CF")
        
            current_url <- driver$getCurrentUrl()
            if(!current_url %>% str_detect(paste0("\\=", stmt))) {
                print("URL problem")
                return(NA)
            }
        }
        
        validate_url(driver = driver)
        
        
        link_element <- find_element_selector(driver = driver, period_selector)
        Sys.sleep(3)
        link_element$clickElement()
        Sys.sleep(2.5)
        table_element <- find_element_selector(driver, '.table-container')
        
        table_data <-
            table_element$getElementAttribute('innerHTML')[[1]] %>% 
            read_html() %>%
            html_table() %>% .[[1]]
        
        
        # Check dates
        validate_dates <- function(df, statement, period) {
            
            # df <- table_data
            # statement <- statement
            # period <- "quarterly"
            
            date_diffs_q <- colnames(df) %>% 
                .[2:length(.)] %>% 
                as.Date() %>% diff() %>% as.integer() %>% 
                abs()
            
            if(statement %in% c("income_statement", "cash_flows")) {
                if(period == 'quarterly') {
                    if(!mean(dplyr::between(date_diffs_q, 80, 100)) > 0.8) {
                        print("quarterly dates not correct")
                        return(NA)
                    }
                } else
                    
                    if(period == 'yearly') {
                        if(!mean(dplyr::between(date_diffs_q, 360, 370)) > 0.8) {
                            print("yearly dates not correct")
                            return(NA)
                        }
                    }    
            }    
        }

        # If the data contains all NAs, return NA
        if(table_data %>% is.na() %>% all()) {
            print("Table has all NAs")
            return(NA)
        }
        
        return(table_data)
    }
    
    is_quarterly <- get_table_data(
        driver = driver,
        statement = "income_statement",
        period = "quarterly")
    
    is_yearly <- get_table_data(
        driver = driver,
        statement = "income_statement",
        period = "yearly")
    
    bs_quarterly <- get_table_data(
        driver = driver,
        statement = "balance_sheet",
        period = "quarterly"
    )
    
    bs_yearly <- get_table_data(
        driver = driver,
        statement = "balance_sheet",
        period = "yearly"
    )
        
    cf_quarterly <- get_table_data(
        driver = driver,
        statement = "cash_flows",
        period = "quarterly"
    )
    
    cf_yearly <- get_table_data(
        driver = driver,
        statement = "cash_flows",
        period = "yearly"
    )
        

    # if(bs_tibble %>% nrow() == 0 | bs_tibble %>% ncol() <= 2) {
    #     bs_df <- NA 
    # } else {
        
    clean_df <- function(df) {
        
        # df <- is_quarterly
    
        if(all(is.na(df))) return(NA)
            
        df %>% 
            janitor::clean_names() %>% 
            janitor::remove_empty(which = "cols") %>% 
            rename(field = fields) %>% 
            mutate(across(-field, ~str_remove_all(.x, ","))) %>%
            mutate(across(-field, as.numeric)) %>% 
            add_column(ticker = ticker) %>%
            select(ticker = ticker, field, everything())
    }
    
    is_quarterly_cleaned <- clean_df(is_quarterly)
    is_yearly_cleaned <- clean_df(is_yearly)
    bs_quarterly_cleaned <- clean_df(bs_quarterly)
    bs_yearly_cleaned <- clean_df(bs_yearly)
    cf_quarterly_cleaned <- clean_df(cf_quarterly)
    cf_yearly_cleaned <- clean_df(cf_yearly)
            
            

    return(list(is_quarterly = is_quarterly_cleaned,
                is_yearly = is_yearly_cleaned,
                bs_quarterly = bs_quarterly_cleaned,
                bs_yearly = bs_yearly_cleaned,
                cf_quarterly = cf_quarterly_cleaned,
                cf_yearly = cf_yearly_cleaned,
                profile = profile_data))
}


ticker_data <- scrape_fundamentals(driver = remote_driver, ticker = "MSFT")


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
        
        ticker_data <- scrape_fundamentals(ticker)
        
        if(!is.na(ticker_data)) {
            
            if(!is.na(ticker_data$profile)) {
                fwrite(ticker_data$profile, 
                       paste0(dir_w, ticker, " - profile ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$is_quarterly_cleaned)) {
                fwrite(ticker_data$is_quarterly_cleaned, 
                       paste0(dir_w, ticker, " - income_statement_quarterly ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$is_yearly_cleaned)) {
                fwrite(ticker_data$is_yearly_cleaned, 
                       paste0(dir_w, ticker, " - income_statement_yearly ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$bs_quarterly_cleaned)) {
                fwrite(ticker_data$bs_quarterly_cleaned, 
                       paste0(dir_w, ticker, " - balance_sheet_quarterly ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$bs_yearly_cleaned)) {
                fwrite(ticker_data$bs_yearly_cleaned, 
                       paste0(dir_w, ticker, " - balance_sheet_yearly ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$cf_quarterly_cleaned)) {
                fwrite(ticker_data$cf_quarterly_cleaned, 
                       paste0(dir_w, ticker, " - cash_flow_quarterly ", 
                              download_date, ".csv"))
            }
            
            if(!is.na(ticker_data$cf_yearly_cleaned)) {
                fwrite(ticker_data$cf_yearly_cleaned,
                       paste0(dir_w, ticker, " - cash_flow_yearly ", 
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


