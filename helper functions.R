
library(tidyverse)

first <- dplyr::first
between <- dplyr::between
filter <- dplyr::filter

# Add download date column
add_download_date <- function(file_path) {
    #####
    # file_path <- is_files_raw[1]
    #####    
    tbl <- read_tibble(file_path)
    
    if(!"download_date" %in% colnames(tbl)) {
        download_date <- file_path %>% 
            str_extract("(?<=\\()[0-9]{4} [0-9]{1,2} [0-9]{1,2}(?=\\))") %>% 
            as.Date("%Y %m %d")
        tbl <- tbl %>%
            add_column(download_date = download_date)
    }
    tbl
}
# is_files_raw[1] %>% add_download_date()


# Check periodicity
has_quarterly_dates <- function(data) {
    dates_are_quarterly <- 
        data %>% 
        colnames() %>% 
        str_subset("x[0-9]{4}_[0-9]{2}_[0-9]{2}") %>% 
        str_remove_all("^x") %>% 
        as.Date("%Y_%m_%d") %>% 
        diff() %>% 
        abs() %>% 
        as.numeric() %>% 
        between(80, 100) %>% 
        all()
    return(dates_are_quarterly)
}

# is_files_raw[1] %>% add_download_date() %>% has_quarterly_dates()

# Analyze field raw names -------------------------------------------------
get_and_save_field_names <- function(files, what = "is") {
    #####
    # files <- is_files_raw[1:5]
    # what <- "is"
    #####
    
    file_data <- map(files, read_tibble)
    tickers <- map(file_data, ~.x %>% pull(ticker) %>% unique)
    fields <- map(file_data, ~.x %>% pull(field))
    names(fields) <- tickers
    
    saveRDS(fields, paste0("data/temp/", what, "_", "fields.rds"))
}


clean_field_names <- function(df) {
    ######
    # df <- bs_files_raw[64] %>% add_download_date()
    ######
    df %>%
        mutate(
            field = field %>% 
                str_to_lower() %>%
                str_remove_all("see note [A-Za-z]{1,2}") %>% 
                str_remove_all("see note \\d{1,2}") %>%
                str_remove_all("note \\d{1,2}") %>% 
                str_remove_all("note [A-Za-z]{1,2}") %>% 
                str_replace_all(" ", "_") %>%
                str_remove_all("\\([A-Za-z]\\)") %>% 
                str_replace_all("\\,", "") %>% 
                str_replace_all("\\(", "_") %>% 
                str_replace_all("\\)", "_") %>%
                str_replace_all("\\.", "_") %>%
                str_replace_all("\\&", "and") %>%
                str_replace_all("\\-", "_") %>%
                str_replace_all("\\:", "_") %>%
                str_replace_all("\\-", "_") %>%
                str_replace_all("\\/", "_") %>%
                str_remove_all("_$") %>%
                str_replace_all("\\$", "_") %>%
                str_replace_all("\\;", "_") %>%
                str_remove_all("^_") %>%
                str_remove_all("\\[") %>%
                str_remove_all("\\]") %>%
                str_replace_all("\\'", "_") %>% 
                str_replace_all("__", "_") %>%
                str_replace_all("__", "_") %>% 
                str_replace_all("__", "_") %>% 
                str_remove_all("_$")
        )
}





# !!!! make function that appends a number to all duplicates so as to
# indicate that is not unique

append_number_dups <- function(data) {
    
    ####################    
    # tmp <-
    #     tribble(~field, ~value,
    #             "revenue", 20233,
    #             "gross_profit", 10339,
    #             "revenue", 1309)
    ####################
    
    # Loop through each field
    fields <- tmp %>% pull(field)
    
    for(field in fields) {
        # field <- "revenue"
        
        field_matches <- which(fields == field)
        
        field_ids <- tibble(
            field_matches = field_matches, 
            field_seq = seq_along(field_matches)
        )
        
        if(nrow(field_ids) > 1) {
            for(i in seq_along(field_ids)) {
                # i <- 1
                tmp[pull(field_ids, field_matches)[i], "field"] <- 
                    paste0(field, "_", i)
            }
        }
    }
    return(tmp)
}



delete_old_files <- function(pattern, dir, step = 4) {
    ######
    # pattern <- "profile_data_consolidated"
    # dir <- "data/cleaned data"
    # dir <- "data"
    ###########
    # Keep evey 'step'th file
    ###########
    # step <- 4
    ######
    
    files_raw_v1 <- 
        list.files(dir, pattern = paste0({{ pattern }}, " \\d{4}_\\d{2}_\\d{2}"), 
                   full.names = TRUE)
    
    files_raw_v2 <- 
        list.files(dir, pattern = paste0({{ pattern }}, " \\(\\d{4} \\d{2} \\d{2}\\)"), 
                   full.names = TRUE)
    
    files_raw <- c(files_raw_v1, files_raw_v2) %>% sort()
    
    file_dates_p1 <- files_raw %>% str_extract("\\d{4} \\d{2} \\d{2}")
    file_dates_p2 <- files_raw %>% str_extract("\\d{4}_\\d{2}_\\d{2}") %>%
        str_replace_all("_", " ")
    
    file_dates_p1[is.na(file_dates_p1)] <- file_dates_p2[!is.na(file_dates_p2)]
    
    file_dates <- 
        unique(file_dates_p1) %>% 
        as.Date("%Y %m %d") %>% 
        sort()
    
    if(length(file_dates) < 10)
        return("Cancelled - Less than 10 unique dates exist.")
    
    date_seq_keep <-
        seq(from = 1, to = length(file_dates), by = step) %>% 
        c(., length(file_dates) - 0:1) %>% unique() %>% sort()
    
    date_seq_del <-
        setdiff(seq_along(file_dates), date_seq_keep)
    
    dates_to_del <- 
        file_dates[date_seq_del] %>% 
        str_replace_all("-", " ")
    
    files_to_del <- files_raw[which(file_dates_p1 %in% dates_to_del)] %>% sort()
    
    file.copy(from = files_to_del, to = "C:/Users/user/Documents/Temporary Recycle Bin")
}

# If a field name follows another specified field name, then rename the field name accordingly (e.g., if the field name is "operating_lease_liabilities" and it follows "total_current_liabilities", then we can assume it's long-term so rename it "long_term_operating_lease_liabilities")
rename_field_with_prior <- function(x, required_prior_string, current_string, replacement) {
    #########
    # x <- bs_files_raw[27] %>% add_download_date() %>% clean_field_names() %>%
    #   consolidate_bs_field_names() %>% pull(field)
    # required_prior_string <- "total_current_liabilities"
    # current_string <- "operating_lease_liabilities"
    # replacement <- "long_term_lease_liabilities"
    #########
    
    id_prior_field <- str_detect(x, pattern = paste0("^", required_prior_string, "$")) %>% which() %>% dplyr::first()
    if(is.na(id_prior_field)) return(x)
    id_current_string <- str_detect(x, pattern = paste0("^", current_string, "$")) %>% which() %>% dplyr::first()
    if(is.na(id_current_string)) return(x)
    
    
    if(id_current_string > id_prior_field) {
        str_replace(x, current_string, replacement)
    } else {
        return(x)
    }
}


rename_field_conditionally <- function(x, 
                                       required_prior_string = NULL,
                                       required_posterior_string = NULL, 
                                       current_string = NULL, 
                                       replacement = NULL) {
    #########
    # x <- bs_cleaned[119] %>% setNames(NULL) %>% .[[1]] %>% pull(field)
    # required_prior_string <- NULL
    # required_posterior_string <- NULL
    # current_string <- NULL 
    # replacement <- NULL
    # required_prior_string <- "total_current_liabilities"
    # current_string <- "operating_lease_liabilities"
    # replacement <- "long_term_lease_liabilities"
    #########

    if(is.null(current_string)) stop("Please provide a current_string!")
    if(is.null(replacement)) stop("Please provide a replacement_string!")
    
    id_prior_field <- 
        str_detect(x, pattern = paste0("^", required_prior_string, "$")) %>% 
        which() %>% first()
    
    id_posterior_field <- 
        str_detect(x, pattern = paste0("^", required_posterior_string, "$")) %>%
        which() %>% first()
    
    id_current_string <- 
        str_detect(x, pattern = paste0("^", current_string, "$")) %>% 
        which() %>% first()
    if(is.na(id_current_string) || length(id_current_string) == 0) return(x)
    
    if((length(id_posterior_field) != 0) & (length(id_prior_field) != 0)) {
      if(!is.na(id_prior_field) & !is.na(id_posterior_field) &
         (id_current_string > id_prior_field) & 
         (id_current_string < id_posterior_field)) {
          x[id_current_string] <- replacement
      }
    }
    if((length(id_prior_field) != 0) & (length(id_posterior_field) != 0)) {
      if(!is.na(id_prior_field) & is.na(id_posterior_field) & 
         (id_current_string > id_prior_field)) {
          x[id_current_string] <- replacement
      }
    }
    if((length(id_prior_field) != 0) & (length(id_posterior_field) != 0)) {
      if(is.na(id_prior_field) & !is.na(id_posterior_field) & 
         (id_current_string < id_posterior_field)) {
          x[id_current_string] <- replacement
      }
    }
    return(x)
}



rename_total_assets_if_missing <- function(x) {
    #########
    # x <- bs_files_raw[91] %>% add_download_date() %>%
    #     clean_field_names() %>% pull(field)
    #########
    
    if(!str_detect(x, "^total_assets$") %>% any() & 
       str_detect(x, "^total_liabilities_and_stockholders_equity$") %>% any()) {
      x <- str_replace(x, 
                       "^total_liabilities_and_stockholders_equity$",
                       "total_assets")
    }
    if(!str_detect(x, "^total_assets$") %>% any() & 
       str_detect(x, "^total_liabilities_and_shareholders_equity$") %>% any()) {
        x <- str_replace(x, 
                         "^total_liabilities_and_shareholders_equity$",
                         "total_assets")
    }
    return(x)
}


# If there are duplicate "net_income" fields, keep the last one
remove_leading_duplicates <- function(x, field = NULL) {
    #########
    # x <- is_cleaned %>% .[names(.) == "ITT"] %>% setNames(NULL) %>% .[[1]] %>%
    #     consolidate_is_field_names()
    # field <- "net_income"
    #########
    
    if(is.null(field)) stop("Provide a field")
    
    fields <- x %>% pull(field)
    
    dup_ids <- str_detect(fields, paste0("^", field, "$")) %>% which()
    if(length(dup_ids) %in% c(0, 1)) return(x)   
    
    if(length(dup_ids) > 1) {
        dups_to_remove <- dup_ids[-length(dup_ids)]
        return(x %>% slice(-dups_to_remove))
    }
}

# is_cleaned %>% .[names(.) == "ITT"] %>% setNames(NULL) %>% .[[1]] %>%
#         consolidate_is_field_names() %>%
#         remove_leading_duplicates(., field = "net_income") %>%
#     pull(field)






split_basic_and_diluted_eps <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "SLDB"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    fields <- x %>% pull(field)
    
    if(!"basic_and_diluted_eps" %in% fields) return(x)
    field_id <- str_detect(fields, "^basic_and_diluted_eps$") %>% which()
    if((field_id %>% length()) > 1) return(x)

    basic_and_diluted_eps <- slice(x, field_id)
    x_adj <- slice(x, -field_id)
    
    cleaned_eps <- rbind(basic_and_diluted_eps, basic_and_diluted_eps) %>% 
        mutate(field = c("basic_eps", "diluted_eps"))
        
    return(x_adj %>% rbind(cleaned_eps))
}

# split_basic_and_diluted_eps(x) %>% View()


split_basic_and_diluted_shares <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "RCAT"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    fields <- x %>% pull(field)
    
    if(!"basic_and_diluted_shares" %in% fields) return(x)
    field_id <- str_detect(fields, "^basic_and_diluted_shares$") %>% which()
    if((field_id %>% length()) > 1) return(x)
    
    basic_and_diluted_shares <- slice(x, field_id)
    x_adj <- slice(x, -field_id)
    
    cleaned_shares <- rbind(basic_and_diluted_shares, basic_and_diluted_shares) %>% 
        mutate(field = c("basic_shares", "diluted_shares"))
    
    return(x_adj %>% rbind(cleaned_shares))
}
 
# x %>% 
#     split_basic_and_diluted_eps() %>% 
#     split_basic_and_diluted_shares() %>% 
#     View()
   
    
# If there is one amortization field and one depreciation field and no amortization_depreciation field,
# then combine them and make depreciation_amortization
consolidate_dep_amor <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "BCOR"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    if(is.null(x)) return(x)
    ticker <- x %>% pull(ticker) %>% first()
    fields <- x %>% pull(field)
    download_date <- x %>% pull(download_date) %>% first()
    
    if (str_detect(fields, "^depreciation_amortization$") %>% any())
        return(x)
    
    dep_id <- str_detect(fields, "^depreciation$") %>% which()
    amor_id <- str_detect(fields, "^amortization$") %>% which()
    
    if (length(dep_id) != 1 | length(amor_id) != 1) # If there is onlly one dep field and one amort field, then combine
        return(x)
    if (dep_id != amor_id) {
        dep_amor <-
            x %>% slice(c(dep_id, amor_id)) %>%
            summarize(across(-c(ticker, field, download_date), ~ sum(.x, na.rm = TRUE))) %>%
            add_column(ticker = ticker,
                       field = "depreciation_amortization",
                       download_date = download_date) %>%
            select(ticker, field, everything()) %>%
            select(everything(), download_date)
        return(x %>% slice(-dep_id, -amor_id) %>% rbind(dep_amor))
    }
}

# consolidate_dep_amor(x) %>% View()


consolidate_sga <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "QUMU"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    if(is.null(x)) return(x)
    ticker <- pull(x, ticker) %>% first()    
    fields <- pull(x, field)
    download_date <- pull(x, download_date) %>% first()
    
    if(str_detect(fields, "^selling_general_administrative$") %>% any()) return(x)
    
    sell_id <- str_detect(fields, "^selling_expense$") %>% which()
    admin_id <- str_detect(fields, "^general_administrative_expense$") %>% which()
    
    if(length(sell_id) != 1 | length(admin_id) != 1) return(x)
    if(sell_id != admin_id) {
        sga <-
            x %>% slice(c(sell_id, admin_id)) %>%
            summarize(across(-c(ticker, field, download_date), ~sum(.x, na.rm = TRUE))) %>% 
            add_column(
                ticker = ticker,
                field = "selling_general_administrative",
                download_date = download_date
            ) %>% 
            select(ticker, field, everything()) %>% 
            select(everything(), download_date)
        return(x %>% slice(-sell_id, -admin_id) %>% rbind(sga))
    }
}

# consolidate_sga(x) %>% View()

consolidate_selling_expense <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "QUMU"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names() %>% 
        # filter(field != "general_administrative_expense")
    ########
    
    if(is.null(x)) return(x)
    ticker <- pull(x, ticker) %>% first()    
    fields <- pull(x, field)
    download_date <- pull(x, download_date) %>% first()
    
    if(str_detect(fields, "administrative") %>% any()) return(x)
    
    sell_id <- str_detect(fields, "^selling_expense$") %>% which()
    
    if(length(sell_id) != 1) {
        return(x)  
    } else {
        return(
            x %>%
                mutate(field = str_replace_all(field, "selling_expense", "selling_general_administrative"))
        )
    }
}

# consolidate_selling_expense(x) %>% pull(field)

# Assumes that the only "selling_general_administrative" components are "selling_expense" and "general_administrative_expense" 
# (what about research_and_development?)
# "R&D costs are not included in SGA" (-Investopedia)


consolidate_general_administrative <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "QUMU"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names() %>%
        # filter(field != "selling_expense")
    ########
    
    if(is.null(x)) return(x)
    ticker <- pull(x, ticker) %>% first()    
    fields <- pull(x, field)
    download_date <- pull(x, download_date) %>% first()
    
    if(str_detect(fields, "selling") %>% any()) return(x)
    
    admin_id <- str_detect(fields, "^general_administrative_expense$") %>% which()
    
    if(length(admin_id) != 1) {
        return(x)  
    } else {
        return(
            x %>%
                mutate(field = str_replace_all(field, "general_administrative_expense", "selling_general_administrative"))
        )
    }
}

# consolidate_general_administrative(x) %>% pull(field)



consolidate_amor <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "QUMU"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    if(is.null(x)) return(x)
    ticker <- x %>% pull(ticker) %>% first()
    fields <- x %>% pull(field)
    download_date <- x %>% pull(download_date) %>% first()
    
    if (str_detect(fields, "depreciation") %>% any())
        return(x)
    
    amor_id <- str_detect(fields, "^amortization$") %>% which()
    
    if (length(amor_id) != 1 ) {
        return(x)
    } else {
        amor <-
            x %>% slice(amor_id) %>% 
            mutate(field = "depreciation_amortization")
        
        return(x %>% slice(-amor_id) %>% rbind(amor))
    }
}

# consolidate_amor(x) %>% pull(field)

consolidate_dep <- function(x) {
    ########
    # x <- is_cleaned %>% .[names(.) == "QUMU"] %>% setNames(NULL) %>% .[[1]] %>% consolidate_is_field_names()
    ########
    
    if(is.null(x)) return(x)
    ticker <- x %>% pull(ticker) %>% first()
    fields <- x %>% pull(field)
    download_date <- x %>% pull(download_date) %>% first()
    
    if (str_detect(fields, "amortization") %>% any())
        return(x)
    
    dep_id <- str_detect(fields, "^depreciation$") %>% which()
    
    if (length(dep_id) != 1 ) {
        return(x)
    } else {
        dep <-
            x %>% slice(dep_id) %>% 
            mutate(field = "depreciation_amortization")
        
        return(x %>% slice(-dep_id) %>% rbind(dep))
    }
}





# if basic_shares and diluted_shares exist and are equal, but only diluted_shares or basic_shares exist, create the other


# If this pattern exists and related fields don't already exist, 
# split the second field and create shares_basic, shares_diluted
# [19] "basic_and_diluted_eps"                                
# [20] "number_of_shares_outstanding" 




get_chrome_driver <- function() {
    driver <- RSelenium::rsDriver(
        port = 9468L,
        browser = "chrome",
        chromever =
            "Version 100.0.4896.127 (Official Build) (64-bit)" %>%
            str_extract("(?<=Version )\\d+\\.\\d+\\.\\d+\\.") %>%
            str_replace_all("\\.", "\\\\.") %>%
            paste0("^", .) %>%
            str_subset(
                string = binman::list_versions(appname = "chromedriver") %>%
                    dplyr::last()
            ) %>%
            as.numeric_version() %>%
            max() %>%
            as.character()
    )
    return(driver[["client"]])
}


# Handle modal
click_modal <- function() {
    suppressMessages(tryCatch({
        modal_element <- remote_driver$findElement(using = "xpath",
                                                   value = '//*[@id="graphFundamentalsModal"]/div[2]/footer/button')
    },
    silent = TRUE, error = function(e) {
    }))
    
    if (exists("modal_element")) {
        modal_element$clickElement()
    }
}

# Handle Login
login <- function(login_details) {
  
  email_element <- remote_driver$findElement(using = "xpath",
                                             value = "/html/body/section/div[2]/div/div/div/form/div[1]/div/input")
  
  email_element$sendKeysToElement(list(login_details$email))
  
  
  pw_element <- remote_driver$findElement(using = "xpath",
                                          value = "/html/body/section/div[2]/div/div/div/form/div[2]/div/input")
  
  pw_element$sendKeysToElement(list(login_details$pw))
  
  remember_element <- remote_driver$findElement(using = "xpath",
                                                value = "/html/body/section/div[2]/div/div/div/form/div[3]/label/input")
  remember_element$clickElement()
  
  button_element <- remote_driver$findElement(using = "xpath",
                                              value = "/html/body/section/div[2]/div/div/div/form/button")
  button_element$clickElement()
}

find_element_xpath <- function(driver, value) {
  driver$findElement(using =  "xpath", 
                     value = value)
}

find_element_selector <- function(driver, value) {
  driver$findElement(using =  "css selector", 
                     value = value)
}

#annualFilingFrequencyTabLi




get_profile_data <- function() {
  
  # Get company name
  name_element <- remote_driver$findElement(using = "xpath",
                                            value = '//*[@id="tableTitle"]')
  business_name <- name_element$getElementText() %>% .[[1]]
  
  # Get SIC industry
  SIC_element <- find_element_xpath(remote_driver, '//*[@id="sicClassification"]')
  SIC <- 
    SIC_element$getElementText() %>% 
    unlist() %>% 
    str_extract("(?<=ion\\: ).*")
  
  ind_group_element <- find_element_xpath(remote_driver, '//*[@id="industryGroup"]')
  industry_group <- 
    ind_group_element$getElementText() %>% 
    unlist() %>% 
    str_extract("(?<=Group\\: ).*")
  
  industry_element <- find_element_xpath(remote_driver, '//*[@id="industry"]')
  industry <- 
    industry_element$getElementText() %>% 
    unlist() %>% 
    str_extract("(?<=Industry\\: ).*")
  
  sector_element <- find_element_xpath(remote_driver, '//*[@id="division"]')
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