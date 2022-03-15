


conflicted::conflict_prefer_all("dplyr", quiet = TRUE)

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
    
    id_prior_field <- str_detect(x, pattern = paste0("^", required_prior_string, "$")) %>% which() %>% first()
    if(is.na(id_prior_field)) return(x)
    id_current_string <- str_detect(x, pattern = paste0("^", current_string, "$")) %>% which() %>% first()
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
    # x <- bs_files_raw[91] %>% add_download_date() %>% clean_field_names() %>% pull(field)
    # required_prior_string <- "accounts_payable"
    # # required_prior_string <- NULL
    # required_posterior_string <- "total_current_liabilities"
    # current_string <- "deferred_revenue"
    # replacement <- "current_deferred_revenue"
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
    if(is.na(id_current_string)) return(x)
    
    if(!is.na(id_prior_field) & !is.na(id_posterior_field) & 
       (id_current_string > id_prior_field) & 
       (id_current_string < id_posterior_field)) {
        x[id_current_string] <- replacement
    }
    if(!is.na(id_prior_field) & is.na(id_posterior_field) & 
       (id_current_string > id_prior_field)) {
        x[id_current_string] <- replacement
    }
    if(is.na(id_prior_field) & !is.na(id_posterior_field) & 
       (id_current_string < id_posterior_field)) {
        x[id_current_string] <- replacement
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





