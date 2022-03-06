




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
    x <- bs_files_raw[91] %>% add_download_date() %>% clean_field_names() %>%
      consolidate_bs_field_names() %>% pull(field)
    required_prior_string <- "accounts_payable"
    # required_prior_string <- NULL
    required_posterior_string <- "total_current_liabilities"
    current_string <- "deferred_revenue"
    replacement <- "current_deferred_revenue"
    #########

    if(is.null(current_string)) return("Please provide a current_string!")
    if(is.null(replacement)) return("Please provide a replacement_string!")
    
    id_prior_field <- 
        str_detect(x, pattern = paste0("^", required_prior_string, "$")) %>% 
        which() %>% first()
    
    id_posterior_field <- 
        str_detect(x, pattern = paste0("^", required_posterior_string, "$")) %>%
        which() %>% first()
    
    id_current_string <- str_detect(x, pattern = paste0("^", current_string, "$")) %>% which() %>% first()
    if(is.na(id_current_string)) stop("current_string not found!")
    
    if(!is.na(id_prior_field) & is.na(id_posterior_field) & 
       (id_current_string < id_posterior_field)) {
        return(str_replace(x, current_string, replacement))
    }
    if(is.na(id_prior_field) & !is.na(id_posterior_field) & 
       (id_current_string > id_prior_field)) {
        return(str_replace(x, current_string, replacement))
    }
    if(is.na(id_prior_field) & is.na(id_posterior_field) & 
       (id_current_string > id_prior_field) & 
       (id_current_string < id_posterior_field)) {
        return(str_replace(x, current_string, replacement))
    }
}

