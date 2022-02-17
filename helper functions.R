




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