#######################
# Clean fundamentals
#######################
# - Add download_date
# - Reformat dates, numbers, etc.
# - Combine multiple tickers into big files

library(tidyverse)
library(data.table)
source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")

dir_proj <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data"

today <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")

# List files
#  - Include the most recent cleaned income_statement files (will have many 
#    duplicates)
raw_files <- 
    list.files(paste0(dir_proj, "/data/raw data"), 
               pattern = "income_statement|balance_sheet|cash_flow",
               full.names = TRUE)
cleaned_files <- 
    list.files(paste0(dir_proj, "/data/cleaned data"), 
               pattern = "income_statement|balance_sheet|cash_flow",
               full.names = TRUE)
# combined_files <- sort(unique(c(raw_files, cleaned_files)), decreasing = TRUE)


cleaned_files[1] %>% read_tibble() %>% 
    colnames()


# Subset files
is_files_raw <- raw_files %>% str_subset("income_statement")
bs_files_raw <- raw_files %>% str_subset("balance_sheet")
cf_files_raw <- raw_files %>% str_subset("cash_flow")

is_files_cleaned <- cleaned_files %>% str_subset("income_statement")
bs_files_cleaned <- cleaned_files %>% str_subset("balance_sheet")
cf_files_cleaned <- cleaned_files %>% str_subset("cash_flow")

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

file_path %>% add_download_date()



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
    
    saveRDS(fields, paste0(what, "_", "fields.rds"))
}

# get_and_save_field_names(files = is_files_raw, what = "is")
# get_and_save_field_names(files = bs_files_raw, what = "bs")
# get_and_save_field_names(files = cf_files_raw, what = "cf")

is_fields <- readRDS("is_fields.rds")
bs_fields <- readRDS("bs_fields.rds")
cf_fields <- readRDS("cf_fields.rds")



clean_field_names <- function(df) {
    ######
    # df <- is_files_raw[72] %>% add_download_date()
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
                str_replace_all("\\'", "_") %>% 
                str_replace_all("__", "_") %>%
                str_replace_all("__", "_") %>% 
                str_replace_all("__", "_") %>% 
                str_remove_all("_$")
        )
}

is_files_raw[1] %>% add_download_date() %>% clean_field_names()



# [3] "total_assets"
# [5] "total_liabilities"                         
# [6] "cash_and_cash_equivalents"                 
# [7] "total_current_assets"                      
# [8] "total_current_liabilities"                 
# [9] "goodwill"                                  
# [10] "short_term_debt"                           
# [11] "non_current_portion_of_long_term_debt"     
# [12] "other_intangible_assets_excluding_goodwill"
# [13] "operating_income"                          
# [14] "net_income"                                
# [15] "net_cash_provided_by_operating_activities" 
# [17] "net_cash_provided_by_investing_activities" 
# [18] "net_cash_provided_by_financing_activities" 
# [19] "cost_of_revenue"                           
# [20] "gross_profit"                              
# [21] "total_operating_expenses"                  
# [22] "earnings_per_share_diluted"  ---->  diluted_eps
# [23] "current_portion_of_long_term_debt"         
# [24] "net_interest_paid"                         
# [25] "net_interest_income"


consolidate_field_names <- function(df) {
    ######
    # df <- is_files_raw[1] %>% add_download_date() %>% clean_field_names()
    ######
    df %>%
        mutate(field = case_when(
            field %in% c(
                "total_revenues",
                "total_operating_revenues",
                "net_revenue",
                "net_revenues",
                "revenue_net",
                "sales_net",
                "net_sales",
                "sales",
                "revenue",
                "revenues",
                "operating_revenue",
                "operating_revenues",
                "net_sales_and_other_revenue",
                "total_net_revenue",
                "net_service_revenues",
                "total_revenues_net"
            ) ~ "total_revenue",
            field %in% c(
                "total_cost_of_revenue",
                "cost_of_products_sold",
                "cost_of_goods_sold",
                "cost_of_sales",
                "total_cost_of_sales",
                "cost_of_revenues",
                "cost_of_sales_including_purchasing_and_warehousing_costs",
                "total_costs_of_revenues",
                "cost_of_revenues_exclusive_of_depreciation_and_amortization_shown_separately_below",
                "cost_of_goods_and_services_sold",
                "cost_of_service_revenues"
            ) ~ "cost_of_revenue",
            str_detect(field,
                       "cost_of_sales_including_.*"
                ) ~ "cost_of_revenue",
            field %in% c(
                "gross_margin",
                "gross_loss"
            ) ~ "gross_profit",
            field %in% c(
                "research_and_development_expenses"
            ) ~ "research_and_development",
            field %in% c(
                "general_and_administrative_expenses"
            ) ~ "general_and_administrative_expense",
            field %in% c(
                "selling_general_and_administrative_expenses",
                "selling_and_administrative_expenses",
                "selling_marketing_general_and_administrative"
            ) ~ "selling_general_and_administrative",
            field %in% c(
                "sales_and_marketing",
                "advertising_and_marketing"
            ) ~ "selling_expense",
            field %in% c(
                "depreciation_amortization_depletion_and_accretion",
                "depreciation_and_amortization_expense"
            ) ~ "depreciation_amortization",
            field %in% c(
                "amortization_of_intangible_assets"
            ) ~ "amortization",
            field %in% c(
                "income_from_operations",
                "operating_loss_profit",
                "income_loss_from_operations",
                "loss_from_operations",
                "operating_loss",
                "operating_income_loss",
                "net_loss_from_operations",
                "loss_income_from_operations",
                "operating_profit_loss",
                "loss_from_operating_activities",
                "earnings_loss_before_interest_and_income_taxes",
                "net_operating_loss"
            ) ~ "operating_income",
            field %in% c(
                "income_before_income_taxes",
                "income_before_provision_for_income_taxes",
                "earnings_before_income_tax_expense",
                "income_from_operations_before_income_taxes",
                "net_loss_before_provision_for_income_tax",
                "income_loss_before_income_taxes",
                "net_income_loss_before_tax",
                "loss_before_income_taxes",
                "income_loss_before_income_tax_credit",
                "income_before_income_tax",
                "loss_income_before_income_taxes",
                "income_loss_before_taxes",
                "loss_before_income_tax_provision_benefit",
                "loss_before_provision_for_income_taxes",
                "income_before_income_tax_expense",
                "earnings_before_income_taxes",
                "income_loss_before_income_tax_expense",
                "loss_before_income_tax",
                "income_loss_before_provision_for_income_taxes",
                "net_loss_before_income_tax_benefit",
                "net_loss_before_income_taxes",
                "income_before_income_tax_expense_and_equity_earnings",
                "income_loss_before_income_tax_expense_benefit",
                "net_income_before_income_taxes"
            ) ~ "income_before_taxes",
            field %in% c(
                "income_taxes",
                "provision_for_income_taxes",
                "provision_for_income_tax_expense",
                "income_tax_expense_benefit",
                "income_tax_benefit_expense",
                "income_tax_provision",
                "income_taxes_credit",
                "benefit_from_provision_for_income_taxes",
                "income_tax_provision_benefit",
                "total_benefit_for_income_taxes",
                "provision_for_benefit_from_income_taxes",
                "provision_benefit_for_income_taxes",
                "income_tax_recovery",
                "benefit_for_income_taxes"
            ) ~ "income_tax_expense",
            field %in% c(
                "net_income",
                "net_earnings_loss",
                "net_income_loss",
                "net_loss",
                "net_loss_income",
                "net_earnings"
            ) ~ "net_income",
            str_detect(field,
                       "net_income_loss_attributable_to(?!.*_per_).*|net_loss_attributable_to(?!.*_per_).*|net_income_attributable_to(?!.*_per_).*"
            ) ~ "net_income",
            field %in% c(
                "dividends_per_common_share_in_dollars_per_share"
            ) ~ "dividends_per_share",
            field %in% c(
                "basic_earnings_per_common_share_in_dollars_per_share",
                "earnings_loss_per_share_in_dollars_per_share",
                "basic_in_dollars_per_share",
                "earnings_loss_per_share_in_dollars_per_share",
                "earnings_per_common_share_basic_in_usd_per_share",
                "basic_and_diluted_loss_per_share",
                "basic_earnings_per_share_in_dollars_per_share",
                "basic_in_dollars_per_unit",
                "basic_usd_per_share",
                "basic_earnings_per_common_share",
                "net_loss_per_share_basic_and_diluted_basic_and_diluted",
                "net_income_per_share_basic",
                "basic_and_diluted_income_loss_per_common_share",
                "net_income_loss_per_share_basic",
                "net_income_per_common_share_basic",
                "basic_and_diluted_loss_per_share_of_common_stock",
                "net_loss_per_common_share_basic_and_diluted",
                "basic_per_share",
                "net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                "net_loss_per_share_basic_and_diluted",
                "net_income_per_common_share_basic_in_usd_per_share",
                "basic_income_per_share_in_usd_per_share",
                "net_income_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                "basic_earnings_per_share",
                "basic_and_diluted_net_loss_per_share_attributable_to_common_stockholders_in_dollars_per_share",
                "basic_and_diluted_net_loss_per_common_share",
                "net_loss_per_share_of_common_stock_basic_and_diluted_in_dollars_per_share",
                "net_loss_income_per_common_share_basic",
                "net_income_loss_per_sharebasic",
                "basic_earnings_in_dollars_per_share",
                "basic_in_usd_per_share",
                "basic_and_diluted_earnings_loss_per_share",
                "basic_and_diluted_in_dollars_per_share",
                "basic_and_diluted_profit_loss_per_common_share",
                "basic_net_income_loss_per_common_share",
                "basic_and_diluted_income_loss_per_share_in_dollars_per_share",
                "earnings_per_share_basic",
                "net_loss_per_share_basic_and_diluted_in_dollars_per_share",
                "basic_and_diluted_net_loss_per_share",
                "basic_net_income_per_share",
                "basic_net_income_per_share_in_usd_per_share",
                "basic_earnings_per_share_in_us_per_share",
                "basic_and_diluted_per_common_share_in_dollars_per_share",
                "earnings_per_common_share_basic_in_dollars_per_share",
                "basic_net_earnings_losses_per_common_share_in_dollars_per_share",
                "basic_income_per_share",
                "earnings_loss_per_common_share_basic",
                "net_loss_per_common_sharebasic_in_dollars_per_share",
                "earnings_per_common_share",
                "net_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                "net_income_loss_per_basic_share",
                "basic_and_diluted_loss_per_common_share",
                "net_loss_per_common_share_basic"
            ) ~ "basic_eps",
            str_detect(field,
                      "total_basic_earnings_per_share_attributable_to.*common_shareholders"
                ) ~ "basic_eps",
            field %in% c(
                "diluted_earnings_per_common_share_in_dollars_per_share",
                "earnings_loss_per_share_assuming_dilution_in_dollars_per_share",
                "diluted_in_dollars_per_share",
                "earnings_per_common_share_diluted_in_usd_per_share",
                "basic_and_diluted_loss_per_share",
                "diluted_earnings_per_share_in_dollars_per_share",
                "diluted_in_dollars_per_unit",
                "diluted_usd_per_share",
                "diluted_earnings_per_common_share",
                "net_loss_per_share_basic_and_diluted_basic_and_diluted",
                "net_income_per_share_diluted",
                "basic_and_diluted_income_loss_per_common_share",
                "net_income_loss_per_share_diluted",
                "net_income_per_common_share_diluted",
                "basic_and_diluted_loss_per_share_of_common_stock",
                "net_loss_per_common_share_basic_and_diluted",
                "diluted_per_share",
                "net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                "net_loss_per_share_basic_and_diluted",
                "net_income_per_common_share_diluted_in_usd_per_share",
                "diluted_income_per_share_in_usd_per_share",
                "net_income_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                "diluted_earnings_per_share",
                "basic_and_diluted_net_loss_per_share_attributable_to_common_stockholders_in_dollars_per_share",
                "basic_and_diluted_net_loss_per_common_share",
                "net_loss_per_share_of_common_stock_basic_and_diluted_in_dollars_per_share",
                "net_loss_income_per_common_share_diluted",
                "diluted_in_usd_per_share",
                "net_income_loss_per_sharediluted",
                "basic_and_diluted_earnings_loss_per_share",
                "basic_and_diluted_in_dollars_per_share",
                "basic_and_diluted_profit_loss_per_common_share",
                "basic_and_diluted_income_loss_per_share_in_dollars_per_share",
                "earnings_per_share_diluted",
                "net_loss_per_share_basic_and_diluted_in_dollars_per_share",
                "diluted_loss_per_common_share_in_dollars_per_share",
                "basic_and_diluted_net_loss_per_share",
                "diluted_net_income_per_share",
                "diluted_earnings_per_share_in_us_per_share",
                "diluted_net_income_per_share_in_usd_per_share",
                "basic_and_diluted_per_common_share_in_dollars_per_share",
                "earnings_per_common_share_diluted_in_dollars_per_share",
                "diluted_net_earnings_losses_per_common_share_in_dollars_per_share",
                "diluted_income_per_share",
                "earnings_loss_per_common_share_diluted",
                "earnings_per_common_share_assuming_dilution",
                "net_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                "net_income_loss_per_diluted_share",
                "basic_and_diluted_loss_per_common_share"
            ) ~ "diluted_eps",
            str_detect(field,
                       "total_diluted_earnings_per_share_attributable_to.*common_shareholders"
            ) ~ "diluted_eps",
            field %in% c(
                "weighted_average_shares_of_common_stock_outstanding_basic_in_shares",
                "basic_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_and_fully_diluted",
                "weighted_average_common_shares_outstanding",
                "weighted_average_basic_shares_outstanding_in_shares",
                "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_common_shares_outstanding",
                "weighted_average_shares_outstanding_basic",
                "basic_and_diluted_income_loss_per_common_share",
                "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_basic",
                "weighted_average_number_of_shares_of_common_stock_outstanding",
                "weighted_average_common_shares_outstanding_basic_and_diluted",
                "weighted_average_common_shares_used_in_computing_net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_basic_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_and_diluted_in_shares",
                "basic_weighted_average_common_shares",
                "basic_and_diluted_weighted_average_shares_outstanding_common_stock_in_shares",
                "weighted_average_shares_used_in_computation_of_basic_and_diluted_net_loss_per_common_share",
                "shares_used_in_computing_net_loss_per_share_of_common_stock_basic_and_diluted_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_in_shares",
                "weighted_average_common_shares_outstanding_used_in_computing_net_loss_per_sharebasic",
                "basic_and_diluted_in_shares",
                "average_number_of_shares_outstanding_basic",
                "weighted_average_shares_basic_and_diluted_in_shares",
                "shares_used_to_compute_earnings_per_common_share_basic_in_shares",
                "basic_and_diluted_weighted_average_shares_outstanding_common_stock",
                "weighted_average_number_of_common_shares_outstanding_basic",
                "shares_used_to_compute_basic_net_income_per_share",
                "weighted_average_shares_used_in_computing_basic_net_income_per_share_in_shares",
                "basic_weighted_average_shares_outstanding_shares",
                "basic_and_diluted_weighted_average_shares_outstanding",
                "weighted_average_common_shares_outstanding_basic_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_during_the_period_basic_and_diluted_in_shares",
                "weighted_average_number_of_common_shares_outstandingbasic_in_shares",
                "weighted_average_common_shares_outstanding_earnings_per_common_share",
                "weighted_average_common_shares_outstanding_basic_and_diluted_in_shares",
                "weighted_average_common_shares_outstanding_basic",
                "basic_weighted_average_common_shares_outstanding",
                "weighted_average_number_of_shares_outstanding_basic_and_fully_diluted",
                "basic_weighted_average_shares_of_common_stock_outstanding_in_shares",
                "weighted_average_shares_used_in_computing_net_loss_per_share_basic_and_diluted"
                ) ~ "basic_shares",
            str_detect(field,
                       "weighted_average_number_of_basic.*common_shares_outstanding"
                ) ~ "basic_shares",
            field %in% c(
                "weighted_average_shares_of_common_stock_outstanding_diluted_in_shares",
                "diluted_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_and_fully_diluted",
                "weighted_average_common_shares_outstanding",
                "weighted_average_diluted_shares_outstanding_in_shares",
                "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_during_the_period_diluted",
                "weighted_average_shares_outstanding_diluted",
                "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_diluted",
                "weighted_average_common_shares_outstanding_basic_and_diluted",
                "weighted_average_common_shares_used_in_computing_net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_diluted_in_shares",
                "weighted_average_number_of_common_shares_outstanding_basic_and_diluted_in_shares",
                "diluted_weighted_average_common_shares",
                "basic_and_diluted_weighted_average_shares_outstanding_common_stock_in_shares",
                "weighted_average_shares_used_in_computation_of_basic_and_diluted_net_loss_per_common_share",
                "shares_used_in_computing_net_loss_per_share_of_common_stock_basic_and_diluted_in_shares",
                "weighted_average_number_of_common_shares_outstanding_diluted_in_shares",
                "weighted_average_common_shares_outstanding_used_in_computing_net_loss_per_sharediluted",
                "basic_and_diluted_in_shares",
                "average_number_of_shares_outstanding_diluted",
                "weighted_average_shares_basic_and_diluted_in_shares",
                "shares_used_to_compute_earnings_per_common_share_diluted_in_shares",
                "basic_and_diluted_weighted_average_shares_outstanding_common_stock",
                "weighted_average_number_of_common_shares_outstanding_diluted",
                "shares_used_to_compute_diluted_net_income_per_share",
                "weighted_average_shares_used_in_computing_diluted_net_income_per_share_in_shares",
                "diluted_weighted_average_shares_outstanding_shares",
                "basic_and_diluted_weighted_average_shares_outstanding",
                "weighted_average_number_of_common_shares_outstanding_basic_and_diluted",
                "weighted_average_number_of_shares_outstanding_during_the_period_basic_and_diluted_in_shares",
                "weighted_average_common_shares_outstanding_earnings_per_common_share_assuming_dilution",
                "weighted_average_common_shares_outstanding_basic_and_diluted_in_shares",
                "weighted_average_common_shares_outstanding_diluted",
                "diluted_weighted_average_common_shares_outstanding",
                "weighted_average_number_of_shares_outstanding_basic_and_fully_diluted",
                "weighted_average_shares_outstanding_diluted_in_shares",
                "weighted_average_shares_used_in_computing_net_loss_per_share_basic_and_diluted"
            ) ~ "diluted_shares",
            str_detect(field,
                      "weighted_average_number_of_diluted.*common_shares_outstanding"
                ) ~ "diluted_shares",
            TRUE ~ field
        ))
}


# is_files_raw[130] %>% add_download_date() %>% clean_field_names() %>% pull(field)

map(is_files_raw[131:140],
    ~add_download_date(.x) %>% clean_field_names() %>% consolidate_field_names() %>%
        pull(field))

    
is_files_raw[130] %>% add_download_date() %>% clean_field_names() %>% View()


str_detect()
c("net_income",
  "net_income_loss_attributable_to_the_aes_per_corporation") %>%
    str_replace("net_income_loss_attributable_to(?!.*_per_).*",
                "net_income")




tibble(field = c("net_income_rsee",
         "net_income_loss_attributable_to_the_aes_corporation")) %>%
mutate(field = case_when(
    str_detect(field,
               "net_income_loss_attributable_to(?!.*_per_).*|net_income_(?!.*rsee)") ~ "net_income",
    TRUE ~ field))


tibble(field = c("net_income",
  "net_income_loss_attributable_to_the_aes_per_corporation")) %>%
    mutate(field = case_when(
        str_detect(field, "net_income_loss_attributable_to(?!.*_per_).*") ~ "net_income",
        TRUE ~ field
        
    ))



                                       

# To Do Later:
    # 1. compute Gross profit if not available
# if amortization is available but depreciation or depreciation and amortization is not, then rename amortization "depreciation_amortization"
# 2. Sometimes a field includes multiple labels (e.g. basic and diluted shares outstanding). It is best to create a separate row for each item and copy the values down.

!!!ticker "AAP" has an error: "weighted_average_common_shares_outstanding"              occurs twice, and the second time is the label for diluted shares outstdning
Create a special rule for this ticker to rename the second occurence

ticker ABIO has duplicated fields: basic_and_diluted (2x)


ticker "ACLS" has basic and diluted followed by basic_shares and diluted_shares

"basic"
"diluted"
"basic"
"diluted"

"basic"
"diluted"
"basic_shares"            "basic_eps"
"diluted_shares" --or--   "diluted_eps"



is_fields_raw %>% unique()



# Sometimes gross profit is not present, but it can be computed!!!

# tickers with irregular fields
# is_fields %>% 
#     .[c("AA", "ARNC", "GNE", "HMC", "IDT", "RE", "SJM")]
# is_fields %>% 
#     map_lgl(~any(str_detect(.x, "\\([A-Za-z]\\)"))) %>% 
#     which()


is_fields_unique <- is_fields_raw %>% unique()


is_field_counts <- map(is_fields_unique, ~(is_fields_raw == .x) %>% sum())
names(is_field_counts) <- is_fields_unique

is_field_counts[is_field_counts >= 200] %>% names()



# total_revenues
is_fields %>%
    map_lgl(~any(str_detect(.x, "Sales, net|Total operating revenues|Revenue|Revenues"))) %>% 
    which() %>% names()




# Consolidate is_fields
consodlidate_is_fields <- function(x) {
    
    
}







get_cleaned_data <- function(files) {
    map_df(files, add_download_date) %>% 
        arrange(ticker, date, desc(download_date)) %>% 
        drop_na(ticker, date, download_date) %>% 
        group_by(ticker, date) %>% 
        # The data is sorted with the most recent download_date first.
        # If the most recent data is missing, pull it from the prior
        # download dates (fill up)
        fill(!c(ticker, date, download_date), .direction = "downup") %>% 
        slice(1) %>% 
        # filter(!duplicated(ticker, date)) %>% 
        ungroup()
}


!!!! Identify tickers without common field names

tickers_wo_key_fields <- NULL
key_fields <- c("total_revenue",
                "net_sales",
                "total_revenues_and_other_income",
                "gross_profit",
                "operating_income", 
                "earnings_before_provision_for_income_taxes"
                "income_before_income_taxes",
                "income_loss_before_income_taxes" (COP)
                "net_earnings",
                "net_income",
                "net_income_loss", (COP)
                "net_income_attributable_to_common_stockholders"
                )

# Example cleaning code
basic_id <-
    is_df_tmp %>% 
    pull(field) %>% 
    str_detect("^Basic$") %>% 
    which()

diluted_id <-
    is_df_tmp %>% 
    pull(field) %>% 
    str_detect("^Diluted$") %>% 
    which()

row_means <-
    is_df_tmp %>% 
    select(-field) %>% 
    rowMeans(na.rm = TRUE)

if(length(basic_id) == 2) {
    if(row_means[basic_id[1]] < 100 & row_means[basic_id[2]] >= 100) {
        is_df_tmp[basic_id[1], "field"] <- "Basic eps"
        is_df_tmp[basic_id[2], "field"] <- "Basic shares"
    }
    if(row_means[diluted_id[1]] < 100 & row_means[diluted_id[2]] >= 100) {
        is_df_tmp[diluted_id[1], "field"] <- "Diluted eps"
        is_df_tmp[diluted_id[2], "field"] <- "Diluted shares"
    }










basic_weighted_average_common_shares --> basic_shares
basic_earnings_per_share --> basic_eps

ticker: COP
Net Income (Loss) Attributable to ConocoPhilips
Basic  1.78
Basic 1,3332,000,000















# Clean data
is_data <- get_cleaned_data(is_files)
bs_data <- get_cleaned_data(bs_files)
cf_data <- get_cleaned_data(cf_files)

# Save
fwrite(is_data, paste0(dir_proj, "/data/cleaned data/income_statements_cleaned ",
                       today, ".csv" ))
fwrite(bs_data, paste0(dir_proj, "/data/cleaned data/balance_sheets_cleaned ",
                       today, ".csv" ))
fwrite(cf_data, paste0(dir_proj, "/data/cleaned data/cash_flows_cleaned ",
                       today, ".csv" ))




# Backup data -------------------------------------------------------------

# Remove old cleaned files
remove_old_cleaned_files <- function() {
    cleaned_files <- 
        list.files(paste0(dir_proj, "/data/cleaned data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    latest_date <- 
        cleaned_files %>% 
        str_extract("(?<= \\()\\d{4} \\d{2} \\d{2}(?=\\)\\.csv)") %>% 
        max()
    
    files_to_remove <- 
        cleaned_files %>% 
        .[{!cleaned_files %>% str_detect(latest_date)}]
    
    file.remove(files_to_remove)
}
remove_old_cleaned_files()



# Zip remaining cleaned files
zip_remaining_files <- function() {
    cleaned_files <- 
        list.files(paste0(dir_proj, "/data/cleaned data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    
    zip::zipr(zipfile = paste0(dir_proj, "/data/backup/cleaned data - backup ", 
                              today, ".zip"), files = cleaned_files)
}

zip <- try(zip_remaining_files())

# Remove old raw files
remove_old_raw_files <- function() {
    raw_files <- 
        list.files(paste0(dir_proj, "/data/raw data"), 
                   pattern = "income_statement|balance_sheet|cash_flow",
                   full.names = TRUE)
    
    file.remove(raw_files)
}

if(!"try-error" %in% class(zip)) {
    remove_old_raw_files()
}


