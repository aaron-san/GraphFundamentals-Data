#######################
# Clean fundamentals
#######################
# - Add download_date
# - Reformat dates, numbers, etc.
# - Combine all tickers into one file

start <- Sys.time() # ~1 hr

library(tidyverse)
library(data.table)

source("C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/helper functions.R")
source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/helper functions.R")
source("C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/helper functions from Fundamentals-Data.R")

# source("C:/Users/AaronH/Desktop/GraphFundamentals-Data/helper functions.R")
# ?? (does this file exist?) source("C:/Users/AaronH/Desktop/Fundamentals-Data/helper functions.R")
# source("C:/Users/AaronH/Desktop/GraphFundamentals-Data/helper functions from Fundamentals-Data.R")

conflicted::conflict_prefer_all("dplyr", quiet=TRUE)

dir_proj <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data"
# dir_proj <- "C:/Users/AaronH/Desktop/GraphFundamentals-Data"

today <- paste0("(", str_replace_all(Sys.Date(), "-", " "), ")")

raw_files <- 
    list.files(paste0(dir_proj, "/data/raw data"), 
               pattern = "income_statement|balance_sheet|cash_flow",
               full.names = TRUE)


cleaned_files <- 
    list.files(paste0(dir_proj, "/data/cleaned data"), 
               pattern = "income_statements_quarterly_cleaned|balance_sheets_quarterly_cleaned|cash_flows_quarterly_cleaned",
               full.names = TRUE)

is_files_raw <- raw_files %>% str_subset("income_statement")
bs_files_raw <- raw_files %>% str_subset("balance_sheet")
cf_files_raw <- raw_files %>% str_subset("cash_flow")


is_files_cleaned_old <- cleaned_files %>% str_subset("income_statements_quarterly_cleaned")
bs_files_cleaned_old <- cleaned_files %>% str_subset("balance_sheets_quarterly_cleaned")
cf_files_cleaned_old <- cleaned_files %>% str_subset("cash_flows_quarterly_cleaned")


# Check for quarterly dates
is_has_quarterly_dates <- map_lgl(is_files_raw, ~has_quarterly_dates(.x))
bs_has_quarterly_dates <- map_lgl(bs_files_raw, ~has_quarterly_dates(.x))
cf_has_quarterly_dates <- map_lgl(cf_files_raw, ~has_quarterly_dates(.x)) 

if({is_has_quarterly_dates %>% which() %>% length()} < 0.95 * length(is_files_raw)) stop("Too much non-quarterly data!")
if({bs_has_quarterly_dates %>% which() %>% length()} < 0.95 * length(bs_files_raw)) stop("Too much non-quarterly data!")
if({cf_has_quarterly_dates %>% which() %>% length()} < 0.95 * length(cf_files_raw)) stop("Too much non-quarterly data!")

# Analyze field names
# get_and_save_field_names(files = is_files_raw, what = "is")
# get_and_save_field_names(files = bs_files_raw, what = "bs")
# get_and_save_field_names(files = cf_files_raw, what = "cf")

# is_fields <- readRDS("data/temp/is_fields.rds")
# bs_fields <- readRDS("data/temp/bs_fields.rds")
# cf_fields <- readRDS("data/temp/cf_fields.rds")





# INCOME STATEMENT --------------------------------------------------------

consolidate_is_field_names <- function(df) {
    ######
    # df <- is_files_raw[1] %>% add_download_date() %>% clean_field_names()
    ######
    df %>%
        mutate(
            field = case_when(
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
                    "total_revenues_net",
                    "total_revenue_net",
                    "revenues_net",
                    "total_net_revenues"
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
                    "cost_of_goods_and_services_sold",
                    "cost_of_service_revenues",
                    "cost_of_service_revenue",
                    "cost_of_net_revenue",
                    "cost_of_food_merchandise_and_other_revenues",
                    "direct_cost_of_revenues_exclusive_of_depreciation_and_amortization",
                    "direct_costs_excluding_depreciation_and_amortization",
                    "total_costs_of_revenue",
                    "total_cost_of_revenues",
                    "total_cost_of_goods_sold"
                ) ~ "cost_of_revenue",
                str_detect(field, #REGEX
                           "(total)?cost_of_sales_including_.*|costs?_of_(products_sold|goods_sold|operations|sales|(net_)?revenues?|product_revenues?|product_sales|services).*(?=exclusive_of_depreciation|excluding_depreciation|exclusive_of_amortization|excluding_amortization).*"
                           ) ~ "cost_of_revenue",
                field %in% c(
                    "gross_margin",
                    "gross_loss",
                    "gross_loss_profit",
                    "total_gross_profit",
                    "gross_profit_loss"
                ) ~ "gross_profit",
                field %in% c("research_and_development_expenses") ~ "research_and_development",
                field %in% c(
                    "general_and_administrative_expenses",
                    "general_and_administrative_expense",
                    "general_and_administrative_and_other",
                    "general_and_administrative",
                    "corporate_expenses"
                ) ~ "general_administrative_expense",
                field %in% c(
                    "selling_general_and_administrative_expenses",
                    "selling_general_and_administrative_expense",
                    "selling_and_administrative_expenses",
                    "selling_marketing_general_and_administrative",
                    "selling_general_and_administrative",
                    "selling_and_general_expenses",
                    "selling_general_and_administrative_exclusive_of_depreciation_and_amortization_shown_below",
                    "selling_general_and_administrative_expenses_excludes_depreciation_and_amortization",
                    "selling_general_and_administrative_expenses_exclusive_of_depreciation_and_amortization",
                    "selling_general_and_administrative_expenses_exclusive_of_depreciation_depletion_and_amortization_shown_separately_above"
                ) ~ "selling_general_administrative",
                field %in% c(
                    "employee_compensation_and_benefits",
                    "compensation",
                    "salaries_and_wages",
                    "salaries_and_employee_benefits",
                    "compensation_and_benefits",
                    "salaries_and_benefits",
                    "compensation_and_employee_benefits"
                ) ~ "salaries_and_wages",
                field %in% c(
                    "sales_and_marketing",
                    "advertising_and_marketing",
                    "selling_and_marketing",
                    "selling_and_marketing_expenses",
                    "advertising_and_promotion",
                    "marketing",
                    "advertising",
                    "selling",
                    "marketing_and_advertising",
                    "sales_and_marketing_expenses"
                ) ~ "selling_expense",
                field %in% c(
                    "research_and_development_expense",
                    "product_development"
                ) ~ "research_and_development",
                field %in% c(
                    "depreciation_amortization_depletion_and_accretion",
                    "depreciation_and_amortization_expense",
                    "depreciation_and_amortization",
                    "depreciation_depletion_and_amortization",
                    "depreciation_amortization_and_accretion",
                    "depreciation_depletion_amortization_and_accretion",
                    "depreciation_accretion_and_amortization",
                    "depreciation_amortization_and_depletion",
                    "depreciation_and_amortization_exclusive_of_depreciation_and_amortization_expense_of_157698_158203_309597_and_315712_respectively_included_in_cost_of_sales",
                    "depreciation_and_amortization_of_lease_costs",
                    "depreciation_and_amortization_other",
                    "depreciation_and_intangible_asset_amortization",
                    "depreciation_and_other_amortization",
                    "depreciation_and_amortization_of_property_and_equipment"
                ) ~ "depreciation_amortization",
                field %in% c(
                    "other_depreciation_and_amortization",
                    "depreciation_expense"
                ) ~ "depreciation",
                field %in% c(
                    "amortization_of_intangible_assets",
                    "amortization_of_intangibles",
                    "amortization_of_other_acquired_intangible_assets",
                    "software_amortization",
                    "other_intangible_amortization",
                    "other_intangible_assets_amortization",
                    "pre_publication_amortization",
                    "program_cost_amortization",
                    "publishing_rights_amortization",
                    "mortgage_servicing_rights_amortization",
                    "nonvested_stock_amortization_expenses",
                    "intangible_amortization_expense",
                    "intangible_asset_amortization_expense",
                    "intangible_assets_amortization",
                    "intangibles_amortization_expense",
                    "financing_costs_amortization",
                    "amortization_expense_of_intangible_assets",
                    "amortization_intangible_assets",
                    "amortization_expense_of_intangible_assets",
                    "core_deposit_and_customer_relationship_intangibles_amortization",
                    "amortization_intangible_assets",
                    "amortization_expense",
                    "intangible_amortization",
                    "intangible_asset_amortization"
                ) ~ "amortization",
                str_detect(field, #REGEX
                           "^amortization_of.*(?!depreciation).*"
                ) ~ "amortization",
                
                field %in% c(
                    "total_operating_costs_and_expenses",
                    "operating_expenses",
                    "total_operating_costs",
                    "total_operating_expense",
                    "operating_and_formation_costs",
                    "total_noninterest_expenses",
                    "total_costs_and_operating_expenses",
                    "total_noninterest_expense",
                    "total_non_interest_expense",
                    "total_non_interest_expenses"
                ) ~ "total_operating_expenses",
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
                    "net_operating_loss",
                    "operating_loss_income",
                    "operating_profit"
                ) ~ "operating_income",
                field %in% c(
                    "total_interest_expense",
                    "interest",
                    "interest_on_short_term_borrowings"
                ) ~ "interest_expense",
                field %in% c(
                    "interest_earned_on_marketable_securities_held_in_trust_account",
                    "total_interest_income",
                    "interest_on_deposits",
                    "interest_and_fees_on_loans",
                    "interest_and_other_income",
                    "service_charges_on_deposit_accounts",
                    "interest_and_other_income_net"
                ) ~ "interest_income",
                field %in% c(
                    "net_interest_income_after_provision_for_loan_losses",
                    "net_interest_income_after_provision_for_credit_losses",
                    "interest_income_expense_net",
                    "interest_expense_net",
                    "net_interest_income",
                    "interest_expense_income_net",
                    "interest_and_other_expense_net",
                    "interest_and_other_income_expense_net"
                ) ~ "interest_income_net",
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
                    "net_income_before_income_taxes",
                    "loss_before_income_tax_expense",
                    "loss_before_taxes",
                    "loss_before_taxes_on_income",
                    "net_income_loss_before_income_taxes",
                    "earnings_loss_before_income_taxes"
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
                    "benefit_for_income_taxes",
                    "income_taxes_benefit",
                    "income_taxes_expense",
                    "less_provision_benefit_for_income_taxes",
                    "benefit_provision_for_income_taxes",
                    "income_tax_provision_benefit_net",
                    "income_tax_benefit",
                    "provision_for_income_tax",
                    "benefit_from_income_taxes",
                    "income_tax_benefit_provision",
                    "income_tax_expense"
                ) ~ "income_taxes",
                field %in% c(
                    "net_income",
                    "net_earnings_loss",
                    "net_income_loss",
                    "net_loss",
                    "net_loss_income",
                    "net_earnings"
                ) ~ "net_income",
                # str_detect(
                    # field,
                    # "net_income_loss_attributable_to(?!.*_per_).*|net_loss_attributable_to(?!.*_per_).*|net_income_attributable_to(?!.*_per_).*"
                # ) ~ "net_income",
                field %in% c(
                    "income_from_continuing_operations_before_income_taxes",
                    "income_loss_from_continuing_operations_before_income_taxes",
                    "loss_from_continuing_operations_before_income_taxes"
                ) ~ "income_from_continuing_operations_before_taxes",
                field %in% c(
                    "loss_from_continuing_operations",
                    "income_loss_from_continuing_operations",
                    "net_income_loss_from_continuing_operations",
                    "net_income_from_continuing_operations"
                ) ~ "net_income_from_continuing_operations",
                
                field %in% c(
                    "income_loss_from_discontinued_operations_net_of_tax",
                    "income_loss_from_discontinued_operations",
                    "loss_from_discontinued_operations"
                ) ~ "net_income_from_discontinued_operations",
                
                field %in% c(
                    "net_income_available_to_common_stockholders",
                    "net_income_available_to_common_shareholders",
                    "net_income_loss_attributable_to_common_shareholders",
                    "net_income_loss_attributable_to_common_stockholders",
                    "net_loss_attributable_to_common_stockholders"
                ) ~ "net_income_available_to_common_shareholders",
                
                field %in% c(
                    "foreign_currency_translation_adjustment",
                    "foreign_currency_translation_adjustments",
                    "foreign_currency_translation",
                    "foreign_currency_translation_gain_loss"
                ) ~ "foreign_currency_translation_adjustment",
                field %in% c("comprehensive_income",
                             "comprehensive_income_loss",
                             "total_comprehensive_loss",
                             "comprehensive_loss_income"
                ) ~ "comprehensive_income",
                field %in% c(
                    "less_net_income_loss_attributable_to_noncontrolling_interests",
                    "less_net_income_attributable_to_noncontrolling_interest",
                    "less_net_income_attributable_to_non_controlling_interests",
                    "net_income_attributable_to_non_controlling_interests",
                    "net_loss_attributable_to_noncontrolling_interest",
                    "net_income_loss_attributable_to_noncontrolling_interests",
                    "net_income_attributable_to_noncontrolling_interests",
                    "less_net_income_attributable_to_noncontrolling_interests",
                    "net_loss_attributable_to_non_controlling_interest"
                ) ~ "net_income_attributable_to_noncontrolling_interest",
                field %in% c("dividends_per_common_share_in_dollars_per_share") ~ "basic_dividends_per_share",
                field %in% c("dividend_on_preferred_stock") ~ "preferred_dividends",
                field %in% c(
                    "basic_earnings_per_common_share_in_dollars_per_share",
                    "earnings_loss_per_share_in_dollars_per_share",
                    "basic_in_dollars_per_share",
                    "earnings_loss_per_share_in_dollars_per_share",
                    "earnings_per_common_share_basic_in_usd_per_share",
                    "basic_earnings_per_share_in_dollars_per_share",
                    "basic_in_dollars_per_unit",
                    "basic_usd_per_share",
                    "basic_earnings_per_common_share",
                    "net_income_per_share_basic",
                    "net_income_loss_per_share_basic",
                    "net_income_per_common_share_basic",
                    "basic_per_share",
                    "net_income_per_common_share_basic_in_usd_per_share",
                    "basic_income_per_share_in_usd_per_share",
                    "basic_earnings_per_share",
                    "net_loss_income_per_common_share_basic",
                    "net_income_loss_per_sharebasic",
                    "basic_earnings_in_dollars_per_share",
                    "basic_in_usd_per_share",
                    "basic_net_income_loss_per_common_share",
                    "earnings_per_share_basic",
                    "basic_net_income_per_share",
                    "basic_net_income_per_share_in_usd_per_share",
                    "basic_earnings_per_share_in_us_per_share",
                    "earnings_per_common_share_basic_in_dollars_per_share",
                    "basic_net_earnings_losses_per_common_share_in_dollars_per_share",
                    "basic_income_per_share",
                    "earnings_loss_per_common_share_basic",
                    "net_loss_per_common_sharebasic_in_dollars_per_share",
                    "earnings_per_common_share",
                    "net_income_loss_per_basic_share",
                    "net_loss_per_common_share_basic",
                    "net_loss_income_per_share_basic_in_usd_per_share",
                    "basic_loss_per_common_share_in_dollars_per_share",
                    "net_loss_per_share_basic_in_dollars_per_share",
                    "basic_net_loss_per_share",
                    "net_income_attributable_to_common_stockholders_per_share_basic_in_dollars_per_share",
                    "earnings_loss_per_share_basic",
                    "net_income_in_dollars_per_share",
                    "earnings_per_share_basic_in_dollars_per_share"
                ) ~ "basic_eps",
                str_detect(
                    field,
                    "total_basic_earnings_per_share_attributable_to.*common_shareholders"
                ) ~ "basic_eps",
                field %in% c(
                    "diluted_earnings_per_common_share_in_dollars_per_share",
                    "earnings_loss_per_share_assuming_dilution_in_dollars_per_share",
                    "diluted_in_dollars_per_share",
                    "earnings_per_common_share_diluted_in_usd_per_share",
                    "diluted_earnings_per_share_in_dollars_per_share",
                    "diluted_in_dollars_per_unit",
                    "diluted_usd_per_share",
                    "diluted_earnings_per_common_share",
                    "net_income_per_share_diluted",
                    "net_income_loss_per_share_diluted",
                    "net_income_per_common_share_diluted",
                    "diluted_per_share",
                    "net_income_per_common_share_diluted_in_usd_per_share",
                    "diluted_income_per_share_in_usd_per_share",
                    "diluted_earnings_per_share",
                    "net_loss_income_per_common_share_diluted",
                    "diluted_in_usd_per_share",
                    "net_income_loss_per_sharediluted",
                    "earnings_per_share_diluted",
                    "diluted_loss_per_common_share_in_dollars_per_share",
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
                    "basic_and_diluted_loss_per_common_share",
                    "net_loss_income_per_share_diluted_in_usd_per_share",
                    "net_loss_per_share_diluted_in_dollars_per_share",
                    "net_income_loss_per_common_share_diluted",
                    "earnings_loss_per_share_diluted",
                    "earnings_per_share_diluted_in_dollars_per_share"
                ) ~ "diluted_eps",
                str_detect(
                    field,
                    #REGEX
                    "total_diluted_earnings_per_share_attributable_to.*common_shareholders"
                ) ~ "diluted_eps",
                field %in% c(
                    "basic_and_diluted_loss_attributable_to_common_stockholders_per_common_share_in_dollars_per_share",
                    "loss_per_share_basic_and_diluted_in_dollars_per_share",
                    "net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                    "net_income_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_share_attributable_to_common_stockholders_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_common_share",
                    "net_loss_per_share_of_common_stock_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_earnings_loss_per_share",
                    "basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_profit_loss_per_common_share",
                    "basic_and_diluted_income_loss_per_share_in_dollars_per_share",
                    "net_loss_per_share_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_share",
                    "basic_and_diluted_per_common_share_in_dollars_per_share",
                    "net_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_loss_per_common_share",
                    "basic_and_diluted_loss_per_share",
                    "net_loss_per_share_basic_and_diluted_basic_and_diluted",
                    "basic_and_diluted_income_loss_per_common_share",
                    "basic_and_diluted_loss_per_share_of_common_stock",
                    "net_loss_per_common_share_basic_and_diluted",
                    "net_loss_per_share_basic_and_diluted",
                    "basic_and_diluted_loss_per_share",
                    "net_loss_per_share_basic_and_diluted_basic_and_diluted",
                    "basic_and_diluted_income_loss_per_common_share",
                    "basic_and_diluted_loss_per_share_of_common_stock",
                    "net_loss_per_common_share_basic_and_diluted",
                    "net_loss_per_share_basic_and_diluted",
                    "net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                    "net_income_loss_per_common_share_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_share_attributable_to_common_stockholders_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_common_share",
                    "net_loss_per_share_of_common_stock_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_earnings_loss_per_share",
                    "basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_profit_loss_per_common_share",
                    "basic_and_diluted_income_loss_per_share_in_dollars_per_share",
                    "net_loss_per_share_basic_and_diluted_in_dollars_per_share",
                    "basic_and_diluted_net_loss_per_share",
                    "earnings_per_share_basic_and_diluted",
                    "basic_and_diluted_income_loss_per_share",
                    "net_loss_per_ordinary_share_basic_and_diluted_usd",
                    "loss_per_share_attributable_to_shareholders_basic_and_diluted_in_dollars_per_share",
                    "loss_per_common_share_basic_and_diluted",
                    "net_loss_per_share_total_basic_and_diluted",
                    "loss_per_share_basic_and_diluted",
                    "basic_and_diluted_income_per_share_in_dollars_per_share",
                    "net_income_available_to_common_stockholders_basic_and_diluted",
                    "net_loss_per_share_basic_and_diluted_in_usd_per_share",
                    "basic_and_diluted_net_loss_applicable_to_common_stockholders_per_share",
                    "basic_and_diluted_net_loss_per_share_attributable_to_common_stockholders",
                    "basic_and_diluted_loss_per_share_in_dollars_per_share",
                    "net_loss_income_per_common_share_basic_and_diluted",
                    "basic_and_diluted_net_income_loss_per_common_unit_in_dollars_per_share",
                    "basic_and_diluted_net_income_per_share_class_a_ordinary_shares_in_dollars_per_share",
                    "net_loss_per_share_available_to_common_stockholders_basic_and_diluted"
                ) ~ "basic_and_diluted_eps",
                field %in% c(
                    "weighted_average_shares_of_common_stock_outstanding_basic_in_shares",
                    "basic_in_shares",
                    "weighted_average_number_of_shares_outstanding_in_shares",
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
                    "weighted_average_shares_used_in_computing_net_loss_per_share_basic_and_diluted",
                    "weighted_number_of_common_shares_outstanding",
                    "weighted_average_common_shares_basic_in_shares",
                    "weighted_average_common_shares_basic",
                    "weighted_average_common_shares_outstanding",
                    "weighted_average_number_of_common_shares_outstanding_in_shares",
                    "weighted_average_shares_outstanding",
                    "weighted_average_shares_outstanding_basic_in_shares",
                    "weighted_average_number_of_shares_outstanding"
                ) ~ "basic_shares",
                str_detect(
                    field,
                    #REGEX
                    "weighted_average_number_of_basic.*common_shares_outstanding|^weighted_average_common_shares_outstanding.*"
                ) ~ "basic_shares",
                field %in% c(
                    "weighted_average_shares_of_common_stock_outstanding_diluted_in_shares",
                    "diluted_in_shares",
                    "weighted_average_diluted_shares_outstanding_in_shares",
                    "weighted_average_number_of_shares_outstanding_during_the_period_diluted",
                    "weighted_average_shares_outstanding_diluted",
                    "weighted_average_number_of_shares_outstanding_diluted_in_shares",
                    "diluted_weighted_average_common_shares",
                    "weighted_average_number_of_common_shares_outstanding_diluted_in_shares",
                    "weighted_average_common_shares_outstanding_used_in_computing_net_loss_per_sharediluted",
                    "average_number_of_shares_outstanding_diluted",
                    "shares_used_to_compute_earnings_per_common_share_diluted_in_shares",
                    "weighted_average_number_of_common_shares_outstanding_diluted",
                    "shares_used_to_compute_diluted_net_income_per_share",
                    "weighted_average_shares_used_in_computing_diluted_net_income_per_share_in_shares",
                    "diluted_weighted_average_shares_outstanding_shares",
                    "weighted_average_common_shares_outstanding_earnings_per_common_share_assuming_dilution",
                    "weighted_average_common_shares_outstanding_diluted",
                    "diluted_weighted_average_common_shares_outstanding",
                    "weighted_average_shares_outstanding_diluted_in_shares",
                    "weighted_average_common_shares_diluted_in_shares",
                    "weighted_average_common_shares_diluted"
                ) ~ "diluted_shares",
                field %in% c(
                    "weighted_average_common_shares_outstanding_basic_and_diluted_in_shares",
                    "weighted_average_shares_used_in_computing_net_loss_per_share_basic_and_diluted",
                    "weighted_average_number_of_shares_outstanding_basic_and_fully_diluted",
                    "weighted_average_shares_used_to_compute_net_loss_per_share_basic_and_diluted",
                    "basic_and_diluted_in_shares",
                    "weighted_average_shares_basic_and_diluted_in_shares",
                    "basic_and_diluted_weighted_average_shares_outstanding_common_stock",
                    "basic_and_diluted_weighted_average_shares_outstanding",
                    "weighted_average_number_of_common_shares_outstanding_basic_and_diluted",
                    "weighted_average_number_of_shares_outstanding_during_the_period_basic_and_diluted_in_shares",
                    "basic_and_diluted_weighted_average_shares_outstanding_common_stock_in_shares",
                    "weighted_average_shares_used_in_computation_of_basic_and_diluted_net_loss_per_common_share",
                    "shares_used_in_computing_net_loss_per_share_of_common_stock_basic_and_diluted_in_shares",
                    "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                    "weighted_average_number_of_shares_outstanding_basic_and_diluted",
                    "weighted_average_number_of_shares_outstanding_diluted",
                    "weighted_average_common_shares_outstanding_basic_and_diluted",
                    "weighted_average_common_shares_used_in_computing_net_loss_per_share_attributable_to_common_stockholders_basic_and_diluted",
                    "weighted_average_number_of_common_shares_outstanding_basic_and_diluted_in_shares",
                    "weighted_average_number_of_common_shares_outstanding_basic_and_fully_diluted",
                    "basic_and_diluted_weighted_average_common_shares_outstanding_*",
                    "weighted_average_shares_used_to_compute_net_loss_per_share_basic_and_diluted_in_shares",
                    "weighted_average_ordinary_sharesbasic_and_diluted",
                    "weighted_average_number_of_shares_used_in_computing_net_loss_per_ordinary_share_basic_and_diluted",
                    "weighted_average_number_of_shares_outstanding_basic_and_diluted_in_shares",
                    "weighted_average_common_stock_shares_outstanding_basic_and_diluted",
                    "weighted_average_shares_outstanding_basic_and_diluted",
                    "weighted_average_shares_outstanding_basic_and_diluted_in_shares",
                    "basic_and_diluted_weighted_average_common_shares_outstanding",
                    "weighted_average_common_units_outstanding_basic_and_diluted_in_shares",
                    "basic_and_diluted_weighted_average_shares_outstanding_class_a_ordinary_shares_in_shares",
                    "weighted_average_number_of_common_shares_outstanding_used_to_compute_net_loss_per_share_basic_and_diluted",
                    "weighted_average_shares_of_common_stock_outstanding_basic_and_diluted",
                    "weighted_average_shares_used_to_compute_net_loss_per_common_share_basic_and_diluted",
                    "shares_used_in_computing_net_loss_per_share_basic_and_diluted"
                ) ~ "basic_and_diluted_shares",
                str_detect(
                    field,
                    #REGEX
                    "basic_and_diluted.*common_shares_outstanding_in_shares"
                ) ~ "basic_and_diluted_shares",
                TRUE ~ field
            )
        )
}

fields_is_to_ignore_1 <- function() {
    c(
        "other",
        "other_income_net",
        "other_expense_net",
        "comprehensive_loss",
        "other_income",
        "expensed_offering_costs",
        "other_income_expense_net",
        "loss_on_sale_of_private_placement_warrants",
        "total_other_income_expense",
        "other_income_expense_net",
        "conversion_inducement_expense",
        "total_other_income_expense",
        "other_income_net",
        "other_expense_net",
        "other_expense_income_net",
        "other_income_loss",
        "total_other_expense_net",
        "other_expense_income_net",
        "total_other_income_expense",
        "other_income_expense",
        "total_other_income_expense",
        "cost_of_service_revenue",
        "other_expense_income",
        "taxes_other_than_income_taxes",
        "dividends_declared_per_common_share",
        "dividends_declared_per_common_share",
        "total_costs_and_expenses",
        "professional_fees",
        "deposits",
        "total_expenses",
        "continuing_operations",
        "discontinued_operations",
        "asbestos_related_costs_benefit_net",
        "asset_impairment_charges",
        "total_other_expenses",
        "total_other_income_expense",
        "other_income_expense_net",
        "total_other_expense_net",
        "total_other_income_net",
        "other_income_expense_net",
        "total_other_income_expense",
        "total_other_income_expense_net",
        "other_operating_expenses",
        "total_other_expense",
        "total_other_income",
        "other_net",
        "other_income_expense_net",
        "other_operating_income_expense_net",
        "other_loss_net",
        "total_noninterest_income",
        "total_non_interest_income",
        "provision_for_loan_losses",
        "data_processing",
        "occupancy_and_equipment",
        "loans_including_fees",
        "other_expenses",
        "occupancy",
        "other_expense",
        "loans",
        "tax_exempt",
        "change_in_fair_value_of_derivative_warrant_liabilities",
        "total_other_income_expenses",
        "restructuring_charges",
        "total_other_expense_income",
        "total_other_expense_net"
    )
}
fields_is_to_ignore_2 <- function() {
    c(
        "other_net",
        "total_other_income_expense_net",
        "borrowings",
        "total_other_expense_income_net",
        "change_in_fair_value_of_warrant_liability",
        "loss_on_extinguishment_of_debt",
        "professional_services",
        "change_in_fair_value_of_derivative_liabilities",
        "change_in_fair_value_of_contingent_consideration",
        "change_in_fair_value_of_derivative_liability",
        "total",
        "gain_on_sale_of_loans",
        "fdic_insurance",
        "gain_loss_on_extinguishment_of_debt",
        "insurance",
        "short_term_borrowings",
        "other_comprehensive_income_loss",
        "bank_owned_life_insurance_income",
        "other_noninterest_income",
        "cash_dividends_declared_per_common_share",
        "fdic_insurance",
        "gain_loss_on_extinguishment_of_debt",
        "insurance",
        "net_occupancy",
        "total_other_comprehensive_income_loss",
        "equipment_expense",
        "furniture_and_equipment",
        "gain_on_extinguishment_of_debt",
        "other_operating_income",
        "real_estate_taxes",
        "nontaxable",
        "other_borrowings",
        "other_income_expenses_net",
        "equipment",
        "franchise_tax_expense",
        "income_from_bank_owned_life_insurance",
        "interest_expense_related_party",
        "change_in_fair_value_of_warrant_liabilities",
        "bank_owned_life_insurance",
        "borrowed_funds",
        "cash_dividends_declared_per_common_share_in_dollars_per_share",
        "commissions",
        "communications",
        "dividends_declared_per_share_in_dollars_per_share",
        "other_expenses_net",
        "other_noninterest_expense",
        "other_operating_expense",
        "property_operating_expenses"
        )
}

# field_patterns_is_to_ignore <- function() {
#   c("")
# }

# is_cleaned <- readRDS("data/temp/is_cleaned.rds")
# if(!exists("is_cleaned")) {
  is_cleaned <- 
    is_files_raw %>% 
    keep(is_has_quarterly_dates) %>% 
    map(~add_download_date(.x) %>% clean_field_names())
  tickers_is <- is_cleaned %>% map_chr(., ~.x[1, "ticker"] %>% unlist())
  names(is_cleaned) <- tickers_is
  saveRDS(is_cleaned, "data/temp/is_quarterly_cleaned.rds")
# }


# map_lgl(is_cleaned, ~dim(.x) %>% length() %>% {. != 2}) %>% which()

get_is_cleaned_list <- function(id) {
    map(
        is_cleaned[id],
        ~ consolidate_is_field_names(.x) %>%
            filter(!field %in% fields_is_to_ignore_1()) %>%
            filter(!field %in% fields_is_to_ignore_2()) %>%
            # filter(!str_detect(field, field_patterns_is_to_ignore())) %>%
            remove_leading_duplicates(field = "net_income") %>%
            split_basic_and_diluted_eps() %>%
            split_basic_and_diluted_shares() %>% 
            consolidate_dep_amor() %>% 
            consolidate_dep() %>% 
            consolidate_amor() %>% 
            consolidate_selling_expense() %>% 
            consolidate_general_administrative()
        )
}

# get_is_cleaned_list(171:180) %>% map(., ~pull(.x, field))


# Common fields
# is_cleaned_list <- readRDS("data/temp/is_cleaned_list.rds")
# if(!exists("is_cleaned_list")) {
    is_cleaned_list <- get_is_cleaned_list(seq_along(is_cleaned))
    saveRDS(is_cleaned_list, "data/temp/is_quarterly_cleaned_list.rds")
# }

is_fields_chr <- 
  is_cleaned_list %>% 
  map(~pull(.x, field)) %>% unlist() %>% as.character() %>% 
  table() %>% as.data.frame() %>% rename(field = ".", n = "Freq") %>% 
  arrange(desc(n))
# is_fields_chr %>% slice(1:90)



# Compare old field names and new field names: ----------------------------

# ticker <- "SHO"
# ticker_id <- 274

# old_fields <- 
#   is_cleaned_list %>% .[names(.) == ticker] %>%
#   # is_cleaned_list[274] %>% 
#   setNames(NULL) %>% .[[1]] %>% pull(field)
# 
# new_fields <- 
#   is_cleaned_list %>% .[names(.) == ticker] %>% 
#   map(~consolidate_is_field_names(.x) %>% pull(field)) %>% 
#   setNames(NULL) %>% .[[1]]
# 
# tibble(old = old_fields, new = new_fields) %>% 
#   filter(!old %in% fields_is_to_ignore()) %>% 
#   filter(!str_detect(old, field_patterns_is_to_ignore())) %>% 
#   View()


################################
################################
# group_1 <- is_cleaned_list %>% 
#   map_lgl(~pull(.x, field) %>% 
#         str_detect("basic_and_diluted") %>% any()) %>% 
#   which()
# group_2 <- is_cleaned_list %>% 
#   map_lgl(~pull(.x, field) %>% 
#             str_detect("^basic_and_diluted_shares$|^basic_and_diluted_eps$") %>% any()) %>% 
#   which()
# 
# setdiff(group_1, intersect(group_1, group_2))
# is_cleaned[1937] %>%
#   map(~consolidate_is_field_names(.x) %>% pull(field)) %>% 
#   unlist() %>% setNames(NULL)
################################
################################

# group_1 <- is_cleaned_list %>% 
#     map_lgl(~pull(.x, field) %>% 
#                 str_detect("selling_expense|general_administrative_expense") %>% any()) %>% 
#     which()
# group_2 <- is_cleaned_list %>% 
#     map_lgl(~pull(.x, field) %>% 
#                 str_detect("^selling_general_administrative$") %>% any()) %>% 
#     which()
# 
# setdiff(group_1, intersect(group_1, group_2))
# is_cleaned[2073] %>%
#     map(~consolidate_is_field_names(.x) %>% pull(field)) %>% 
#     unlist() %>% setNames(NULL)
################################
################################



is_fields_to_keep <-
    is_fields_chr %>% mutate(pct = round(n / max(n), 2)) %>% 
    filter(pct > 0.03) %>% 
    pull(field) %>% 
    as.character() %>% 
    .[!. %in% c(
        "basic",
        "diluted",
        "selling_expense",
        "general_administrative_expense",
        "basic_and_diluted",
        "amortization"   
    )]
    

is_cleaned_list_final <-
    get_is_cleaned_list(seq_along(is_cleaned)) %>% 
    map(~filter(.x, field %in% is_fields_to_keep))

has_dups_is <- is_cleaned_list_final %>% map_lgl(., ~duplicated(.x$field) %>% any())
is_tickers_with_dup_fields <- has_dups_is[has_dups_is == TRUE] %>% names()
# is_tickers_with_dup_fields


is_new <- 
  is_cleaned_list_final %>% 
  .[!names(.) %in%  is_tickers_with_dup_fields] %>% 
  reduce(., ~suppressMessages(full_join(.x, .y))) %>% 
  pivot_longer(-c(ticker, field, download_date),
               names_to = "date") %>% 
  pivot_wider(names_from = "field", values_from = "value") %>% 
  mutate(date = as.Date(date, "x%Y_%m_%d")) %>% 
  mutate(statement = "1 - is_new")
    

is_cleaned_old <- read_tibble(is_files_cleaned_old %>% max())
# colnames(is_cleaned_old)

is_old <-
    is_cleaned_old %>%
    # rename(
    #     basic_eps = "earnings_per_share_basic",
    #     diluted_eps = "earnings_per_share_diluted",
    #     interest_income_net = "net_interest_income",
    #     interest_expense = "net_interest_paid"
    # ) %>% 
    mutate(statement = "2 - is_old") %>% 
    select(any_of(c("ticker", "date", "download_date", is_fields_to_keep)))
    
     

is_final <-
  is_new %>% 
  full_join(is_old) %>% 
  arrange(ticker, desc(date), desc(download_date), statement) %>%
  group_by(ticker, date) %>% 
  fill(where(is.numeric), .direction = "up") %>% 
  slice(1) %>% 
  select(-statement, -download_date)

cnts <- 
    is_final %>% 
    group_by(ticker, date) %>% 
    count()
if(nrow(cnts %>% filter(n != 1)) >= 1) stop("Duplicate dates!")


fwrite(is_final, paste0("data/cleaned data/graphfund_income_statements_quarterly_cleaned (", str_replace_all(Sys.Date(), "-", " "), ").csv"))
# is_final <- read_tibble(list.files("data/cleaned data", 
#                                    pattern = "income_statements_cleaned",
#                                    full.names = TRUE) %>% max())

# jsonlite::toJSON(is_final %>% slice(1:20))
write(jsonlite::toJSON(is_final), "data/cleaned data/is_quarterly_final.json")
# is_final_json <- jsonlite::fromJSON("data/cleaned data/is_final.json")




# Inspect net_interest_income and net_interest_paid fields:
# cf_cleaned_old %>% 
#     select(ticker, date, net_interest_income, net_interest_paid) %>% 
#     filter(!is.na(net_interest_income) & !is.na(net_interest_paid)) #%>% 
#     filter(net_interest_income < 0)
#     mutate(equal = net_interest_income == net_interest_paid) %>% 
#     filter(equal == TRUE)




# !!!!!!
# # To Do Later:
#     1. compute Gross profit if not available
#         if amortization is available but depreciation or depreciation and amortization is not, then rename amortization "depreciation_amortization"
#     2. Find out how to clean up:
#         interest_expense, 
#         interest_income_net, and 
#         interest_expense_net  
#     3. Make a rule such that if there is a field with "revenue" or 
#        "sales" in the top 3 rows of the table and there are no other 
#        fields containing these names, then call it total_revenue, if 
#        "total_revenue" and "total" don't' exist in top 3 rows
#     4. Test that income_taxes is the right sign. Compute an 
#        income_taxes field (income_before_taxes - net_income) and 
#        compare.
# 
# 
# 
# ticker ABIO has duplicated fields: basic_and_diluted (2x)
# 
# 
# ticker "ACLS" has basic and diluted followed by basic_shares and diluted_shares
# 
# "basic"
# "diluted"
# "basic"
# "diluted"
# 
# "basic"
# "diluted"
# "basic_shares"            "basic_eps"
# "diluted_shares" --or--   "diluted_eps"

# Sometimes gross profit is not present, but it can be computed!!!






# BALANCE SHEET -----------------------------------------------------------


consolidate_bs_field_names <- function(df) {
    ######
    # df <- bs_files_raw[1] %>% add_download_date() %>% clean_field_names()
    ######
    df %>%
        mutate(
            field = case_when(
                ##########
                # Assets #
                ##########
                field %in% c(
                    "cash_and_cash_equivalents",
                    "cash_and_equivalents",
                    "cash_cash_equivalents_and_restricted_cash",
                    "total_cash_and_cash_equivalents",
                    "current_asset_cash"
                ) ~ "cash",
                field %in% c(
                    "short_term_marketable_securities",
                    "trading_securities",
                    "marketable_securities"
                ) ~ "short_term_investments",
                # str_detect(field, # REGEX
                # ) ~ "short_term_investments",
                # field %in% c(
                # ) ~ "cash_and_short_term_investments",
                field %in% c(
                    "accounts_receivable_net",
                    "receivables_net",
                    
                    # !!!! Create a rule such that if there are no allowance_for_... fields, then consider this net of that.
                    # "customer_receivables",
                    # "trade_receivables",
                    "receivables_net_of_allowance",
                    "trade_receivables_net",
                    "trade_accounts_receivable_net",
                    "accounts_receivable"
                ) ~ "receivables_net",
                str_detect(
                    field,
                    # REGEX
                    "accounts_receivable_net_of_allowance.*|accounts_receivable_less_allowance.*|receivables_net_of_allowances.*|trade_receivables_net_of_allowance.*"
                ) ~ "receivables_net",
                field %in% c(
                    "accounts_receivable_allowance_for_doubtful_accounts",
                    "trade_accounts_receivable_allowance",
                    "trade_receivables_allowances_in_dollars",
                    "allowance_for_doubtful_accounts_receivable_current",
                    "receivables_allowances",
                    "trade_accounts_receivable_allowances_in_dollars",
                    "accounts_receivable_allowance",
                    "allowance_for_doubtful_accounts_trade_receivables_current"
                ) ~ "allowance_for_doubtful_accounts",
                # field %in% c() ~ "total_current_assets",
                field %in% c(
                    "inventories",
                    "total_inventory",
                    "inventory_net",
                    "inventories_net"
                ) ~ "inventory",
                str_detect(field, # REGEX
                           "inventory_net_of_allowance.*") ~ "inventory",
                field %in% c("investment_securities_available_for_sale") ~ "securities_available_for_sale",
                str_detect(
                    field,
                    # REGEX
                    "investment_securities_held_to_maturity_net_of_allowance.*"
                ) ~ "securities_held_to_maturity",
                field %in% c(
                    "property_plant_and_equipment_net",
                    "total_property_and_equipment_net",
                    "property_and_equipment_net",
                    "properties_plants_and_equipment_net",
                    "property_and_equipment_net_of_accumulated_depreciation",
                    "net_property_plant_and_equipment",
                    "premises_and_equipment_net",
                    "fixed_assets_net",
                    "net_property_and_equipment"
                ) ~ "property_plant_equipment_net",
                str_detect(
                    field,
                    # REGEX
                    "property_and_equipment_net_of_accumulated_depreciation_of_.*|property_plant_and_equipment_net_of_accumulated_depreciation_of_.*"
                ) ~ "property_plant_equipment_net",
                field %in% c(
                    "less_accumulated_depreciation",
                    "accumulated_depreciation_of_property_and_equipment",
                    "property_plant_and_equipment_owned_accumulated_depreciation",
                    "accumulated_depreciation_and_amortization",
                    "less_accumulated_depreciation_and_amortization",
                    "property_and_equipment_accumulated_depreciation"
                ) ~ "accumulated_depreciation",
                field %in% c(
                    "common_stock_shares_outstanding_in_shares",
                    "common_stock_shares_outstanding",
                    "common_stock_outstanding_shares",
                    "common_stock_outstanding_in_shares",
                    "common_stock_outstanding"
                ) ~ "shares_basic",
                field %in% c(
                    "preferred_stock_shares_outstanding",
                    "preferred_stock_shares_outstanding_in_shares",
                    "preferred_stock_outstanding_in_shares",
                    "preference_shares_shares_outstanding"
                ) ~ "shares_preferred",
                
                ###############
                # Liabilities #
                ###############
                field %in% c(
                    "accounts_payable_and_accrued_expenses",
                    "accounts_payable_and_accrued_liabilities",
                    "trade_accounts_payable",
                    "accounts_payable_and_accrued_expense",
                    "trade_payables",
                    "accounts_payable_and_other_current_liabilities",
                    "accounts_payable_trade"
                ) ~ "accounts_payable",
                # field %in% c(
                # ) ~ "total_current_liabilities",
                field %in% c(
                    "current_portion_of_notes_payable_and_long_term_debt",
                    "current_maturities_of_long_term_debt_and_finance_leases",
                    "current_portion_of_long_term_debt_and_finance_lease_obligations",
                    "current_portion_of_long_term_debt_and_finance_leases",
                    "current_portion_of_long_term_debt_net",
                    "current_maturities_of_long_term_debt",
                    "current_portion_of_long_term_debt_net_of_deferred_finance_costs",
                    "current_portion_of_long_term_debt",
                    "current_maturities_of_debt",
                    "current_portion_of_long_term_debt",
                    "current_maturities_of_long_term_debt_and_finance_lease_obligations",
                    "long_term_debt_current_portion",
                    "current_portion_of_long_term_debt_and_bank_borrowings",
                    "current_portion_of_long_term_borrowings",
                    "debt_current",
                    "current_debt",
                    "current_portion_of_notes_payable",
                    "current_portion_of_notes_payable",
                    "short_term_borrowings"
                ) ~ "short_term_debt",
                str_detect(field, # REGEX
                           "^long_term_debt_due_within_one_year.*$") ~ "short_term_debt",
                field %in% c(
                    "notes_payable_and_long_term_debt_less_current_portion",
                    "long_term_debt_and_finance_lease_obligations",
                    "long_term_debt_and_finance_leases",
                    "long_term_debt_net",
                    "long_term_debt_net_of_current_portion",
                    "long_term_debt_net_of_current_portion_and_deferred_finance_costs_and_other_liabilities",
                    "debt_net_of_current_maturities",
                    "long_term_borrowings",
                    "long_term_debt_less_current_portion",
                    "long_term_debt_less_current_maturities",
                    "long_term_debt_net_of_current_maturities"
                ) ~ "long_term_debt",
                field %in% c(
                    "current_maturities_of_operating_leases",
                    "current_portion_of_lease_liabilities",
                    "current_portion_of_lease_liability",
                    "current_portion_of_obligations_under_operating_leases",
                    "operating_lease_liabilities_current",
                    "operating_lease_liability_short_term",
                    "operating_lease_liabilities_current_portion",
                    "lease_liability_current",
                    "current_maturities_of_operating_lease_obligations",
                    "current_portion_of_lease_obligations",
                    "operating_lease_liability_current",
                    "current_portion_of_operating_lease_liabilities",
                    "current_portion_of_operating_lease_liability",
                    "current_portion_of_operating_lease_liability",
                    "lease_liability_current_portion",
                    "current_lease_liabilities",
                    "operating_lease_liability_current_portion",
                    "current_operating_lease_liabilities"
                ) ~ "short_term_lease_liabilities",
                field %in% c(
                    "operating_lease_liability_net_of_current_portion",
                    "operating_lease_liabilities_noncurrent",
                    "operating_lease_liability_long_term",
                    "operating_lease_liability_noncurrent_portion",
                    "lease_liability_non_current",
                    "operating_lease_liabilities_net_of_current_portion",
                    "non_current_portion_of_lease_liabilities",
                    "long_term_operating_lease_liability",
                    "long_term_operating_lease_liabilities",
                    "lease_obligations_net_of_current_portion",
                    "operating_lease_liability_noncurrent",
                    "lease_liability_net_of_current_portion",
                    "operating_lease_liabilities_less_current_portion",
                    "operating_lease_liabilities_non_current",
                    "non_current_operating_lease_liabilities",
                    "noncurrent_operating_lease_liabilities",
                    "lease_liabilities_net_of_current_portion"
                ) ~ "long_term_lease_liabilities",
                
                # !! there are also finance_lease_obligations !!! file 119
                
                field %in% c(
                  "retained_earnings_deficit",
                  "retained_deficit",
                  "retained_earnings_accumulated_deficit"
                ) ~ "retained_earnings",
                field %in% c(
                    "current_portion_of_deferred_revenue",
                    "deferred_revenue_current_portion",
                    "current_deferred_revenue",
                    "deferred_revenue_current"
                ) ~ "short_term_deferred_revenue",
                field %in% c(
                  "deferred_revenue_less_current_portion",
                  "deferred_revenue_non_current"
                  ) ~ "long_term_deferred_revenue",
                field %in% c("convertible_notes_payable_noncurrent_portion") ~ "long_term_convertible_notes_payable",
                field %in% c(
                    "noncontrolling_interests",
                    "noncontrolling_interest",
                    "non_controlling_interests",
                    "non_controlling_interest"
                ) ~ "minority_interest",
                field %in% c(
                  "commitments",
                  "commitments_and_contingent_liabilities",
                  "contingent_consideration"
                ) ~ "commitments_and_contingencies",
                field %in% c(
                    "total_stockholders_deficit_equity",
                    "total_stockholders_deficit",
                    "total_stockholders_equity_deficit",
                    "total_stockholders_equity",
                    "total_equity",
                    "total_shareholders_equity_deficit"
                ) ~ "total_shareholders_equity",
                str_detect(
                    field,
                    # REGEX
                    "^total_shareholders_equity_available_to.*|total_.*(?=shareholders_deficit).*"
                ) ~ "total_shareholders_equity",
                field %in% c(
                    "total_liabilities_and_stockholders_equity",
                    "total_liabilities_and_stockholders_deficit",
                    "total_liabilities_and_equity",
                    "total_liabilities_and_stockholders_equity_deficit"
                ) ~ "total_liabilities_and_stockholders_equity",
                
                TRUE ~ field
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "total_current_liabilities",
                required_posterior_string = "total_liabilities",
                current_string = "operating_lease_liabilities",
                replacement = "long_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "total_current_liabilities",
                current_string = "^debt$",
                replacement = "long_term_debt"
            ),
            field = rename_field_conditionally(
                x = field,
                required_posterior_string = "total_current_assets",
                current_string = "segregated_cash_and_investments",
                replacement = "short_term_investments"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "accounts_payable",
                required_posterior_string = "total_current_liabilities",
                current_string = "deferred_revenue",
                replacement = "short_term_deferred_revenue"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "accounts_payable",
                required_posterior_string = "total_current_liabilities",
                current_string = "deferred_revenues",
                replacement = "short_term_deferred_revenue"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "total_current_liabilities",
                required_posterior_string = "total_liabilities",
                current_string = "deferred_revenue",
                replacement = "long_term_deferred_revenue"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "accounts_payable",
                required_posterior_string = "total_current_liabilities",
                current_string = "operating_lease_liabilities",
                replacement = "short_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
                x = field,
                required_prior_string = "total_current_liabilities",
                required_posterior_string = "total_liabilities",
                current_string = "operating_lease_liability",
                replacement = "long_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_assets",
              required_posterior_string = "total_current_liabilities",
              current_string = "lease_liabilities",
              replacement = "short_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_current_liabilities",
              required_posterior_string = "total_liabilities",
              current_string = "lease_liability",
              replacement = "long_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_assets",
              required_posterior_string = "total_current_liabilities",
              current_string = "finance_lease_liabilities",
              replacement = "short_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_current_liabilities",
              required_posterior_string = "total_liabilities",
              current_string = "finance_lease_liabilities",
              replacement = "long_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_assets",
              required_posterior_string = "total_current_liabilities",
              current_string = "lease_liability",
              replacement = "short_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "accounts_payable",
              required_posterior_string = "total_current_liabilities",
              current_string = "operating_lease_liability",
              replacement = "short_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "total_current_liabilities",
              required_posterior_string = "total_liabilities",
              current_string = "operating_lease_liability",
              replacement = "long_term_lease_liabilities"
            ),
            field = rename_field_conditionally(
              x = field,
              required_prior_string = "cash",
              required_posterior_string = "total_current_assets",
              current_string = "investments",
              replacement = "short_term_investments"
            ),
            field = rename_total_assets_if_missing(x = field)
        )
}

fields_bs_to_ignore_1 <- function() {
  c("deferred_income_tax_liability_net",
    "other_accrued_liabilities",
    "retained_earnings",
    "accumulated_other_comprehensive_loss_net_of_taxes",
    "other_noncurrent_liabilities",
    "other_long_term_liabilities",
    "noncurrent_insurance_claims",
    "accrued_taxesother_than_income",
    "accrued_compensation",
    "other_noncurrent_assets",
    "right_of_use_assets",
    "common_stock_authorized_in_shares",
    "common_stock_shares_issued_in_shares",
    "common_stock_shares_authorized_in_shares",
    "other_current_assets",
    "other_long_term_assets",
    "preferred_stock_shares_authorized_in_shares",
    "accumulated_deficit",
    "accumulated_other_comprehensive_loss",
    "liabilities_associated_with_assets_held_for_sale",
    "floor_plan_notes_payablenon_trade_net",
    "floor_plan_notes_payabletrade_net",
    "treasury_stock_shares_in_shares",
    "common_stock_shares_issued_in_shares",
    "common_stock_shares_issued",
    "common_stock_issued_shares",
    "common_stock_shares_authorized_in_shares",
    "common_stock_shares_authorized",
    "common_stock_authorized_shares",
    "common_stock_par_value_in_dollars_per_share",
    "common_stock_par_value",
    "common_stock_par_or_stated_value_per_share",
    "preferred_stock_shares_issued",
    "preferred_stock_shares_authorized",
    "preferred_stock_par_value",
    "accrued_compensation_and_employee_benefits",
    "other_assets",
    "right_of_use_asset_operating",
    "costs_incurred_in_excess_of_amounts_billed",
    "accrued_expenses_and_other_liabilities",
    "operating_lease_right_of_use_assets",
    "contracts_in_transit",
    "prepaid_expenses_and_other_current_assets",
    "noncurrent_income_taxes_payable",
    "additional_paid_in_capital",
    "preferred_stock_shares_issued_in_shares",
    "accrued_expenses_and_other_current_liabilities",
    "other_liabilities",
    "redeemable_noncontrolling_interests",
    "deferred_income_taxes",
    "other_non_current_assets",
    "other_current_liabilities",
    "paid_in_capital",
    "other_non_current_liabilities",
    "total_long_term_liabilities",
    "total_non_current_liabilities",
    "discontinued_operations_current_liabilities",
    "other_long_term_assets_net",
    "funds_payable_and_amounts_payable_to_customers",
    "contract_assets",
    "customer_deposits",
    "derivative_liabilities",
    "other_receivables",
    "income_taxes_receivable"
  )
}
fields_bs_to_ignore_2 <- function() {
  c("deferred_revenue_net_of_current_portion",
    "deposits_and_other_assets",
    "reinvested_earnings",
    "other",
    "payables_to_brokerage_customers",
    "construction_in_progress",
    "advance_to_seller",
    "research_and_development_supplies",
    "prepaid_research_and_development",
    "capital_in_excess_of_par_value",
    "other_investments",
    "common_stock_par_value_usd_per_share",
    "unamortized_debt_discounts", 
    "unamortized_debt_premium",
    "common_stock_shares_redemption",
    "deferred_tax_assets_net",
    "prepaid_expenses_and_other_assets",
    "operating_lease_right_of_use_asset_net",
    "acquired_in_process_research_and_development",
    "right_to_use_asset",
    "total_other_assets",
    "common_stock_issued",
    "work_in_progress",
    "deferred_subscriber_acquisition_costs_net",
    "common_stock_and_additional_paid_in_capital",
    "long_term_other_assets",
    "common_stock_authorized",
    "client_funds_obligations",
    "operating_lease_right_of_use_asset",
    "deferred_contract_costs",
    "common_stock_par_value_in_us_per_share",
    "total_current_assets_before_funds_held_for_clients",
    "funds_held_for_clients",
    "preference_shares_shares_issued",
    "preference_shares_par_value_in_dollars_per_share",
    "preference_shares_shares_authorized",
    "deferred_expenses",
    "preferred_stock_liquidation_preference_value"
    )
}
fields_bs_to_ignore_3 <- function() {
  c("total_non_current_assets",
    "bank_owned_life_insurance",
    "loans_held_for_sale",
    "operating_lease_right_of_use_assets_net",
    "cash_and_due_from_banks",
    "prepaid_expenses",
    "accumulated_other_comprehensive_income",
    "total_deposits",
    "deferred_tax_assets",
    "common_stock",
    "investments",
    "common_stock_issued_in_shares",
    "cash_and_due_from_banks",
    "deferred_tax_liabilities",
    "accumulated_other_comprehensive_loss_income",
    "other_intangibles_net",
    "deferred_offering_costs",
    "deferred_tax_liabilities_net",
    "derivative_warrant_liabilities",
    "due_to_related_party",
    "federal_funds_sold",
    "derivative_liability",
    "due_to_related_parties",
    "interest_bearing_deposits",
    "warrant_liabilities",
    "income_tax_receivable",
    "right_of_use_asset",
    "income_tax_payable",
    "prepaid_and_other_current_assets",
    "deferred_tax_asset",
    "other_accrued_expenses",
    "loans",
    "unearned_premiums",
    "allowance_for_loan_losses",
    "right_of_use_assets_net",
    "line_of_credit",
    "other_real_estate_owned",
    "common_stock_par_value_in_usd_per_share",
    "interest_payable",
    "prepaid_expenses_and_other",
    "accrued_and_other_liabilities",
    "deferred_policy_acquisition_costs",
    "deferred_underwriting_commissions",
    "noninterest_bearing",
    "allowance_for_credit_losses",
    "preferred_stock",
    "total_investments",
    "asset_retirement_obligations",
    "deferred_income_tax_liabilities",
    "deferred_tax_liability_net",
    "federal_home_loan_bank_advances",
    "interest_receivable"
  )
}
fields_bs_to_ignore_4 <- function() {
  c("deferred_tax_liability",
    "other_assets_net",
    "dividends_payable",
    "interest_bearing",
    "warrant_liability"
  )
}      
      
field_patterns_bs_to_ignore <- function() {
  c("^common_stock_.*(?=shares_authorized).*(?=shares_issued)|preferred_stock.*(?=shares_authorized).*(?=issued)|other_intangible_assets.*|^total_liabilities_class.*(?=shares).*|^treasury_stock.*|common_stock.*(?=shares_issued).*(?=outstanding).*|common_stock.*(?=par_value).*(outstanding).*|common_shares.*(?=par_value).*(?=authorized).*|^preferred_stock.*(?=authorized).*(?=issued).*|preferred_stock_par.*|common_stock_shares_par.*|income_taxes_payable.*|^common_shares_authorized.*|^preferred_shares_authorized.*|common_shares_shares_issued_in_shares|^common_shares_held_in_treasury.*|^common_shares.*(?=issued).*|^reinsurance.*|preferred_stock_authorized.*|preferred_stock_issued.*|^accrued_interest.*|^deferred_income_taxes.*|preferred_stock_.*_shares_outstanding|^treasury_shares.*|^par_value.*|^accumulated.*income_loss|^preferred_stock.*(?=outstanding)")
}


# bs_cleaned <- readRDS("data/temp/bs_cleaned.rds")
# if(!exists("bs_cleaned")) {
  bs_cleaned <-
    bs_files_raw %>% 
    keep(bs_has_quarterly_dates) %>% 
    map(~add_download_date(.x) %>% clean_field_names())
  tickers_bs <- bs_cleaned %>% map_chr(., ~.x[1, "ticker"] %>% unlist())
  names(bs_cleaned) <- tickers_bs
  saveRDS(bs_cleaned, "data/temp/bs_quarterly_cleaned.rds")
# }

  
get_bs_cleaned_list <- function(id) {
    map(
        bs_cleaned[id],
        ~ consolidate_bs_field_names(.x) %>%
            filter(!field %in% fields_bs_to_ignore_1()) %>%
            filter(!field %in% fields_bs_to_ignore_2()) %>%
            filter(!field %in% fields_bs_to_ignore_3()) %>%
          filter(!field %in% fields_bs_to_ignore_4()) %>%
            filter(!str_detect(field, field_patterns_bs_to_ignore()))
    )
}


# get_bs_cleaned_list(111:120) %>% map(., ~pull(.x, field))


# Common fields
# bs_cleaned_list <- readRDS("data/temp/bs_cleaned_list.rds")
# if(!exists("bs_cleaned_list")) {
    bs_cleaned_list <- get_bs_cleaned_list(seq_along(bs_cleaned))
    saveRDS(bs_cleaned_list, "data/temp/bs_quarterly_cleaned_list.rds")
# }

# Get most common fields
bs_fields_chr <- 
    bs_cleaned_list %>% 
    map(~pull(.x, field)) %>% unlist() %>% as.character() %>% 
    table() %>% as.data.frame() %>% rename(field = ".", n = "Freq") %>% 
    arrange(desc(n))
# bs_fields_chr %>% slice(1:60)

bs_fields_to_keep <-
  bs_fields_chr %>% mutate(pct = round(n / max(n), 2)) %>% 
  filter(pct > 0.03) %>% 
  pull(field) %>% 
  as.character() %>% 
  .[!. %in% c(
    "restricted_cash",
    "short_term_deferred_revenue",
    "accrued_expenses",
    "prepaid_expenses",
    "accumulated_other_comprehensive_income",
    "accrued_liabilities",
    "total_deposits",
    "deferred_tax_assets",
    "cash_and_due_from_banks",
    "deferred_tax_liabilities",
    "common_stock",
    "intangible_assets",
    "common_stock_issued_in_shares",
    "investments",
    "lease_liabilities",
    "notes_payable",
    "long_term_deferred_revenue",
    "operating_lease_liabilities",
    "operating_lease_right_of_use_assets_net"
  )]

bs_cleaned_list_final <-
  get_bs_cleaned_list(seq_along(bs_cleaned)) %>% 
  map(~filter(.x, field %in% bs_fields_to_keep))

has_dups_bs <- bs_cleaned_list_final %>% map_lgl(., ~duplicated(.x$field) %>% any())
bs_tickers_with_dup_fields <- has_dups_bs[has_dups_bs == TRUE] %>% names()
# bs_tickers_with_dup_fields


bs_new <- 
  bs_cleaned_list_final %>% 
  .[!names(.) %in%  bs_tickers_with_dup_fields] %>% 
  reduce(., ~suppressMessages(full_join(.x, .y))) %>% 
  pivot_longer(-c(ticker, field, download_date),
               names_to = "date") %>% 
  pivot_wider(names_from = "field", values_from = "value") %>% 
  mutate(date = as.Date(date, "x%Y_%m_%d")) %>% 
  mutate(statement = "1 - bs_new")


bs_cleaned_old <- read_tibble(bs_files_cleaned_old %>% max())
# colnames(bs_cleaned_old)

bs_old <-
  bs_cleaned_old %>%
  # rename(
  #   diluted_eps = "earnings_per_share_diluted"
  # ) %>%
  mutate(statement = "2 - bs_old") %>%
  select(any_of(c("ticker", "date", "download_date", "statement", bs_fields_to_keep)))


bs_final <-
  bs_new %>%
  full_join(bs_old) %>%
  arrange(ticker, desc(date), desc(download_date), statement) %>%
  group_by(ticker, date) %>%
  fill(where(is.numeric), .direction = "up") %>%
  slice(1) %>%
  select(-statement, -download_date)

# cnts <- 
#   bs_final %>% 
#   group_by(ticker, date) %>% 
#   count()
# if(nrow(cnts %>% filter(n != 1)) >= 1) stop("Duplicate dates!")

fwrite(bs_final, paste0("data/cleaned data/graphfund_balance_sheets_quarterly_cleaned (", str_replace_all(Sys.Date(), "-", " "), ").csv"))
# bs_final <- read_tibble(list.files("data/cleaned data", 
#                                    pattern = "balance_sheets_cleaned",
#                                    full.names = TRUE) %>% max())

# jsonlite::toJSON(bs_final %>% slice(1:20))
write(jsonlite::toJSON(bs_final), "data/cleaned data/bs_quarterly_final.json")
# bs_final_json <- jsonlite::fromJSON("data/cleaned data/bs_final.json")




# !!!!
#   #### TO DO Later ######
# 
#   1. Compute cash_and_short_term_investments if not available 
#      and both items exist separately
#   2. If total_assts doesn't exits but total liabilities and 
#      equity exists, compute total assets'
#   3. If total_assets doesn't exist but total liabilieis and 
#      equity does, then rename as total_assets'
#   4. Check if long_term_lease_liabilites is included in 
#      long_term_debt or total_debt. If not, then can choose to 
#      add it later to create adjusted_long_term_debt






# CASH FLOWS --------------------------------------------------------------

consolidate_cf_field_names <- function(df) {
  ######
  # df <- cf_files_raw[1] %>% add_download_date() %>% clean_field_names()
  ######
  df %>%
    mutate(
      field = case_when(
      
          field %in% c(
            "net_loss",
            "net_earnings_loss",
            "net_income_loss",
            "net_earnings",
            "net_loss_income",
            "net_loss_for_the_period"
          ) ~ "net_income",
        field %in% c(
          "depreciation_and_amortization",
          "provision_for_depreciation_depletion_and_amortization",
          "depreciation_depletion_and_amortization",
          "depreciation_amortization_and_accretion",
          "depreciation_and_amortization_expense",
          "depreciation_and_amortization_of_property_and_equipment",
          "amortization_and_depreciation"
        ) ~ "depreciation_amortization",
        field %in% c(
          "depreciation_expense",
          "depreciation_of_property_and_equipment",
          "depreciation",
          "depreciation_of_premises_and_equipment"
        ) ~ "depreciation",
        field %in% c(
          "amortization_of_intangible_assets",
          "amortization_of_deferred_financing_costs",
          "amortization_of_debt_issuance_costs",
          "amortization_of_right_of_use_assets",
          "amortization_of_intangibles",
          "amortization_of_debt_discount_and_issuance_costs",
          "amortization_of_operating_lease_right_of_use_assets",
          "amortization_of_debt_discount",
          "amortization",
          "amortization_of_debt_discounts",
          "amortization_of_debt_discount_and_debt_issuance_costs",
          "amortization_of_core_deposit_intangible",
          "amortization_of_debt_issuance_costs_and_debt_discount",
          "amortization_of_deferred_loan_costs",
          "amortization_expense"
        ) ~ "amortization",
        field %in% c(
          "share_based_compensation_expense",
          "share_based_compensation",
          "equity_based_compensation",
          "stock_compensation_expense"
        ) ~ "stock_based_compensation",
        field %in% c(
          "purchases_of_marketable_securities",
          "purchases_of_available_for_sale_securities",
          "purchases_of_equity_securities",
          "purchases_of_investment_securities",
          "purchase_of_short_term_investments",
          "purchase_of_marketable_securities",
          "purchases_of_securities_available_for_sale",
          "purchase_of_available_for_sale_securities",
          "purchase_of_investments",
          "purchase_of_securities_available_for_sale"
        ) ~ "purchases_of_short_term_investments",
        field %in% c(
          "proceeds_from_sale_of_investments",
          "proceeds_from_sales_and_maturities_of_investments",
          "maturities_of_short_term_investments",
          "proceeds_from_sales_of_securities_available_for_sale",
          "proceeds_from_maturities_of_marketable_securities",
          "maturities_of_marketable_securities",
          "proceeds_from_maturities_of_investments",
          "sales_of_marketable_securities"
        ) ~ "proceeds_from_sales_and_maturities_of_investments",
        field %in% c(
          "taxes_paid",
          "cash_paid_for_income_taxes_net_of_refunds",
          "cash_paid_during_the_period_for_income_taxes",
          "cash_paid_for_income_taxes_net"
        ) ~ "cash_paid_for_taxes",
        field %in% c(
          "net_cash_used_in_operating_activities",
          "net_cash_from_used_in_continuing_operations",
          "cash_provided_by_operating_activities",
          "net_cash_provided_by_used_in_operating_activities",
          "cash_provided_from_used_for_operations",
          "cash_flows_from_operating_activities",
          "net_cash_provided_by_operating_activities",
          "net_cash_provided_by_used_for_operating_activities",
          "net_cash_from_operating_activities",
          "net_cash_flows_used_in_operations",
          "net_cash_used_by_operating_activities",
          "net_cash_used_in_provided_by_operating_activities",
          "net_cash_flows_from_operating_activities",
          "cash_used_in_operating_activities",
          "net_cash_provided_used_by_operating_activities",
          "net_cash_used_for_operating_activities",
          "net_cash_flows_used_in_operating_activities",
          "net_cash_used_provided_by_financing_activities",
          "cash_provided_by_used_in_operating_activities"
        ) ~ "operating_cash_flows",
        field %in% c(
          "investment_in_property_and_equipment",
          "purchase_of_property_plant_and_equipment",
          "additions_to_property_and_equipment",
          "additions_to_investments",
          "capital_expenditures",
          "acquisitions_of_property_and_equipment",
          "purchases_of_property_plant_and_equipment",
          "additions_to_property_plant_and_equipment",
          "purchases_of_premises_and_equipment",
          "acquisition_of_property_and_equipment",
          "purchases_of_fixed_assets",
          "purchase_of_property_and_equipment",
          "payments_for_capital_expenditures",
          "expenditures_for_property_plant_and_equipment_and_capitalized_software",
          "cash_paid_for_capital_expenditures",
          "acquisition_of_property_plant_and_equipment",
          "purchase_of_fixed_assets",
          "purchase_of_premises_and_equipment",
          "purchase_of_equipment",
          "purchases_of_equipment",
          "expenditures_for_property_plant_and_equipment",
          "purchases_of_property_and_equipment_net"
        ) ~ "purchases_of_property_and_equipment",
        field %in% c(
          "proceeds_from_dispositions_of_property_plant_and_equipment",
          "proceeds_from_the_sale_of_assets",
          "proceeds_from_sale_of_premises_and_equipment",
          "proceeds_from_disposition_of_property_and_other_assets",
          "proceeds_from_the_sale_of_property_and_equipment",
          "proceeds_from_disposal_of_property_and_equipment",
          "proceeds_from_sales_of_premises_and_equipment",
          "proceeds_from_sale_of_property_plant_and_equipment",
          "proceeds_from_sale_of_property_and_equipment",
          "proceeds_from_sale_of_assets",
          "proceeds_from_sales_of_property_and_equipment",
          "proceeds_from_sale_of_fixed_assets",
          "sales_of_property_and_equipment"
        ) ~ "proceeds_from_sale_of_property_and_equipment",
      
      field %in% c(
          "net_cash_used_in_investing_activities",
          "cash_used_in_investing_activities",
          "net_cash_from_used_in_investing_activities",
          "net_cash_provided_by_investing_activities",
          "cash_provided_from_investing_activities",
          "cash_flows_from_investing_activities",
          "net_cash_provided_by_used_for_investing_activities",
          "net_cash_used_for_investing_activities",
          "net_cash_provided_by_used_in_investing_activities",
          "net_cash_used_for_provided_by_investing_activities",
          "net_cash_required_by_investing_activities",
          "net_cash_used_in_provided_by_investing_activities",
          "net_cash_flows_provided_by_used_in_investing_activities",
          "net_cash_provided_used_by_investing_activities",
          "net_cash_flows_from_investing_activities",
          "net_cash_used_by_investing_activities",
          "net_cash_flows_used_in_investing_activities",
          "net_cash_from_investing_activities",
          "net_cash_from_investing_activities",
          "cash_provided_by_used_in_investing_activities",
          "cash_used_for_investing_activities"
        ) ~ "investing_cash_flows",
      field %in% c(
        "proceeds_from_shares_issued_under_stock_plans",
        "proceeds_from_common_stock_offering_net",
        "proceeds_from_the_issuance_of_common_stock",
        "proceeds_from_issuance_of_common_stock_net_of_issuance_costs",
        "proceeds_from_issuance_of_common_stock",
        "proceeds_from_issuance_of_shares",
        "issuance_of_common_stock",
        "net_proceeds_from_issuance_of_common_stock",
        "proceeds_from_issuance_of_common_stock_net"
      ) ~ "proceeds_from_issuance_of_basic_shares",
      field %in% c(
        "purchases_of_common_stock",
        "repurchase_of_common_stock",
        "purchases_of_common_shares",
        "payments_to_repurchase_common_stock",
        "purchases_of_shares",
        "purchase_of_treasury_stock",
        "common_stock_repurchases",
        "common_stock_repurchased",
        "acquisition_of_treasury_stock",
        "purchases_of_treasury_stock",
        "purchase_of_treasury_shares",
        "share_repurchases"
      ) ~ "repurchases_of_basic_shares",
      str_detect(field, # REGEX
                 "^repurchases_of_common_stock.*"
      ) ~ "repurchases_of_basic_shares",
      field %in% c(
      "proceeds_from_issuance_of_long_term_debt",
      "proceeds_from_borrowings_on_debt",
      "borrowings_on_long_term_debt",
      "proceeds_from_borrowings",
      "proceeds_from_long_term_borrowings",
      "proceeds_from_notes_payable",
      "proceeds_from_long_term_debt",
      "borrowings_of_debt",
      "proceeds_from_debt"
      ) ~ "proceeds_from_issuance_of_debt",
      field %in% c(
        "repayments_on_debt",
        "payments_to_retire_debt",
        "payment_of_long_term_debt",
        "repayments_on_long_term_borrowings",
        "repayments_of_long_term_borrowings",
        "principal_payments_on_long_term_debt",
        "payments_of_long_term_debt",
        "repayment_of_debt",
        "repayment_of_long_term_debt",
        "repayments_of_long_term_debt",
        "repayment_of_notes_payable",
        "payments_on_notes_payable",
        "principal_payments_on_finance_lease_obligations",
        "payments_on_debt",
        "repayments_of_notes_payable",
        "principal_payments_on_finance_leases",
        "repayments_of_borrowings",
        "payments_on_long_term_debt"
      ) ~ "repayments_of_debt",
      field %in% c(
        "proceeds_from_exercise_of_warrants",
        "proceeds_from_the_exercise_of_stock_options",
        "proceeds_from_stock_option_exercises",
        "proceeds_from_stock_options_exercised"
      ) ~ "proceeds_from_exercise_of_stock_options",
      field %in% c(
        "cash_dividends_paid",
        "cash_dividends_paid_on_common_stock",
        "common_stock_dividends_paid",
        "cash_dividends",
        "cash_dividends_paid",
        "payments_of_dividends",
        "dividends_paid_on_common_stock",
        "dividends_paid",
        "dividend_payments",
        "payment_of_dividends",
        "dividends_paid_to_shareholders"
      ) ~ "cash_dividends",
        field %in% c(
          "cash_used_in_provided_by_financing_activities",
          "net_cash_from_used_in_financing_activities",
          "net_cash_provided_by_financing_activities",
          "cash_provided_from_used_for_financing_activities",
          "cash_flows_from_financing_activities",
          "net_cash_used_in_financing_activities",
          "net_cash_provided_by_used_for_financing_activities",
          "net_cash_used_for_financing_activities",
          "net_cash_provided_by_used_in_financing_activities",
          "net_cash_used_in_provided_by_financing_activities",
          "net_cash_used_in_provided_by_financing_activities",
          "net_cash_flows_provided_by_financing_activities",
          "net_cash_provided_by_required_by_financing_activities",
          "net_cash_flows_from_financing_activities",
          "net_cash_used_for_provided_by_financing_activities",
          "net_cash_provided_used_by_financing_activities",
          "net_cash_from_financing_activities",
          "cash_provided_by_financing_activities",
          "net_cash_used_by_financing_activities",
          "cash_used_in_financing_activities",
          "net_cash_flows_used_in_financing_activities"
          ) ~ "financing_cash_flows",
        field %in% c(
          "consolidated_cash_and_cash_equivalents_end_of_the_period",
          "cash_and_cash_equivalents",
          "cash_and_cash_equivalents_and_restricted_cash_at_end_of_period",
          "cash_and_equivalents_end_of_period",
          "cash_and_cash_equivalents_as_of_end_of_period",
          "cash_and_cash_equivalents_at_end_of_period",
          "cash_and_cash_equivalents_end_of_period",
          "cash_and_cash_equivalents_end_of_the_quarter",
          "cash_and_cash_equivalents_end_of_year",
          "cash_at_end_of_period",
          "cash_at_end_of_the_period",
          "cash_cash_equivalents_and_restricted_cash_at_end_of_period",
          "cash_cash_equivalents_and_restricted_cash_at_the_end_of_period",
          "cash_and_cash_equivalents_at_end_of_year",
          "cash_end_of_period",
          "cash_cash_equivalents_and_restricted_cash_end_of_period",
          "cash_ending",
          "cash_and_cash_equivalents_at_end_of_the_period",
          "cash_and_cash_equivalentsend_of_period",
          "cash_and_cash_equivalents_ending",
          "total_cash_cash_equivalents_and_restricted_cash",
          "cash_end_of_the_period",
          "cash_and_cash_equivalents_at_the_end_of_the_period",
          "cash_cash_equivalents_restricted_cash_and_restricted_cash_equivalents",
          "cash_and_cash_equivalents_and_restricted_cash_end_of_period",
          "cash_and_cash_equivalents_at_the_beginning_of_the_period",
          "cash_and_restricted_cash_end_of_period",
          "cash_cash_equivalents_and_restricted_cash_at_the_end_of_the_period",
          "cash_and_restricted_cash_at_end_of_period",
          "cash_and_cash_equivalents_end_of_the_period"
        ) ~ "cash",
      field %in% c(
        "decrease_increase_in_cash_and_cash_equivalents",
        "net_increase_decrease_in_cash_and_cash_equivalents",
        "net_change_in_cash_and_cash_equivalents",
        "net_increase_in_cash_and_restricted_cash",
        "change_in_cash_and_cash_equivalents",
        "net_increase_decrease_in_cash_cash_equivalents_and_restricted_cash",
        "net_change_in_cash_and_equivalents",
        "net_decrease_increase_in_cash_and_cash_equivalents",
        "net_increase_in_cash_and_cash_equivalents",
        "increase_decrease_in_cash_and_cash_equivalents",
        "increase_decrease_in_cash_and_cash_equivalents_including_cash_classified_within_current_assets_held_for_sale",
        "net_decrease_increase_in_cash_cash_equivalents_and_restricted_cash",
        "net_change_in_cash_cash_equivalents_and_restricted_cash",
        "net_change_in_cash",
        "net_decrease_in_cash_cash_equivalents_and_restricted_cash",
        "net_increase_decrease_in_cash_and_cash_equivalents_and_restricted_cash",
        "increase_decrease_in_cash_and_restricted_cash",
        "net_decrease_in_cash_and_cash_equivalents",
        "net_increase_in_cash_cash_equivalents_and_restricted_cash",
        "net_decrease_in_cash",
        "change_in_cash_cash_equivalents_and_restricted_cash",
        "net_increase_in_cash_and_cash_equivalents_and_restricted_cash",
        "net_decrease_increase_in_cash",
        "net_increase_decrease_in_cash",
        "net_increase_in_cash",
        "increase_in_cash_and_cash_equivalents",
        "increase_decrease_in_cash_cash_equivalents_and_restricted_cash",
        "increase_in_cash_cash_equivalents_and_restricted_cash",
        "net_decrease_increase_in_cash_and_cash_equivalents_and_restricted_cash",
        "decrease_in_cash_and_cash_equivalents",
        "net_decrease_in_cash_and_cash_equivalents_and_restricted_cash",
        "cash_cash_equivalents_restricted_cash_and_restricted_cash_equivalents_period_increase_decrease_including_exchange_rate_effect",
        "net_change_in_cash_and_cash_equivalents_and_restricted_cash",
        "net_increase_decrease_in_cash_and_restricted_cash",
        "decrease_increase_in_cash_cash_equivalents_and_restricted_cash",
        "net_change_in_cash_and_restricted_cash"
      ) ~ "change_in_cash",
      field %in% c(
        "cash_and_cash_equivalents_at_beginning_of_period",
        "cash_and_cash_equivalents_at_beginning_of_year",
        "cash_cash_equivalents_and_restricted_cash_at_beginning_of_year",
        "cash_cash_equivalents_and_restricted_cash_beginning_of_period",
        "cash_cash_equivalents_and_restricted_cash_at_beginning_of_period",
        "cash_and_cash_equivalents_beginning_of_period",
        "cash_and_cash_equivalents_at_the_beginning_of_period",
        "cash_and_restricted_cash_beginning_of_year",
        "cash_at_beginning_of_period",
        "cash_beginning_of_period",
        "cash_cash_equivalents_and_restricted_cash_at_beginning_of_the_period",
        "cash_cash_equivalents_and_restricted_cash_at_start_of_period",
        "cash_beginning_of_the_period",
        "cash_and_cash_equivalents_and_restricted_cash_beginning_of_period",
        "cash_and_cash_equivalents_beginning_of_year",
        "cash_beginning_of_year",
        "cash_and_cash_equivalents_and_restricted_cash_at_beginning_of_period",
        "cash_and_cash_equivalents_at_beginning_of_the_period",
        "cash_and_cash_equivalents_at_beginning_of_the_year",
        "cash_and_restricted_cash_beginning_of_period"
      ) ~ "cash_beginning",
      field %in% c(
        "effect_of_exchange_rate_changes_on_cash",
        "effect_of_exchange_rate_changes_on_cash_and_cash_equivalents",
        "effect_of_currency_exchange_rate_changes_on_cash_cash_equivalents_and_restricted_cash",
        "effect_of_exchange_rates_on_cash_and_cash_equivalents",
        "effect_of_exchange_rate_on_cash",
        "effect_of_exchange_rates_on_cash",
        "exchange_rate_effects",
        "effect_of_foreign_exchange_rate_changes_on_cash_and_cash_equivalents",
        "effect_of_foreign_exchange_rate_changes_on_cash_cash_equivalents_and_restricted_cash"
      ) ~ "exchange_rate_effects_on_cash",
      str_detect(field, # REGEX
        "effect(s?)_of_exchange_rate_changes.*"
      ) ~ "exchange_rate_effects_on_cash",
        TRUE ~ field
      )
    )
}

fields_cf_to_ignore_1 <- function() {
  c("cash_paid_for_interest",
    "other_net",
    "leasing_commissions",
    "decrease_in_insurance_reserves_and_policyholder_funds",
    "amortization_of_deferred_acquisition_costs",
    "right_of_use_lease_assets_recognized_operating_leases",
    "other_non_current_liabilities",
    "other_non_current_assets",
    "other_current_assets",
    "accrued_liabilities",
    "change_in_other_assets",
    "change_in_security_deposits_payable",
    "accounts_payable",
    "operating_right_of_use_asset",
    "inventories",
    "prepaid_income_tax",
    "accounts_payable_and_accrued_expenses",
    "non_cash_lease_expense",
    "operating_lease_right_of_use_assets_and_liabilities",
    "customer_deposits_and_advance_payments",
    "income_tax_receivable",
    "other_changes_net",
    "decrease_in_accounts_payable_and_accrued_expenses",
    "income_tax_receivable",
    "compensation_expense_related_to_share_awards",
    "other",
    "notes_receivable",
    "shares_withheld_for_tax_payments",
    "income_taxes",
    "exercise_of_stock_options",
    "debt_issuance_costs",
    "income_taxes_paid",
    "net_cash_from_discontinued_operations",
    "net_cash_used_in_continuing_operations",
    "net_income_from_discontinued_operations",
    "income_loss_from_continuing_operations",
    "other_financing_activities",
    "deferred_financing_costs",
    "other_investing_activities",
    "inventory",
    "other_assets_and_liabilities",
    "increase_in_receivables",
    "increase_decrease_in_inventories",
    "increase_decrease_in_noncurrent_assets",
    "decrease_in_accounts_payable_trade",
    "increase_decrease_in_other_liabilities",
    "income_tax_assets_and_liabilities_net",
    "deferred_income_taxes",
    "capitalized_software_development_costs"
  )
}
fields_cf_to_ignore_2 <- function() {
  c("deferred_taxes",
    "other_liabilities",
    "deferred_revenue",
    "proceeds_from_redemption_of_auction_rate_security",
    "right_of_use_assets",
    "interest",
    "trade_receivables",
    "changes_in_operating_assets_and_liabilities",
    "other_noncurrent_assets",
    "long_term_lease_liabilities",
    "other_noncurrent_liabilities",
    "total_adjustments",
    "other_long_term_assets_and_liabilities_net",
    "deferred_underwriting_commissions",
    "non_cash_licensed_technology_impairment_charge",
    "operating_lease_liabilities",
    "change_in_right_of_use_asset",
    "other_adjustments_net",
    "contracts_in_transit",
    "cash_paid_for_taxes",
    "change_attributable_to_other_operating_activities",
    "stock_based_compensation_expense",
    "net_cash_provided_by_continuing_operating_activities",
    "other_assets",
    "cashless_warrant_exercises",
    "other_non_cash_operating_activities",
    "increase_decrease_in_accounts_payable",
    "deferred_rent_and_other_liabilities",
    "net_change_in_operating_leases",
    "cash_used_to_pay_taxes",
    "due_to_a_stockholder",
    "other_payables_and_accrued_expenses",
    "cash_paid_for_income_taxes",
    "increase_decrease_in_accounts_receivable"
    )
}
fields_cf_to_ignore_3 <- function() {
  c("advances_from_related_parties",
    "net_change_in_deposits",
    "net_increase_decrease_in_other_assets",
    "net_gains_on_sale_of_sba_loans",
    "donated_securities",
    "net_increase_decrease_in_other_assets",
    "net_increase_decrease_in_accrued_expenses_and_other_liabilities",
    "tenant_receivable_related_party",
    "operating_lease_liability",
    "related_party_payables",
    "other_payables",
    "option_expense",
    "interest_and_other_receivables",
    "operating_lease_right_of_use_assets",
    "increase_decrease_in_other_assets",                        
    "increase_decrease_in_receivables",
    "increase_decrease_in_other_current_assets",
    "increase_decrease_in_advance_billings",
    "increase_decrease_in_accrued_liabilities",
    "cash_paid_received_for_taxes",
    "other_non_cash_income_and_expenses",
    "deferred_tax",
    "increase_in_inventory",
    "decrease_in_deferred_revenue",
    "decrease_in_operating_lease_liability",
    "noncash_interest_expense",
    "deferred_contract_acquisition_costs",
    "deferred_revenue_and_due_to_customers",
    "premiums_receivable",
    "deferred_acquisition_costs",
    "reinsurance_balances_payable",
    "other_items_net",
    "increase_in_inventory",
    "decrease_in_deferred_revenue",
    "decrease_in_operating_lease_liability"
  )
}
fields_cf_to_ignore_4 <- function() {
  c("investment_in_unconsolidated_joint_ventures",
    "return_of_investment_in_unconsolidated_joint_ventures",
    "distribution_of_earnings_from_unconsolidated_joint_ventures",
    "interest_earned_on_investment_held_in_trust_account",
    "prepaid_assets",
    "state_franchise_tax_accrual",
    "contract_liabilities",
    "other_long_term_liabilities",
    "payments_of_offering_costs",
    "other_current_and_noncurrent_assets_and_liabilities",
    "gain_on_property_dispositions_and_impairment_losses_net",
    "lifo_expense",
    "accrued_liabilities_other",
    "accrued_clinical_liabilities",
    "accrued_clinical_liabilities",
    "non_cash_royalty_revenue_related_to_royalty_monetization",
    "revaluation_of_contingent_consideration",
    "change_in_acquired_contingent_consideration_obligation",
    "non_cash_royalty_revenue",
    "deferred_tax_provision_benefit",
    "change_in_derivative_liability",
    "decrease_in_accounts_receivable",
    "increase_decrease_in_prepaid_expenses_and_other_current_assets",
    "decrease_in_accounts_payable_accrued_expenses_and_other_current_liabilities",
    "decrease_in_other_non_current_liabilities",
    "net_amortization_of_investment_securities_premiums",
    "decrease_increase_in_accrued_interest_receivable",
    "decrease_in_accrued_interest_payable",
    "mortgage_loans_originated_for_sale",
    "proceeds_from_sales_of_loans_originated_for_sale",
    "increase_in_deferred_tax_expense",
    "increase_in_other_liabilities",
    "deferred_tax_expense_benefit",
    "other_current_and_non_current_assets",
    "deferred_revenues_current_and_non_current",
    "accrued_payroll_and_related_benefits",
    "other_current_and_non_current_liabilities",
    "other_accrued_liabilities"
  )
}
fields_cf_to_ignore_5 <- function() {
  c(
    #(includes pay-in-kind and capitalized interest expense amortization)
    "non_cash_interest_expense", 
    "contract_assets",
    "inventories_net",
    "loss_on_extinguishment_of_debt",
    "lease_liabilities",
    "other_current_liabilities",
    "bad_debt_expense",
    "accrued_interest",
    "deferred_income_tax_benefit",
    "deferred_income_tax_expense_benefit",
    "net_increase_in_deposits",
    "other_current_liabilities",
    "bad_debt_expense",
    "accrued_interest",
    "other_non_cash_items",
    "accrued_and_other_liabilities",
    "noncash_lease_expense",
    "other_financing_activities_net",
    "other_investing_activities_net",
    "distributions_to_noncontrolling_interests",
    "loss_on_disposal_of_property_and_equipment",
    "accrued_interest_receivable",
    "customer_deposits",
    "income_tax_payable",
    "trade_receivables_net",
    "provision_for_credit_losses",
    "accrued_interest_payable",
    "payment_of_debt_issuance_costs",
    "provision_for_deferred_income_taxes",
    "purchase_of_intangible_assets",
    "taxes_paid_related_to_net_share_settlement_of_equity_awards",
    "increase_in_other_assets",
    "deferred_income_taxes_net",
    "proceeds_from_revolving_credit_facility",
    "originations_of_loans_held_for_sale",
    "taxes_paid_related_to_net_share_settlement_of_equity_awards",
    "deferred_income_tax_expense",
    "increase_in_accounts_receivable",
    "prepaid_and_other_assets",
    "other_operating_activities",
    "payment_of_offering_costs",
    "decrease_in_other_liabilities",
    "deferred_tax_benefit",
    "decrease_increase_in_other_assets",
    "net_change_in_loans",
    "decrease_in_other_assets",
    "deposits",
    "mpairment_charges",
    "interest_receivable",
    "net_increase_in_loans",
    "gain_on_sale_of_loans",
    "increase_decrease_in_accounts_payable_and_accrued_expenses",
    "increase_in_inventories",
    "loss_on_debt_extinguishment"
  )
}
fields_cf_to_ignore_6 <- function() {
  c(
    "deferred_revenues",
    "other_receivables",
    "equity_based_compensation_expense",
    "lease_liability",
    "unearned_premiums",
    "prepaid_and_other_current_assets",
    "net_increase_decrease_in_deposits",
    "other_long_term_assets",
    "payments_of_debt_issuance_costs",
    "impairment_charges",
    "accretion_of_debt_discount",
    "increase_decrease_in_prepaid_expenses",
    "increase_in_accounts_payable_and_accrued_expenses",
    "other_non_cash_items_net",
    "right_of_use_assets_obtained_in_exchange_for_new_operating_lease_liabilities",
    "non_cash_operating_lease_expense",
    "accounts_and_other_receivables",
    "accrued_and_other_current_liabilities",
    "accrued_capital_expenditures",
    "operating_leases",
    "purchases",
    "interest_paid",
    "loss_on_disposal_of_fixed_assets", 
    "net_increase_decrease_in_loans", 
    "offering_costs_paid", 
    "other_adjustments", 
    "other_invested_assets",
    "other_non_cash_adjustments",
    "earnings_on_bank_owned_life_insurance",
    "equity_securities",
    "increase_in_accounts_payable",
    "increase_in_accrued_interest_receivable",
    "deferred_income_tax",
    "deferred_income_tax_benefit_expense",
    "provision_for_bad_debts",
    "provision_for_loan_losses", 
    "provision_for_doubtful_accounts",
    "taxes",
    "loss_on_disposal_of_assets",
    "merchandise_inventories",
    "net_decrease_increase_in_loans",
    "change_in_accounts_receivable",
    "gain_loss_on_sale_of_assets",
    "allowance_for_doubtful_accounts",
    "bank_premises_and_equipment",
    "leased_assets_obtained_in_exchange_for_new_operating_lease_liabilities",
    "deferred_rent",
    "available_for_sale",
    "decrease_increase_in_accounts_receivable",
    "decrease_increase_in_inventories",
    "due_to_related_party",
    "other_accrued_expenses",
    "other_investments",
    "other_operating_assets_and_liabilities",
    "payment_tax_withholding_share_based_payment_arrangement"
  )
}

field_patterns_cf_to_ignore <- function() {
  c("^net_change_in_accounts_payable.*|^payments_of_tax_withholding.*|^accounts_receivable|^decrease_in_receivables.*|^decrease_in_prepaid_expenses.*|^decrease_in_taxes.*|^prepaid_expenses.*|^receivable.*|^payment_of_taxes.*|^accounts_payable.*|^change_in_fair_value_of.*|^upfront_costs.*|^income_taxes.*|^investments_in_.*(?=options).*|^accrued_expenses.*|^equity_in_net.*|^net_cash_provided.*(?=continuing_operations).*|^net_cash_provided.*(?=discontinued_operations).*|^income_from_discontinued.*|^trade_accounts_receivable.*|^income_from_unconsolidated.*|^trade_accounts_payable.*|^insurance_claims.*|^accrued_compensation.*|financing_of_energy.*|^floor_plan.*|^change_in_payable.*|^gains_on_disposition.*|^proceeds_from_payroll.*|^imputed_interest.*|^interest_payable.*")
}

# cf_cleaned <- readRDS("data/temp/cf_cleaned.rds")
# if(!exists("cf_cleaned")) {
  cf_cleaned <-
    cf_files_raw %>% 
    keep(cf_has_quarterly_dates) %>% 
    map(~add_download_date(.x) %>% clean_field_names())
  tickers_cf <- cf_cleaned %>% map_chr(., ~.x[1, "ticker"] %>% unlist())
  names(cf_cleaned) <- tickers_cf
  saveRDS(cf_cleaned, "data/temp/cf_quarterly_cleaned.rds")
# }

get_cf_cleaned_list <- function(id) {
  map(
    cf_cleaned[id],
    ~ consolidate_cf_field_names(.x) %>%
      filter(!field %in% fields_cf_to_ignore_1()) %>%
      filter(!field %in% fields_cf_to_ignore_2()) %>%
      filter(!field %in% fields_cf_to_ignore_3()) %>%
      filter(!field %in% fields_cf_to_ignore_4()) %>%
      filter(!field %in% fields_cf_to_ignore_5()) %>%
      filter(!field %in% fields_cf_to_ignore_6()) %>%
      filter(!str_detect(field, field_patterns_cf_to_ignore())) %>% 
      consolidate_dep_amor() %>% 
      consolidate_dep() %>% 
      consolidate_amor()
    )
}

# get_cf_cleaned_list(81:90) %>% map(., ~pull(.x, field))


# Common fields
# cf_cleaned_list <- readRDS("data/temp/cf_cleaned_list.rds")
# if(!exists("cf_cleaned_list")) {
    cf_cleaned_list <- get_cf_cleaned_list(seq_along(cf_cleaned))
    saveRDS(cf_cleaned_list, "data/temp/cf_quarterly_cleaned_list.rds")
# }

cf_fields_chr <- 
  cf_cleaned_list %>% 
  map(~pull(.x, field)) %>% unlist() %>% as.character() %>% 
  table() %>% as.data.frame() %>% rename(field = ".", n = "Freq") %>% 
  arrange(desc(n))

  

cf_fields_to_keep <-
  cf_fields_chr %>% mutate(pct = round(n / max(n), 2)) %>% 
  filter(pct > 0.03) %>% 
  pull(field) %>% 
  as.character() %>% 
  .[!. %in% c(
    "amortization",
    "depreciation",
    "dividends_paid",
    "interest_paid",
    "proceeds_from_exercise_of_stock_options",
    "restricted_cash",
    "amortization_of_debt_discount",
    "purchases_of_marketable_securities",
    "provision_for_credit_losses",
    "acquisitions_net_of_cash_acquired",
    "end_of_period"
  )]


cf_cleaned_list_final <-
  get_cf_cleaned_list(seq_along(cf_cleaned)) %>% 
  map(~filter(.x, field %in% cf_fields_to_keep))

has_dups_cf <- cf_cleaned_list_final %>% map_lgl(., ~duplicated(.x$field) %>% any())
cf_tickers_with_dup_fields <- has_dups_cf[has_dups_cf == TRUE] %>% names()
# cf_tickers_with_dup_fields


cf_new <- 
  cf_cleaned_list_final %>% 
  .[!names(.) %in% cf_tickers_with_dup_fields] %>% 
  reduce(., ~suppressMessages(full_join(.x, .y))) %>% 
  pivot_longer(-c(ticker, field, download_date),
               names_to = "date") %>% 
  pivot_wider(names_from = "field", values_from = "value") %>% 
  mutate(date = as.Date(date, "x%Y_%m_%d")) %>% 
  mutate(statement = "1 - cf_new")

cf_cleaned_old <- read_tibble(cf_files_cleaned_old %>% max())
# colnames(cf_cleaned_old)


cf_old <-
  cf_cleaned_old %>%
  # rename(
  #   operating_cash_flows = "net_cash_provided_by_operating_activities",
  #   investing_cash_flows = "net_cash_provided_by_investing_activities",
  #   financing_cash_flows = "net_cash_provided_by_financing_activities",
  #   cash = "cash_and_cash_equivalents",
  #   basic_eps = "earnings_per_share_basic",
  #   diluted_eps = "earnings_per_share_diluted"
  # ) %>%
  mutate(statement = "2 - cf_old") %>%
  select(any_of(c("ticker", "date", "download_date", "statement", bs_fields_to_keep)))


cf_final <-
  cf_new %>% 
  full_join(cf_old) %>% 
  arrange(ticker, desc(date), desc(download_date), statement) %>%
  group_by(ticker, date) %>%
  fill(where(is.numeric), .direction = "up") %>%
  slice(1) %>%
  select(-statement, -download_date)

# cnts <-
#   cf_final %>%
#   group_by(ticker, date) %>%
#   count()
# if(nrow(cnts %>% filter(n != 1)) >= 1) stop("Duplicate dates!")


fwrite(cf_final, paste0("data/cleaned data/graphfund_cash_flows_quarterly_cleaned (", str_replace_all(Sys.Date(), "-", " "), ").csv"))
# cf_final <- read_tibble(list.files("data/cleaned data",
#                                    pattern = "cash_flows_cleaned",
#                                    full.names = TRUE) %>% max())

# jsonlite::toJSON(cf_final %>% slice(1:20))
write(jsonlite::toJSON(cf_final), "data/cleaned data/cf_quarterly_final.json")
# cf_final_json <- jsonlite::fromJSON("data/cleaned data/cf_final.json")


# -------------------------------------------------------------------------

end <- Sys.time(); end - start


























# -------------------------------------------------------------------------







# !!! Sometimes there are multiple depreciation and amortization fields, so
# consolidate them into one if there isn't' a total
# 
# 
# !!!! sometimes there are multiple instances of a changed field. Find tickers with duplicate fields to see where the field name changes were innappopriate
# 
# 
# If there is a depreciation field but no amortization field, then call it depreciation_amortization, and vice-versa
# 
# 
# sometimes, "end_of_period" or "end_of_the_period" exists and likely represents "cash". If it exists after "financing_cash_flows" and "cash" doesn't' already exist, convert to "cash"



































































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


