#–– 0) PACKAGES & WRDS SETUP ----------------------------------------
library(DBI)
library(RPostgres)
library(dplyr)
library(dbplyr)
library(stringr)
library(lubridate)
library(readr)
library(purrr)
library(MatchIt)

# 0a) WRDS connection
con <- dbConnect(
  Postgres(),
  host     = "wrds-pgdata.wharton.upenn.edu",
  port     = 9737,
  dbname   = "wrds",
  user     = Sys.getenv("WRDS_USER"),
  password = Sys.getenv("WRDS_PASS"),
  sslmode  = "require"
)

#–– 1) PULL & CLEAN RESTATEMENTS (2015–2024) --------------------------------
filings <- tbl(con, Id("audit_audit_comp","f39_restatement_filings")) %>%
  transmute(filing_key   = restatement_filing_key,
            notif_key    = restatement_notification_fkey,
            ftp_file     = ftp_file_name_fkey,
            restate_date = file_date)

periods <- tbl(con, Id("audit_audit_comp","f39_restatement_periods")) %>%
  filter(type == "Y") %>%
  transmute(filing_key = restatement_filing_fkey,
            notif_key  = restatement_notification_fkey,
            year)

rst <- inner_join(filings, periods, by = c("filing_key","notif_key")) %>%
  collect()

# 1b) Pull total assets & derive CIK
feed39 <- tbl(con, Id("audit_audit_comp","feed39_financial_restatements")) %>%
  select(ftp_file = ftp_file_name_fkey,
         company_fkey,
         at           = matchfy_balsh_assets) %>%
  collect()

rst <- rst %>%
  left_join(feed39, by = "ftp_file") %>%
  mutate(cik = as.character(as.integer(company_fkey)))

# 1c) Filter & dedupe one record per firm‐year
clean_rst <- rst %>%
  filter(year %in% 2015:2024,
         nzchar(cik), !is.na(at), at > 0) %>%
  group_by(cik, year) %>%
  slice_min(restate_date, with_ties = FALSE) %>%
  ungroup()

message("Restatements 2015–2024: ", nrow(clean_rst))

# 1d) Pull Compustat company master for CIK→GVKEY & SIC2
comp_co <- tbl(con, Id("comp","company")) %>%
  select(gvkey, cik, sic) %>%
  collect() %>%
  mutate(
    cik  = as.character(as.integer(cik)),        # strip leading zeros
    sic2 = floor(as.numeric(sic)/100)
  ) %>%
  select(-sic)

#–– 2) FETCH FUNDAMENTALS & CALCULATE RATIOS ------------------------------
# 2a) Year‐end fundamentals 2015–2024
funda_raw <- tbl(con, Id("comp","funda")) %>%
  filter(datadate >= as.Date("2015-01-01"),
         datadate <= as.Date("2024-12-31")) %>%
  select(gvkey, datadate,
         at,    # assets
         sale,  # sales
         ni,    # net income
         rect,  # receivables
         act,   # current assets
         lct,   # current liabilities
         invt,  # inventory
         dltt, dlc, # long-term + current debt
         ceq    # common equity
  ) %>%
  collect()

# 2b) Keep most-recent filing per gvkey-year, attach SIC2
funda_sub <- funda_raw %>%
  arrange(gvkey, datadate) %>%
  group_by(gvkey, year = year(datadate)) %>%
  slice_max(datadate, with_ties = FALSE) %>%
  ungroup() %>%
  left_join(comp_co %>% select(gvkey, sic2), by="gvkey") %>%
  filter(!is.na(sic2), at > 0)

# 2c) Compute richer financial ratios
funda_ratios <- funda_sub %>%
  arrange(gvkey, year) %>%
  group_by(gvkey) %>%
  mutate(
    roa                       = ni / at,
    debt_to_equity            = (dltt + dlc) / ceq,
    current_ratio             = act / lct,
    quick_ratio               = (act - invt) / lct,
    total_accruals_to_assets  = ((at - lag(at)) - (ni - lag(ni))) / at,
    sales_growth              = (sale - lag(sale)) / lag(sale),
    receivables_growth        = (rect - lag(rect)) / lag(rect),
    asset_turnover            = sale / at
  ) %>%
  ungroup() %>%
  select(gvkey, year, sic2, at,
         roa, debt_to_equity,
         current_ratio, quick_ratio,
         total_accruals_to_assets,
         sales_growth, receivables_growth,
         asset_turnover)

#–– 2d) Map restatement sample to GVKEY & SIC2 -----------------------------
rest2 <- clean_rst %>%
  left_join(comp_co %>% select(gvkey, cik, sic2), by="cik") %>%
  filter(!is.na(gvkey))

#–– 2e) Build rest_pairs & control_pool -----------------------------------
rest_pairs   <- rest2 %>% select(gvkey, year)
control_pool <- anti_join(funda_ratios, rest_pairs, by = c("gvkey","year"))

#–– 2f) Build df_rst (restaters) with all ratios ----------------------------
df_rst <- rest2 %>%
  # keep only the restatement key variables + sic2 from rest2
  select(gvkey, year, sic2) %>%
  # join in all the funda_ratios *except* their own sic2
  inner_join(
    funda_ratios %>% select(-sic2),
    by = c("gvkey", "year")
  ) %>%
  transmute(
    gvkey, 
    year, 
    sic2,            # carried forward from rest2
    at,              # pulled from funda_ratios
    log_at            = log(at),
    roa, 
    debt_to_equity,
    current_ratio, 
    quick_ratio,
    total_accruals_to_assets,
    sales_growth, 
    receivables_growth,
    asset_turnover,
    is_rst            = 1L
  )


#–– 2g) Build df_ctrl (controls) with same features ------------------------
df_ctrl <- control_pool %>%
  transmute(
    gvkey, year, sic2, at,
    log_at                   = log(at),
    roa, debt_to_equity,
    current_ratio, quick_ratio,
    total_accruals_to_assets,
    sales_growth, receivables_growth,
    asset_turnover,
    is_rst                   = 0L
  )

#–– 2h) Combine and nearest-neighbor match on year + SIC2 + log_at ---------
df_match <- bind_rows(df_rst, df_ctrl)

m.out <- matchit(
  is_rst ~ log_at,
  data   = df_match,
  method = "nearest",
  ratio  = 1,
  exact  = ~ year + sic2
)
matched   <- match.data(m.out)
rest_firms<- matched %>% filter(is_rst == 1)
ctrl_firms<- matched %>% filter(is_rst == 0)

message("Matched ",
        nrow(rest_firms), " restatements ↔ ",
        nrow(ctrl_firms), " controls")

#–– 2i) SAVE & DISCONNECT ---------------------------------------------------
write_csv(rest_firms, "matched_restatement_firms.csv")
write_csv(ctrl_firms,   "matched_control_firms.csv")
dbDisconnect(con)
