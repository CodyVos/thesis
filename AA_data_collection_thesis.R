#–– 0) PACKAGES & WRDS SETUP ----------------------------------------
library(DBI)
library(RPostgres)
library(dplyr)
library(dbplyr)
library(stringr)
library(lubridate)
library(readr)
library(purrr)
library(openai)
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
# 1a) Get restatement filings + fiscal‐year periods
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

# 1b) Pull assets & company_fkey → derive CIK
feed39 <- tbl(con, Id("audit_audit_comp","feed39_financial_restatements")) %>%
  select(ftp_file = ftp_file_name_fkey,
         company_fkey,
         at           = matchfy_balsh_assets) %>%
  collect()

rst <- rst %>%
  left_join(feed39, by = "ftp_file") %>%
  mutate(cik = as.character(as.integer(company_fkey)))

# 1c) Filter & dedupe
clean_rst <- rst %>%
  filter(year %in% 2015:2024,
         nzchar(cik), !is.na(at), at > 0) %>%
  group_by(cik, year) %>%
  slice_min(restate_date, with_ties = FALSE) %>%
  ungroup()

message("Restatements 2015–2024: ", nrow(clean_rst))

#–– 2) MATCH TO Compustat GVKEY & SIC2 -------------------------------------
# 2a) Pull company master
comp_co <- tbl(con, Id("comp","company")) %>%
  select(gvkey, cik, sic) %>%
  collect() %>%
  mutate(cik  = as.character(as.integer(cik)),
         sic2 = floor(as.numeric(sic)/100)) %>%
  select(-sic)

# 2b) Join → get gvkey & sic2 on restatements
rest2 <- clean_rst %>%
  left_join(comp_co, by = "cik")

message("Mapped to gvkey: ", sum(!is.na(rest2$gvkey)), "/", nrow(rest2))

#–– 3) BUILD FUNDAMENTALS UNIVERSE & CONTROL POOL --------------------------
year_min <- 2015; year_max <- 2024
start_date <- sprintf("%04d-01-01", year_min)
end_date   <- sprintf("%04d-12-31", year_max)

funda_raw <- tbl(con, Id("comp","funda")) %>%
  filter(datadate >= as.Date(start_date),
         datadate <= as.Date(end_date)) %>%
  select(gvkey, datadate, at) %>%
  collect()

funda_sub <- funda_raw %>%
  mutate(year = year(datadate)) %>%
  group_by(gvkey, year) %>%
  slice_max(datadate, with_ties = FALSE) %>%
  ungroup() %>%
  left_join(comp_co %>% select(gvkey, sic2), by="gvkey") %>%
  filter(!is.na(sic2), at > 0) %>%
  select(gvkey, year, sic2, at)

# exclude restatements
rest_pairs   <- rest2 %>% filter(!is.na(gvkey)) %>% select(gvkey, year)
control_pool <- anti_join(funda_sub, rest_pairs, by = c("gvkey","year"))

#–– 4) EXACT MATCH (YEAR,SIC2) & NEAREST log(AT) ----------------------------
df_rst  <- rest2 %>%
  filter(!is.na(gvkey), !is.na(sic2)) %>%
  transmute(gvkey, year, sic2, log_at = log(at), is_rst = 1L)

df_ctrl <- control_pool %>%
  transmute(gvkey, year, sic2, log_at = log(at), is_rst = 0L)

df_match <- bind_rows(df_rst, df_ctrl)

m.out <- matchit(is_rst ~ log_at,
                 data   = df_match,
                 method = "nearest",
                 ratio  = 1,
                 exact  = ~ year + sic2)
matched   <- match.data(m.out)
rest_firms<- matched %>% filter(is_rst==1)
ctrl_firms<- matched %>% filter(is_rst==0)

message("Matched ", nrow(rest_firms), " restatements ↔ ", nrow(ctrl_firms), " controls")

#–– 5) SAVE & CLEANUP ------------------------------------------------------
write_csv(rest_firms, "matched_restatement_firms.csv")
write_csv(ctrl_firms,   "matched_control_firms.csv")

dbDisconnect(con)
