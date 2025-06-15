#–– 6) EXTRACT POLICIES FROM SEC NOTES DATASETS -----------------------------
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

# 6a) Read matched CIK/year
matched <- bind_rows(
  read_csv("matched_restatement_firms.csv", col_types = cols()) %>% 
    transmute(gvkey, year, is_rst = 1L),
  read_csv("matched_control_firms.csv",     col_types = cols()) %>% 
    transmute(gvkey, year, is_rst = 0L)
)

# 6b) Get CIK for each gvkey
con <- dbConnect(
  Postgres(),
  host     = "wrds-pgdata.wharton.upenn.edu",
  port     = 9737,
  dbname   = "wrds",
  user     = Sys.getenv("WRDS_USER"),
  password = Sys.getenv("WRDS_PASS"),
  sslmode  = "require"
)
comp_map <- tbl(con, Id("comp","company")) %>%
  select(gvkey, cik) %>%
  collect() %>%
  mutate(
    cik = str_pad(as.character(as.integer(cik)), width = 10, pad = "0")
  )
dbDisconnect(con)

# 6c) Build our input df of (cik, year, is_rst)
df_input <- matched %>%
  left_join(comp_map, by = "gvkey") %>%
  filter(!is.na(cik)) %>%
  transmute(cik, year, is_rst)

unique_ciks  <- df_input$cik  %>% unique()
unique_years<- df_input$year %>% unique()

# 6d) Loop through all ZIPs, extract only the SignificantAccountingPoliciesTextBlock rows
all_zips <- list.files(pattern = "_notes\\.zip$")
if (file.exists("policies_raw.csv")) file.remove("policies_raw.csv")

for (z in all_zips) {
  cat("Processing", z, "...\n")
  contents <- unzip(z, list = TRUE)$Name
  
  subf <- grep("sub\\.tsv$", contents, value=TRUE)[1]
  txtf <- grep("txt\\.tsv$", contents, value=TRUE)[1]
  if (is.na(subf) || is.na(txtf)) next
  
  # read submissions
  sub_df <- read_tsv(unz(z, subf),
                     col_types = cols(
                       adsh = col_character(),
                       cik  = col_character(),
                       fy   = col_integer(),
                       .default = col_skip()
                     )) %>%
    mutate(cik = str_pad(as.character(as.integer(cik)), width = 10, pad = "0")) %>%
    filter(cik %in% unique_ciks, fy %in% unique_years) %>%
    select(adsh, cik, fy)
  if (nrow(sub_df)==0) next
  
  # chunk‐read the TXT file
  cb <- SideEffectChunkCallback$new(function(df, pos) {
    df %>%
      filter(tag == "SignificantAccountingPoliciesTextBlock") %>%
      rename(text_block = value) %>%
      inner_join(sub_df, by = "adsh") %>%
      transmute(cik, year = fy, adsh, text_block) %>%
      { if (nrow(.)>0) write_csv(., "policies_raw.csv", append = file.exists("policies_raw.csv")) }
  })
  
  read_tsv_chunked(unz(z, txtf), cb, chunk_size = 50000,
                   col_types = cols(
                     adsh    = col_character(),
                     tag     = col_character(),
                     version = col_character(),
                     qtrs    = col_integer(),
                     iprx    = col_integer(),
                     lang    = col_character(),
                     dcml    = col_integer(),
                     durp    = col_double(),
                     datp    = col_double(),
                     dimh    = col_character(),
                     dimn    = col_integer(),
                     coreg   = col_character(),
                     escaped = col_logical(),
                     srclen  = col_integer(),
                     txtlen  = col_integer(),
                     footnote= col_character(),
                     context = col_character(),
                     value   = col_character()
                   ))
}
cat("Finished writing policies_raw.csv\n")


#–– 7) DEDUPLICATE & JOIN BACK ---------------------------------------------
pol_raw <- read_csv("policies_raw.csv", col_types=cols())

pol_clean <- pol_raw %>%
  mutate(text_len = nchar(text_block)) %>%
  group_by(cik, year) %>%
  slice_max(text_len, with_ties=FALSE) %>%
  ungroup() %>%
  select(cik, year, text_block)

final_df <- df_input %>%
  left_join(pol_clean, by=c("cik","year")) %>%
  left_join(clean_rst %>% select(cik, year, at), by=c("cik","year"))


