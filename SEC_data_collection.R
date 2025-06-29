#–– 1) EXTRACT POLICIES FROM SEC NOTES DATASETS -----------------------------
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
library(glue)

# 1a) Read matched gvkey/year from AA output
matched <- bind_rows(
  read_csv("matched_restatement_firms.csv", col_types = cols()) %>%
    transmute(gvkey, year, is_rst = 1L),
  read_csv("matched_control_firms.csv",     col_types = cols()) %>%
    transmute(gvkey, year, is_rst = 0L)
)

# 1b) Retrieve & zero-pad CIKs
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
  mutate(cik = str_pad(as.character(as.integer(cik)), 10, "left", "0"))
dbDisconnect(con)

# 1c) Build the SEC‐input df
df_input <- matched %>%
  left_join(comp_map, by="gvkey") %>%
  filter(!is.na(cik)) %>%
  select(gvkey, cik, year, is_rst)

unique_ciks  <- unique(df_input$cik)
unique_years <- unique(df_input$year)

# 1d) Locate all “_notes.zip” bundles
all_zips <- list.files(path=".", pattern="notes\\.zip$",
                       recursive=TRUE, full.names=TRUE)
if (length(all_zips)==0) stop("No *notes.zip files found under ", getwd())

# 1e) Remove any old CSV
raw_csv <- file.path(getwd(), "policies_raw.csv")
if (file.exists(raw_csv)) file.remove(raw_csv)

# 1f) Loop to extract only the policy text blocks
for (z in all_zips) {
  cat("Processing", z, "…\n")
  files <- unzip(z, list=TRUE)$Name
  subf <- grep("sub\\.tsv$", files, value=TRUE)[1]
  txtf <- grep("txt\\.tsv$", files, value=TRUE)[1]
  if (is.na(subf) || is.na(txtf)) next
  
  # 1f1) Read submission index
  sub_df <- read_tsv(unz(z, subf),
                     col_types = cols_only(
                       adsh = col_character(),
                       cik  = col_character(),
                       fy   = col_integer()
                     )) %>%
    mutate(cik = str_pad(as.character(as.integer(cik)), 10, "left", "0")) %>%
    filter(cik %in% unique_ciks, fy %in% unique_years) %>%
    select(adsh, cik, fy)
  if (nrow(sub_df)==0) next
  
  # 1f2) Chunk‐read the txt file, keep only policy blocks
  cb <- SideEffectChunkCallback$new(function(df, pos) {
    df %>%
      filter(tag=="SignificantAccountingPoliciesTextBlock") %>%
      rename(text_block = value) %>%
      inner_join(sub_df, by="adsh") %>%
      transmute(cik, year=fy, adsh, text_block) %>%
      { if (nrow(.)>0) write_csv(., raw_csv, append=TRUE) }
  })
  read_tsv_chunked(unz(z, txtf), cb, chunk_size=50000,
                   col_types=cols(
                     adsh     = col_character(),
                     tag      = col_character(),
                     version  = col_character(),
                     qtrs     = col_integer(),
                     iprx     = col_integer(),
                     lang     = col_character(),
                     dcml     = col_integer(),
                     durp     = col_double(),
                     datp     = col_double(),
                     dimh     = col_character(),
                     dimn     = col_integer(),
                     coreg    = col_character(),
                     escaped  = col_logical(),
                     srclen   = col_integer(),
                     txtlen   = col_integer(),
                     footnote = col_character(),
                     context  = col_character(),
                     value    = col_character()
                   ))
}
cat("Finished writing all policy blocks to CSV.\n")

#–– 2) DEDUPLICATE & CLEAN TEXT BLOCKS -----------------------------------
pol_raw <- read_csv(raw_csv, show_col_types=FALSE)

# name columns (3 or 4 col file)
if (ncol(pol_raw)==3) {
  colnames(pol_raw) <- c("cik","year","text_block")
} else if (ncol(pol_raw)==4) {
  colnames(pol_raw) <- c("cik","year","adsh","text_block")
} else {
  stop("policies_raw.csv has ", ncol(pol_raw), " columns — expected 3 or 4.")
}

pol_clean <- pol_raw %>%
  mutate(text_len = nchar(text_block)) %>%
  group_by(cik, year) %>%
  slice_max(text_len, with_ties=FALSE) %>%
  ungroup() %>%
  select(cik, year, text_block)

policies <- pol_clean %>%
  inner_join(df_input %>% select(cik, year), by=c("cik","year"))

#–– 3) READ IN YOUR FINANCIAL RATIOS FROM AA -----------------------------
df_financials <- bind_rows(
  read_csv("matched_restatement_firms.csv", col_types = cols()),
  read_csv("matched_control_firms.csv",     col_types = cols())
)

#–– 4) ASSEMBLE final_dataset_with_text_embeddings_and_assets.csv ----------
final_df <- df_input %>%
  left_join(df_financials, by = c("gvkey","year","is_rst")) %>%  # bring in all ratios + SIC2 + at
  left_join(policies,      by = c("cik","year")) %>%            # attach text_block
  filter(!is.na(text_block)) %>%                                # drop any missing policy text
  mutate(sic2 = as.factor(sic2))

#–– 5) SANITY‐CHECK & SAVE ----------------------------------------------
final_df %>% summarize(
  total_obs    = n(),
  missing_at   = sum(is.na(at)),
  missing_text = sum(is.na(text_block)),
  missing_sic2 = sum(is.na(sic2))
) %>% print()

write_csv(final_df,
          "final_dataset_with_text_embeddings_and_assets.csv",
          na = "")
