#–– PACKAGES & SETUP -------------------------------------------------------
library(dplyr)
library(readr)
library(stringr)
library(openai)
library(purrr)
library(tibble)

# 0b) OpenAI key check and re-connect to WRDS
if (!nzchar(Sys.getenv("OPENAI_API_KEY"))) {
  stop("Please set OPENAI_API_KEY in your environment (e.g. in ~/.Renviron) and restart R.")
}

# Re‐connect to pull SIC codes
con2 <- dbConnect(
  Postgres(),
  host    = "wrds-pgdata.wharton.upenn.edu",
  port    = 9737,
  dbname  = "wrds",
  user    = Sys.getenv("WRDS_USER"),
  password= Sys.getenv("WRDS_PASS"),
  sslmode = "require"
)

comp_sic2 <- tbl(con2, Id("comp","company")) %>%
  select(cik, sic) %>%
  collect() %>%
  mutate(
    # drop leading zeros and re‐pad to ten digits for consistency
    cik  = str_pad(as.character(as.integer(cik)), width = 10, pad = "0"),
    sic2 = floor(as.numeric(sic)/100)
  ) %>%
  select(cik, sic2)

dbDisconnect(con2)

#–– 8) EMBEDDINGS -----------------------------------------------------------
# only keep those with text
policies <- final_df %>%
  filter(!is.na(text_block) & str_length(text_block)>=10)

# safe truncation (≈14k chars)
TRUNCATE_CHARS <- 14000L
truncate_to_safe <- function(txt) if (nchar(txt)>TRUNCATE_CHARS) substr(txt,1,TRUNCATE_CHARS) else txt

# prepare embedding column
policies$policy_embedding <- NA_character_

# helper
get_one_embedding <- function(txt, idx=NA_integer_) {
  txt2 <- truncate_to_safe(txt)
  resp <- tryCatch(
    create_embedding(model="text-embedding-ada-002", input=txt2),
    error = function(e) e
  )
  if (inherits(resp,"error")) return(NA_character_)
  emb <- resp$data$embedding[[1]]
  if (!is.numeric(emb)) return(NA_character_)
  paste(emb, collapse=",")
}

# loop
for (i in seq_len(nrow(policies))) {
  if (i%%100==1) message("Embedding ",i,"/",nrow(policies))
  policies$policy_embedding[i] <- get_one_embedding(policies$text_block[i], i)
}

# save
write_csv(policies, "all_firms_with_policy_embeddings.csv", na="")
message("All done: embeddings in all_firms_with_policy_embeddings.csv")


# 8b) Build restatement assets table
rst_assets <- clean_rst %>%
  select(cik, year, at) %>%
  distinct()

# 8c) Build control assets table
ctrl_assets <- funda_sub %>%
  inner_join(comp_map %>% select(gvkey, cik), by = "gvkey") %>%
  select(cik, year, at) %>%
  distinct()

# 8d) Assemble final_df
final_df <- df_input %>%
  left_join(rst_assets,  by = c("cik","year")) %>%
  left_join(ctrl_assets, by = c("cik","year"), suffix = c(".rst",".ctrl")) %>%
  mutate(at = coalesce(at.rst, at.ctrl)) %>%
  select(cik, year, is_rst, at) %>%
  left_join(
    policies %>% select(cik, year, text_block, policy_embedding),
    by = c("cik","year")
  ) %>%
  # Pull in sic2
  left_join(comp_sic2, by = "cik") %>%
  # drop any rows with missing policy text or at
  filter(!is.na(text_block))

final_df <- final_df %>%
  filter(!is.na(at))
  
# 8e) Save it
write_csv(final_df, "final_dataset_with_text_embeddings_and_assets.csv")
message("✅ final_df is ready!")



#–– 2) DEFINE EXPECTED EMBEDDING DIMENSION --------------------------------
# e.g. embedding-ada-002 returns 1536‐length vectors
expected_dim <- str_count(final_df$policy_embedding[1], ",") + 1

#–– 3) FILTER TO ONLY VALID EMBEDDINGS ------------------------------------
df_emb <- final_df %>%
  # drop NA and those that don’t split to exactly expected_dim numbers
  filter(
    !is.na(policy_embedding),
    str_count(policy_embedding, ",") + 1 == expected_dim
  )

#–– 4) PARSE EMBEDDINGS INTO A NUMERIC MATRIX -----------------------------
emb_list <- str_split(df_emb$policy_embedding, pattern = ",")
emb_mat  <- matrix(
  as.numeric(unlist(emb_list)),
  ncol   = expected_dim,
  byrow  = TRUE
)

#–– 5) TAKE FIRST 64 EMBEDDING DIMENSIONS ----------------------------------
emb64 <- emb_mat[, 1:64, drop = FALSE]
colnames(emb64) <- paste0("Emb", seq_len(ncol(emb64)))

#–– 6) BIND BACK TO df_emb -----------------------------------------------
df_final_emb64 <- bind_cols(
  df_emb,
  as_tibble(emb64)
)

final_df <- df_final_emb64

final_df$sic2 <- as.factor(final_df$sic2)


