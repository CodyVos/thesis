#–– PACKAGES & SETUP -------------------------------------------------------
library(dplyr)
library(readr)
library(stringr)
library(openai)
library(purrr)
library(tibble)
library(tidyr)

#–– 0) READ THE FULL “TEXT + RATIOS” DATASET -------------------------------
final_df <- read_csv("final_dataset_with_text_embeddings_and_assets.csv")

#–– 1) FILTER TO FIRMS WITH VALID POLICY TEXT ------------------------------
policies <- final_df %>%
  filter(!is.na(text_block) & str_length(text_block) >= 10)

#–– 2) OPENAI KEY CHECK ----------------------------------------------------
if (!nzchar(Sys.getenv("OPENAI_API_KEY"))) {
  stop("Please set OPENAI_API_KEY in your environment and restart R.")
}

#–– 3) GENERATE RAW EMBEDDINGS ---------------------------------------------
# Truncate long text to ~14k chars to fit within embedding limits
TRUNCATE_CHARS <- 14000L
truncate_to_safe <- function(txt) {
  if (nchar(txt) > TRUNCATE_CHARS) substr(txt, 1, TRUNCATE_CHARS) else txt
}

# Initialize embedding column
policies$policy_embedding <- NA_character_

# Helper to call OpenAI and collapse into comma‐string
get_one_embedding <- function(txt) {
  txt2 <- truncate_to_safe(txt)
  res  <- tryCatch(
    create_embedding(model = "text-embedding-ada-002", input = txt2),
    error = function(e) NULL
  )
  if (is.null(res)) return(NA_character_)
  emb <- res$data$embedding[[1]]
  paste(emb, collapse = ",")
}

# Loop with progress messages
for (i in seq_len(nrow(policies))) {
  if (i %% 100 == 1) message("Embedding ", i, "/", nrow(policies))
  policies$policy_embedding[i] <- get_one_embedding(policies$text_block[i])
}

# Save the raw embeddings alongside *all* other columns
write_csv(policies,
          "policies_with_embeddings.csv",
          na = "")

#–– 4) PARSE & TRUNCATE EMBEDDINGS INTO 64 DIMENSIONS ----------------------
# Re‐load to be safe
df_raw <- read_csv("policies_with_embeddings.csv", show_col_types = FALSE)

# Compute expected embedding length
expected_dim <- str_count(df_raw$policy_embedding[1], ",") + 1

df_filtered <- df_raw %>%
  filter(
    !is.na(policy_embedding),
    str_count(policy_embedding, ",") + 1 == expected_dim
  )

# Split into numeric and form matrix
emb_list <- str_split(df_filtered$policy_embedding, ",")
emb_mat  <- matrix(
  as.numeric(unlist(emb_list)),
  ncol   = expected_dim,
  byrow  = TRUE
)

# Keep only first 64 dims
emb64 <- emb_mat[, 1:64, drop = FALSE]
colnames(emb64) <- paste0("Emb", seq_len(ncol(emb64)))

#–– 5) BIND BACK & DROP ANY INCOMPLETE ROWS -------------------------------
df_final <- bind_cols(
  df_filtered %>% select(-policy_embedding),
  as_tibble(emb64)
) %>%
  drop_na()  # this will remove any rows missing text, at, ratios or embeddings

#–– 6) SAVE THE COMPLETE MODEL-READY DATASET ------------------------------
write_csv(df_final,
          "final_dataset_with_64embeddings.csv",
          na = "")
