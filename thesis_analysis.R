#–– LIBRARIES & DATA PREP ---------------------------------------------------
library(readr)
library(caret)
library(ranger)
library(pROC)
library(dplyr)
library(tidyr)
library(stringr)
library(tibble)

# 1) Load the fully-embedded, ratio-augmented dataset
final_df <- read_csv("final_dataset_with_64embeddings.csv")


#–– 1a) Fix variable types before CV ----------------------------------------
df_prepped <- final_df %>%
  mutate(
    # Outcome as factor
    is_rst = factor(ifelse(is_rst == 1 | is_rst == "Yes", "Yes", "No"),
                    levels = c("No","Yes")),
    # Industry code as factor
    sic2   = as.factor(sic2),
    # Year as integer (optional)
    year   = as.integer(year)
  ) %>%
  # Ensure all financial ratios and assets are numeric
  mutate(across(
    c(at,
      roa,
      debt_to_equity,
      current_ratio,
      quick_ratio,
      total_accruals_to_assets,
      sales_growth,
      receivables_growth,
      asset_turnover),
    as.numeric
  )) %>%
  # Ensure embedding dimensions are numeric
  mutate(across(starts_with("Emb"), as.numeric))

#–– 1b) Build df_cv (drop IDs, keep predictors + target) --------------------
df_cv <- df_prepped %>%
  select(-cik, -gvkey)  # drop identifier columns

# 2) 5‐fold stratified CV control
set.seed(2025)
cv_ctrl <- trainControl(
  method          = "cv",
  number          = 5,
  classProbs      = TRUE,
  summaryFunction = twoClassSummary,
  savePredictions = "final",
  verboseIter     = TRUE
)

# 3) Define predictor sets and tune grids

# Your new financial ratios plus assets & industry
fin_cols <- c(
  "at",
  "sic2",
  "roa",
  "debt_to_equity",
  "current_ratio",
  "quick_ratio",
  "total_accruals_to_assets",
  "sales_growth",
  "receivables_growth",
  "asset_turnover"
)

# The first-64 embedding dimensions
emb_cols <- paste0("Emb", 1:64)

models_spec <- list(
  Embeddings = list(
    formula  = as.formula(paste("is_rst ~", paste(emb_cols, collapse = " + "))),
    tuneGrid = expand.grid(
      mtry          = c(5, 10, 20, 40, 64),
      splitrule     = "gini",
      min.node.size = c(1, 5, 10)
    )
  ),
  Financials = list(
    formula  = as.formula(paste("is_rst ~", paste(fin_cols, collapse = " + "))),
    tuneGrid = expand.grid(
      # √p ≈ √10 ≈ 3 → try around that
      mtry          = c(2, 3, 5),
      splitrule     = "gini",
      min.node.size = c(1, 5, 10)
    )
  ),
  Combined   = list(
    formula  = as.formula(
      paste("is_rst ~", paste(c(emb_cols, fin_cols), collapse = " + "))
    ),
    tuneGrid = expand.grid(
      mtry          = c(5, 10, 20, 40, 64),
      splitrule     = "gini",
      min.node.size = c(1, 5, 10)
    )
  )
)

#–– 4) Train & Evaluate All Three Models ------------------------------------
results <- list()

for (nm in names(models_spec)) {
  cat("\n\n===== MODEL:", nm, "=====\n")
  spec <- models_spec[[nm]]
  
  set.seed(2025)
  rf_cv <- train(
    spec$formula,
    data       = df_cv,
    method     = "ranger",
    metric     = "ROC",
    num.trees  = 500,
    tuneGrid   = spec$tuneGrid,
    trControl  = cv_ctrl,
    importance = "impurity"
  )
  
  results[[nm]] <- rf_cv
  
  # 4a) CV Results & Plot
  print(rf_cv)
  plot(rf_cv, main = paste(nm, "– ROC vs. mtry"))
  
  # 4b) Best hyperparameters & CV AUC
  best_params <- rf_cv$bestTune
  best_auc    <- max(rf_cv$results$ROC)
  cat("\nBest hyperparameters for", nm, ":\n")
  print(best_params)
  cat("Cross‐validated AUC:", round(best_auc, 3), "\n\n")
  
  # 4c) Held‐out predictions (pooled)
  preds <- rf_cv$pred
  if (!is.null(best_params$mtry)) {
    preds <- preds %>%
      filter(
        mtry          == best_params$mtry,
        splitrule     == best_params$splitrule,
        min.node.size == best_params$min.node.size
      )
  }
  
  # 4d) Confusion Matrix & Pooled AUC
  cm <- confusionMatrix(
    factor(preds$pred, levels = c("No","Yes")),
    factor(preds$obs,  levels = c("No","Yes")),
    positive = "Yes"
  )
  print(cm)
  
  roc_obj <- roc(preds$obs, preds$Yes)
  cat("Pooled CV AUC (recomputed):", round(auc(roc_obj), 3), "\n")
}

#–– 5) Feature Importance for Combined Model -------------------------------
vi <- varImp(results$Combined, scale = FALSE)$importance %>%
  rownames_to_column("feature") %>%
  mutate(
    block = case_when(
      str_starts(feature, "Emb")                                      ~ "Embedding",
      feature == "at"                                                 ~ "Assets",
      feature == "sic2"                                               ~ "Industry",
      feature %in% fin_cols                                           ~ "Financial Ratios",
      TRUE                                                            ~ "Other"
    )
  )

vi_summary <- vi %>%
  group_by(block) %>%
  summarize(total = sum(Overall), .groups = "drop") %>%
  mutate(pct = total / sum(total) * 100)

cat("\nFeature Importance Summary (Combined Model):\n")
print(vi_summary)

#–– 6) Prepare pooled predictions for DeLong tests -------------------------
preds_emb <- results$Embeddings$pred %>%
  filter(
    mtry          == results$Embeddings$bestTune$mtry,
    splitrule     == results$Embeddings$bestTune$splitrule,
    min.node.size == results$Embeddings$bestTune$min.node.size
  ) %>%
  select(rowIndex, Resample, obs, EmbP = Yes)

preds_fin <- results$Financials$pred %>%
  filter(
    mtry          == results$Financials$bestTune$mtry,
    splitrule     == results$Financials$bestTune$splitrule,
    min.node.size == results$Financials$bestTune$min.node.size
  ) %>%
  select(rowIndex, Resample, obs, FinP = Yes)

preds_cmb <- results$Combined$pred %>%
  filter(
    mtry          == results$Combined$bestTune$mtry,
    splitrule     == results$Combined$bestTune$splitrule,
    min.node.size == results$Combined$bestTune$min.node.size
  ) %>%
  select(rowIndex, Resample, obs, CmbP = Yes)

preds_all <- preds_fin %>%
  inner_join(preds_emb, by = c("rowIndex","Resample","obs")) %>%
  inner_join(preds_cmb, by = c("rowIndex","Resample","obs"))

#–– 7) Paired DeLong tests -------------------------------------------------
roc_fin <- roc(preds_all$obs, preds_all$FinP)
roc_emb <- roc(preds_all$obs, preds_all$EmbP)
roc_cmb <- roc(preds_all$obs, preds_all$CmbP)

test_fin_vs_emb <- roc.test(roc_fin, roc_emb, paired = TRUE, method = "delong")
test_cmb_vs_fin <- roc.test(roc_cmb, roc_fin, paired = TRUE, method = "delong")
test_cmb_vs_emb <- roc.test(roc_cmb, roc_emb, paired = TRUE, method = "delong")

cat("\n=== DeLong’s Test for AUC Differences ===\n\n")
cat("Financials vs. Embeddings:\n"); print(test_fin_vs_emb)
cat("\nCombined vs. Financials:\n");      print(test_cmb_vs_fin)
cat("\nCombined vs. Embeddings:\n");     print(test_cmb_vs_emb)
