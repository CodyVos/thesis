#–– LIBRARIES & DATA PREP ---------------------------------------------------
library(caret)
library(ranger)
library(pROC)
library(dplyr)
library(tidyr)

# 1) Prepare df_cv with factor target
df_cv <- final_df %>%
  mutate(
    is_rst = factor(ifelse(is_rst == 1, "Yes", "No"), levels = c("No","Yes"))
  )

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
emb_cols    <- paste0("Emb", 1:64)
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
    formula  = is_rst ~ at + sic2,
    tuneGrid = expand.grid(
      mtry          = c(1, 2),        # √p ≈ √2 ≈ 1.4 → try 1,2
      splitrule     = "gini",
      min.node.size = c(1, 5, 10)
    )
  ),
  Combined   = list(
    formula  = as.formula(
      paste("is_rst ~", paste(c(emb_cols, "at", "sic2"), collapse = " + "))
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
  
  # 5) CV Results & Plot
  print(rf_cv)
  plot(rf_cv, main = paste(nm, "– ROC vs. mtry"))
  
  # 6) Best Parameters & CV AUC
  best_params <- rf_cv$bestTune
  best_auc    <- max(rf_cv$results$ROC)
  cat("\nBest hyperparameters for", nm, ":\n")
  print(best_params)
  cat("Cross‐validated AUC:", round(best_auc, 3), "\n\n")
  
  # 7) Held‐out predictions (pooled)
  preds <- rf_cv$pred
  # filter to best‐tuned combo if multiple rows
  if (!is.null(best_params$mtry)) {
    preds <- preds %>%
      filter(
        mtry          == best_params$mtry,
        splitrule     == best_params$splitrule,
        min.node.size == best_params$min.node.size
      )
  }
  
  # 8) Confusion Matrix & Pooled AUC
  cm <- confusionMatrix(
    factor(preds$pred, levels = c("No","Yes")),
    factor(preds$obs,  levels = c("No","Yes")),
    positive = "Yes"
  )
  print(cm)
  
  roc_obj <- roc(preds$obs, preds$Yes)
  cat("Pooled CV AUC (recomputed):", round(auc(roc_obj), 3), "\n")
}

#–– 9) Feature Importance for Combined Model -------------------------------
library(tibble)
vi <- varImp(results$Combined, scale = FALSE)$importance %>%
  rownames_to_column("feature") %>%
  mutate(
    block = case_when(
      str_starts(feature, "Emb") ~ "Embedding",
      feature == "at"            ~ "Assets",
      feature == "sic2"          ~ "Industry",
      TRUE                        ~ "Other"
    )
  )

vi_summary <- vi %>%
  group_by(block) %>%
  summarize(total = sum(Overall)) %>%
  mutate(pct = total / sum(total) * 100)

cat("\nFeature Importance Summary (Combined Model):\n")
print(vi_summary)
#–– 10) Prepare pooled predictions for DeLong tests -------------------------

# Extract the pooled “Yes” probabilities for each model, aligned on the same rows
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

# Align all three on the same rowIndex & Resample
preds_all <- preds_fin %>%
  inner_join(preds_emb, by = c("rowIndex","Resample","obs")) %>%
  inner_join(preds_cmb, by = c("rowIndex","Resample","obs"))

#–– 11) Paired DeLong tests ------------------------------------------------

roc_fin <- roc(preds_all$obs, preds_all$FinP)
roc_emb <- roc(preds_all$obs, preds_all$EmbP)
roc_cmb <- roc(preds_all$obs, preds_all$CmbP)

test_fin_vs_emb <- roc.test(roc_fin, roc_emb, paired = TRUE, method = "delong")
test_cmb_vs_fin <- roc.test(roc_cmb, roc_fin, paired = TRUE, method = "delong")
test_cmb_vs_emb <- roc.test(roc_cmb, roc_emb, paired = TRUE, method = "delong")

cat("\n=== DeLong’s Test for AUC Differences ===\n\n")

cat("Financials vs. Embeddings:\n")
print(test_fin_vs_emb)
cat("\nCombined vs. Financials:\n")
print(test_cmb_vs_fin)
cat("\nCombined vs. Embeddings:\n")
print(test_cmb_vs_emb)
