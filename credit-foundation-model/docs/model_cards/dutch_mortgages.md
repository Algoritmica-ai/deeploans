# Model Card — Dutch Mortgages 30M

**Status:** TEMPLATE (deliverable). **Intended use:** credit embeddings for default/prepay/
cure scoring on ESMA Annex 2 Dutch RMBS panels. **Architecture:** three-branch encoder, 30M.
**Training data:** synthetic `Algoritmica/green-lion-2024-2025` (500k loans × 24 months).
**Eval:** ROC-AUC/PR-AUC lift over XGBoost on `default_6m`. **Limitations:** trained on
synthetic data; not for production credit decisions without revalidation. **Audit trail:**
git commit + W&B run id recorded at training time.
