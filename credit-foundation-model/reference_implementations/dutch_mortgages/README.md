# Reference implementation — Dutch mortgages (validation set)

Synthetic Dutch RMBS panel (ESMA Annex 2, 500k loans × 24 monthly cutoffs) used as the
controlled **validation/ablation** set. It carries a hidden `_segment` latent (eval-only, never
a feature) that drives a 16–32× default spread invisible to tabular features — the ceiling the
foundation model is meant to break.

```bash
# split (no origination column → derive from reporting − seasoning, DL-007)
python scripts/prepare_data.py -c configs/fannie_mae/prepare.yaml \
    --input data/raw/all_cutoffs.parquet --origination_col null --out_dir data/processed

# field schema (fully generated for this panel) + baseline
python scripts/classify_schema.py -c configs/fannie_mae/classify.yaml \
    --input data/processed/train.parquet --out configs/dutch_mortgages/tokenizer.yaml
python scripts/train_baseline.py --config configs/dutch_mortgages/baseline.yaml \
    --book data/raw/loan_book.parquet --report reports/baseline_report.md   # Gate G1 + ceiling
```

Configs live in `configs/dutch_mortgages/` (`baseline.yaml` = roles + leakage lists;
`tokenizer.yaml` = generated field schema). Gate G1 (honest, gated, no-leakage) =
ROC 0.73 / PR-AUC 0.046; a leaky config scores 0.93 — never quote that.
