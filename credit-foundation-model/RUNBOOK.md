# Runbook — Running the Pipeline

End-to-end steps for the full pipeline as built today. Every script follows one grammar:
`-c <recipe.yaml>` plus dotted overrides (`--key.path value`); every path may be local,
`gs://`, or `s3://` (`credit_fm.utils.storage`). The reference corpus recipes live in
`configs/mortgage_performance/`; the Dutch synthetic panel is the validation set. See
`docs/architecture.md` for how the pieces fit.

## 0. Environment (GPU container)
```bash
bash scripts/setup_container.sh     # restart-proof venv + install + git (docs/container_setup.md)
```
Secrets and the GCS key live in `/workspace/secrets.env` and
`/workspace/.gcloud/credit-fm-sa.json` — never committed.

## 1. Ingest → per-loan monthly panel
```bash
python scripts/ingest_mortgage_performance.py -c configs/mortgage_performance/ingest.yaml
# full 25-year corpus at the 4% loan-hash sample:
python scripts/ingest_mortgage_performance.py -c configs/mortgage_performance/ingest_2000_2024.yaml
# audit the produced panel (re-derives labels from retained raw columns):
python scripts/validate_ingest.py --panel <out>/panel_2000_2024.parquet
```
Derives `reporting_date` (ISO), `dlq_num`, `default_event` (D180 / credit-event zero-balance),
`prepay_event`, `is_performing`. See `notebooks/00_data_bible.ipynb` for the full schema.

## 2. Split (loan-stratified, temporal — DL-007)
```bash
python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
    --run_name run_2000_2024 --reporting_max 2022-12-31   # cap = pretrain stays blind to the OOT era
# → {train,val,test}.parquet + splits.csv + splits.meta.json
python scripts/validate_splits.py --dir <out_dir>          # disjoint/temporal/manifest audit
```
See `notebooks/01_data_splits.ipynb` for what the split guarantees (and what it's *not* — the
credit test is the OOT harness in step 7, not `test.parquet`).

## 3. Field schema (classify)
```bash
python scripts/classify_schema.py -c configs/mortgage_performance/classify.yaml    # report + suggestions
```
For the mortgage reference the shipped `configs/mortgage_performance/tokenizer.yaml` is curated on top of
the classifier's output (leakage exclusion, ARM/IO drops, regulatory anchors) — see
`notebooks/02_schema_classification.ipynb`.

## 4. Fit the tokenizer (train split only — DL-008)
```bash
python scripts/train_tokenizer.py -c configs/mortgage_performance/tokenizer_fit.yaml
# → configs/mortgage_performance/tokenizer.json (frozen vocab; 552 tokens on the full corpus) + QA report
```

## 5. Encode shards (encode-once — DL-014)
```bash
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --workers 32
# → shard-*.parquet + manifest.json per split (the token-id contract the model reads)
```

## 6. Pretrain (MLM)
```bash
python scripts/pretrain.py -c configs/mortgage_performance/pretrain.yaml
```
Train and val loss falling *together* = generalising; a widening gap = memorising (DL-015 — the
~26M model needs the full corpus, not a 100k-loan slice). MLM loss is a proxy; the verdict is
step 7.

## 7. The out-of-time verdict
```bash
# the bar: XGBoost on 57 no-leakage features, identical calendar window
python scripts/build_oot_baseline.py --train-years 2016-2021 --test-years 2022-2023 \
    --sample-pct 20 --report reports/mortgage_oot_2022_2023.md

# the FM: embeddings at each observation cutoff, then the adaptation ladder
python scripts/extract_embeddings.py -c configs/mortgage_performance/extract.yaml
python scripts/evaluate_downstream.py -c configs/mortgage_performance/evaluate.yaml
python scripts/finetune.py -c configs/mortgage_performance/finetune_oot.yaml --mode full   # frozen|lora|full
```
Reference result: FM full 0.8257 ROC / 0.0113 AP vs baseline 0.7913 / 0.0057 on 2022–23
observations (2023–24 defaults). Guards: performing-at-observation gate, loan-disjoint,
embargo, train-only downsampling.

## 8. Package a checkpoint
```bash
python scripts/publish_model.py -c configs/mortgage_performance/publish.yaml
# → models/<name>/ (safetensors + config + tokenizer + card + load example)
```

## Verify
```bash
ruff check . && pytest -q
```
Each pipeline stage also has an artifact validator (`scripts/validate_*.py`) — run it against
the stage's real output after any rerun.

## New asset class
See `docs/extending.md` — new `configs/<asset>/` recipes through the same scripts; the Dutch
panel is the worked example.
