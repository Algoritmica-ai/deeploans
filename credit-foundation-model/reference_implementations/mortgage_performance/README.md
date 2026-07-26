# Reference implementation — single-family mortgages

The framework's reference corpus: ~25 years of real-world US fixed-rate mortgages (public
single-family mortgage performance data). See
[`docs/data/mortgage_performance.md`](../../docs/data/mortgage_performance.md) for the schema, label
(D180 / Zero-Balance credit event), and leakage discipline, and
[`notebooks/00_data_bible.ipynb`](../../notebooks/00_data_bible.ipynb) for the full
column-by-column reference.

## Pipeline (config-driven — `-c recipe.yaml` + dotted overrides)

| Step | Command | Output |
|------|---------|--------|
| Ingest | `python scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml` | one `part-<quarter>.parquet` per source (sharded + resumable — rerun skips finished quarters) |
| Validate | `python scripts/validate_ingest.py --panel <out>/panel_2000_2024/part-2016Q1.parquet` | PASS/FAIL audit (any shard) |
| Split | `python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml --run_name run_2000_2024 --reporting_max 2022-12-31` | `{train,val,test}.parquet` + `splits.{csv,meta.json}` |
| Validate | `python scripts/validate_splits.py --dir <out_dir>` | PASS/FAIL audit |
| Tokenizer fit | `python scripts/train_tokenizer.py -c configs/mortgage_performance/tokenizer_fit.yaml` | `tokenizer.json` (552-token frozen vocab) + QA report |
| Encode | `python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --workers 32` | token-id shards + manifest |
| Pretrain | `python scripts/pretrain.py -c configs/mortgage_performance/pretrain.yaml` | MLM checkpoint |
| OOT verdict | `scripts/build_oot_baseline.py` + `extract_embeddings` + `finetune -c configs/mortgage_performance/finetune_oot.yaml` | reports |

Every path is pluggable (`credit_fm.utils.storage`): local, `gs://`, or `s3://`.

## Config

- [`configs/mortgage_performance/baseline.yaml`](../../configs/mortgage_performance/baseline.yaml) — id/time/label/gate
  roles + the `exclude`/`leakage` lists (the no-peeking contract).
- `configs/mortgage_performance/tokenizer.yaml` — field schema (profile/event routing, bins, anchors);
  curated on top of `classify_schema.py`'s suggestions (leakage exclusion, ARM/IO drops).
- `configs/mortgage_performance/common.yaml` + one recipe per stage.

## Out-of-time evaluation — the bar and the result

`scripts/build_oot_baseline.py` builds the calendar-split bar: observe each loan every December
it is performing, label = default within 12 months, train on past years / test on future years,
with **loan-disjoint** and **embargo** guards.

| Experiment | Window | Result |
|---|---|---|
| XGBoost bar | train 2016–2021 → test 2022–2023 | ROC 0.7913 / AP 0.0057 |
| FM 26M full fine-tune (4% corpus) | identical window | ROC 0.8257 / AP 0.0113 |
| **FM 100M full fine-tune (10% corpus)** | identical window | **ROC 0.8468 / AP 0.0175** |
| Crisis stress (baseline) | train 2000–2006 → test 2008–2010 | ROC 0.757 / AP 0.024 |

Long runs: detach with `nohup … > logs/run.log 2>&1 &` and `tail -f` the log.

## Scoring, calibration, and serving (v1.1 G6)

Batch-score a portfolio, calibrate the scores into PDs on a held-out window (never a test
cutoff — the stage refuses), then score calibrated:

```bash
python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
    --cutoff 2021-12-31 --out $RUNS/calibration_scores.parquet   # calibration window
python scripts/calibrate.py -c configs/mortgage_performance/calibrate.yaml # fits calibrator.json
python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
    --calibrator $RUNS/calibrator.json                           # scores + calibrated `pd` col
python scripts/validate_scores.py --scores <scores> --labeled-panel <panel>  # incl. Brier (check I)
```

**Serving example** (`serve.py` — explicitly an example: no auth/TLS/scaling; it reuses
`credit_fm.inference.scoring`, so an HTTP score equals a batch score):

```bash
pip install "credit_fm[serving]"
python reference_implementations/mortgage_performance/serve.py \
    --checkpoint runs/m_100m_ft.pt --calibrator runs/calibrator.json --port 8000

curl -s localhost:8000/score -H 'Content-Type: application/json' -d '{
  "cutoff": "2023-12-31",
  "loans": [
    {"loan_id": "L1", "reporting_date": "2023-11-30", "loan_age": 23, "original_ltv": 80,
     "channel": "R", "current_upb": 190000, "current_interest_rate": 6.5, "is_performing": true},
    {"loan_id": "L1", "reporting_date": "2023-12-31", "loan_age": 24, "original_ltv": 80,
     "channel": "R", "current_upb": 189000, "current_interest_rate": 6.5, "is_performing": true}
  ]
}'
# -> {"cutoff":"2023-12-31","n_scored":1,"calibrated":true,
#     "scores":[{"loan_id":"L1","score":0.41,"pd":0.0031,"rank":1,...}]}
```
