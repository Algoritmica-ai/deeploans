# Fannie Mae Single-Family Loan Performance — data notes

**Primary pretraining corpus.** Real-world US single-family **fixed-rate** mortgages, ~25 years,
in the GCS bucket `gs://sriram-credit-fm-data`, **Hive-partitioned by reporting period**:

    fannie_by_reporting/reporting_year=<YYYY>/reporting_quarter=<Q#>/from_<acqQ>_*.parquet

i.e. partitioned by the *observation* quarter; within each partition, files are sharded by
acquisition cohort (`from_2000Q1` = loans originated ~2000Q1). One file holds the monthly rows
(3 per loan) for one (reporting-quarter, cohort) slice. Schema = the published Fannie *Single-
Family Loan Performance* layout, **113 snake_case columns** (verified against the data:
`loan_identifier`, `monthly_reporting_period`, `origination_date`,
`current_loan_delinquency_status`, `zero_balance_code`, …; dates are `MMYYYY` strings).

> **Span matters.** Because the data is partitioned by reporting period, one quarter = ~3 monthly
> rows per loan. A meaningful forward-horizon baseline (observe at `obs_date`, predict default
> within `horizon_months`) needs the reporting span **[obs_date, obs_date + horizon]** ingested —
> e.g. obs 2016-12-31 + 12-month horizon → ingest `2016Q1..2017Q4`. A single quarter only
> exercises the pipeline plumbing; the gated task has too few new defaults to score.

## Why it's the primary set (vs the Dutch synthetic panel)
- **Real data** — makes the "FM beats tabular" thesis credible, not a synthetic artefact.
- **True `Origination Date`** (field 14) — temporal split orders by it directly
  (`prepare_data.py --origination-col origination_date`); no seasoning-derivation (DL-007).
- **Already a monthly performance panel** (field 3, Monthly Reporting Period) — maps onto the
  event/history branches with no reshaping.
- The Dutch synthetic panel stays as a **controlled validation/ablation set** — it carries the
  hidden `_segment` latent that gives us the segment-ceiling proof (not possible on real data).

## Structure
- **Grain:** one row per `loan_id` per `reporting_date` (monthly).
- **Static / profile fields** (constant per loan): channel, seller, original interest rate,
  original UPB, original loan term, origination date, original LTV / CLTV, number of borrowers,
  DTI, FICO at origination, first-time-homebuyer, loan purpose, property type, number of units,
  occupancy, property state, MI %, amortization type, etc.
- **Dynamic / event fields** (per period): current interest rate, current actual UPB, loan age,
  remaining months to maturity, current FICO.

## Label (derived in `scripts/ingest_fannie_mae.py`)
- **`default_event`** = `Current Loan Delinquency Status` reaches **D180** (≥ 6 months delinquent)
  **OR** `Zero Balance Code` ∈ {02 third-party sale, 03 short sale, 09 REO/deed-in-lieu,
  15 note sale} — the standard Fannie credit-event definition.
- **`prepay_event`** = `Zero Balance Code` == 01 (prepaid / matured).
- **`is_performing`** = current (dlq 0) and not yet terminated — the performing-at-observation gate.

## Leakage discipline (same rules as Dutch — see `configs/fannie_mae/baseline.yaml`)
Contemporaneous-state / outcome-revealing fields are dropped for default prediction and the task
is gated to performing-at-observation: current delinquency status, zero-balance code + dates,
foreclosure/disposition dates, last-paid-installment, and all loss / proceeds / expense / deferral
/ modification-loss columns. Split by `loan_id` (never row); temporal by origination; vocab/bins
fit on `train` only.

## Ingest → pipeline
Ingest reads either explicit files (local dev) or a span of reporting quarters from the GCS
Hive layout (auto-loads the service-account key at `/workspace/.gcloud/credit-fm-sa.json`).

```bash
# A. dev plumbing: explicit local sample files (proves the chain runs; not a real metric)
python scripts/ingest_fannie_mae.py --files data/raw/<file1>.parquet data/raw/<file2>.parquet

# B. real run: a multi-year reporting span from GCS (covers obs_date + horizon)
python scripts/ingest_fannie_mae.py \
    --gcs-root gs://sriram-credit-fm-data/fannie_by_reporting \
    --reporting 2016Q1 2016Q2 2016Q3 2016Q4 2017Q1 2017Q2 2017Q3 2017Q4

# then: loan-stratified temporal split (real orig date), persisted BACK to the bucket
python scripts/prepare_data.py --input data/raw/fannie_mae/panel.parquet \
    --origination-col origination_date \
    --out-dir gs://sriram-credit-fm-data/processed/fannie_mae/run_2016_2017
# generated tokenizer config
python scripts/classify_schema.py --input data/raw/fannie_mae/panel.parquet \
    --out configs/fannie_mae/tokenizer.yaml
# honest Gate-G1 baseline, reading the splits straight from the bucket
python scripts/train_baseline.py --config configs/fannie_mae/baseline.yaml \
    --data-dir gs://sriram-credit-fm-data/processed/fannie_mae/run_2016_2017 \
    --report reports/fannie_baseline_report.md
```

`--out-dir` / `--data-dir` are **pluggable** (`credit_fm.utils.storage`): local path, `gs://`, or
`s3://` — only the URL scheme changes. GCS auth auto-loads `/workspace/.gcloud/credit-fm-sa.json`;
for a future S3 move, `pip install s3fs` and pass an `s3://…` URL (AWS creds via the env chain).

## Derived columns (in `ingest_fannie_mae.py`)
`loan_id` (← `loan_identifier`), `reporting_date` & `origination_date` (ISO `YYYY-MM-DD` strings,
month-end, parsed from the `MMYYYY` fields), `dlq_num` (int; `XX`→NaN), `default_event`,
`prepay_event`, `is_performing`. Verified: dlq codes `00..15`+`XX`; ZBC `01` prepay / `02,03,09,15`
credit events.

## Open items
- Align `obs_date` in `baseline.yaml` to the ingested reporting span (default 2016-12-31 / 12mo
  assumes a 2016Q1..2017Q4 ingest).
- For full pretraining, ingest the full reporting span (all ~years) — sizeable; partition output.