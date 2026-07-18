# Baseline Report — XGBoost (Gate G1)

Config `configs/dutch_mortgages/baseline.yaml` · split `data/processed` (loan-stratified temporal, DL-007).

## Setup

- **Task:** observe each loan at `2024-12-31`; predict whether `default_crr_flag`==Y within the next **6 cutoffs** (`2025-01-31` ... `2025-06-30`).
- **Population:** loans present at the observation date; loan-stratified temporal split (train trains, test scores).
- **Gate** keeps only currently-performing loans (`arrears_bucket` in {Performing, 1-29 DPD, 30-59 DPD, 60-89 DPD}) -> predict *new* events.
- **Feature sets:** *full* = 58; *clean* = 50 (drops the 8 leakage columns below).

**Population** (loans observed at the date):

| Split | observed | gated (Gate G1) | default % | gated default % |
|---|--:|--:|--:|--:|
| train | 378,530 | 366,974 | 3.95% | 1.67% |
| val | 47,371 | 45,887 | 3.98% | 1.66% |
| test | 47,288 | 45,858 | 4.00% | 1.71% |
| **total** | **473,189** | **458,719** | | |

## Results (test split)

| Config | ROC-AUC | PR-AUC | pos% |
|---|--:|--:|--:|
| (1) full features, no gate | 0.9288 | 0.8045 | 4.00% |
| (2) full features + gate | 0.8382 | 0.4345 | 1.71% |
| (3) no-leakage features, no gate | 0.7356 | 0.0994 | 4.00% |
| (4) no-leakage + gate (Gate G1) | 0.7391 | 0.0463 | 1.71% |

## Reading it
- **Gate G1 = config (4)**: ROC-AUC 0.739, PR-AUC 0.046 -- the honest bar the foundation model must beat.
- Removing the leakage columns (current-distress state, almost the answer) is the (1)->(3) drop.
- The gate (predict *new* defaults among performing loans) is the realistic task.

**8 leakage columns** (dropped in clean): `arrears_bucket`, `performing_status`, `default_crr_flag`, `foreclosure_flag`, `days_past_due`, `arrears_amount`, `forbearance_flag`, `restructuring_flag`

**11 excluded** (ids / deal metadata / constants -- never features): `transaction_name`, `esma_transaction_identifier`, `closing_date`, `maturity_date_proxy`, `originator_name`, `servicer_name`, `currency`, `country`, `property_valuation_type`, `interest_payment_frequency`, `principal_payment_frequency`

## Caveat
- Synthetic data is rule-based, so the clean baseline runs higher than a real portfolio.

## Architectural validation — the hidden `_segment` ceiling

The generator assigns each loan a hidden fragility latent `_segment` (in `loan_book`, not the panel -- evaluation-only, never a feature). It drives default but is largely inaccessible to tabular models. On the Gate-G1 cohort:

**(A)** the hidden segment is a large source of default risk:

| Segment | loans (test) | default rate |
|---|--:|--:|
| 0 (stable) | 28,768 | 0.28% |
| 1 (baseline) | 11,411 | 1.37% |
| 2 (fragile) | 5,679 | 9.60% |
| **spread** | | **34x** |

**(B)** if a model could *see* the segment, accuracy jumps (oracle -- diagnostic only):

| | ROC-AUC | PR-AUC |
|---|--:|--:|
| Gate G1 (observables only) | 0.739 | 0.046 |
| + oracle `_segment` | 0.844 | 0.084 |
| **headroom** | **+0.105** | **+82%** |

**(C)** but a tabular model *can't* recover the segment -- overall 65% accuracy vs 63% from always guessing the majority. Per-segment recall shows it leans on the majority class and misses the rest:

| Segment | loans (test) | recall |
|---|--:|--:|
| 0 (stable) | 28,768 | 95.2% |
| 1 (baseline) | 11,411 | 2.2% |
| 2 (fragile) | 5,679 | 39.7% |

**Conclusion.** The segment is a real, large source of default risk (A) that tabular models can only weakly recover (C); a model that could fully see it would nearly double PR-AUC (B). The foundation model reads each loan's behavioural *sequence* to recover that latent -- that headroom above the baseline is the project's thesis, now quantified.

Reproduce: `python scripts/train_baseline.py --config configs/dutch_mortgages/baseline.yaml --data-dir data/processed --book data/raw/loan_book.parquet --report reports/baseline_report.md`
