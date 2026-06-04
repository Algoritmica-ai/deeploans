# Synthetic Dutch RMBS Generator — Run Plan

End-to-end plan for generating **500,000 Dutch residential-mortgage loan IDs
across 24 monthly cutoffs (Jan 2024 → Dec 2025)** in the Hypoport / ESMA
Annex 2 schema, using Data Designer for primitive sampling and a
vectorised pandas+numpy ageing pass for longitudinal dynamics.

## 1. What this directory contains

| File | Role |
|---|---|
| `data_designer_loan_book.py` | Data Designer config (samplers + tiny flag expressions) and the pandas post-processor that produces the 71-column Hypoport-parity loan book at month 0. |
| `age_to_panel.py` | Vectorised numpy ageing pass: prepayment hazard, Markov delinquency, amortisation, HPI uplift, bucket recomputation. Emits 24 per-cutoff CSVs. |
| `run.py` | End-to-end orchestrator. |
| `RUN_PLAN.md` | This file. |
| `out_smoke/` | 5k-loan smoke run produced during development (24 cutoffs, schema-validated). |

## 2. Architecture in one paragraph

Data Designer is per-row — it samples each record independently and is
strong at enforcing correlations **between columns of the same row** (via
SUBCATEGORY conditioning, expression columns, validators). It is **not**
designed to enforce correlations *across rows of the same loan over time*
(a longitudinal panel). So we use it for what it's best at — realistic
correlated origination snapshots with primitive samplers — and do the
deterministic ageing (amortisation, Markov delinquency, prepayment, HPI) in
numpy, which is two orders of magnitude faster per row.

```
       ┌─────────────────────────┐      ┌────────────────────────┐
       │ Data Designer           │ ───▶ │ Pandas post-processor  │
       │ • UUID / Category /     │      │ • Annuity formula      │
       │   Gaussian / Lognorm /  │      │ • Maturity date        │
       │   Subcategory samplers  │      │ • OLTV → market value  │
       │ • Tiny Jinja flags only │      │ • Bucket columns       │
       └─────────────────────────┘      └──────────┬─────────────┘
                                                    │
                                                    ▼
                               ┌────────────────────────────────────┐
                               │  loan_book.parquet  (month 0)      │
                               └─────────────────┬──────────────────┘
                                                  │
                                                  ▼
                               ┌────────────────────────────────────┐
                               │ Vectorised numpy ageing pass       │
                               │ • Bernoulli prepayment hazard      │
                               │ • Markov delinquency chain         │
                               │ • Closed-form amortisation         │
                               │ • HPI drift → recompute LTVs       │
                               │ • Dynamic bucket recompute         │
                               └─────────────────┬──────────────────┘
                                                  │
              ┌───────────────────────────────────┼───────────────────────────────────┐
              ▼                                                                         ▼
   green_lion_202401_1_synthetic_loan_tape.csv      ...    green_lion_202512_1_synthetic_loan_tape.csv
                                                                                          │
                                                                                          ▼
                                                                          all_cutoffs.parquet
```

## 3. Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install data-designer numpy pandas pyarrow
```

Python 3.10–3.13. Tested with `data-designer==0.6.0`.

**No API key is required** for this pipeline. We use only samplers and
expression columns (no LLM calls); the Data Designer init prints a warning
about missing API keys which can be ignored. If you want to add LLM-generated
columns later (e.g., synthetic underwriting notes), set one of:

```bash
export NVIDIA_API_KEY=...       # free tier at build.nvidia.com
# or
export OPENAI_API_KEY=...
# or
export OPENROUTER_API_KEY=...
```

## 4. Quick start (smoke test, ~10 s)

```bash
python run.py --num-records 5000 --out-dir ./out_smoke
```

Expected output:

```
=== Phase 1: Data Designer loan book (5,000 records) ===
[loan-book] DataDesigner generating 5,000 records...
[loan-book] Deriving static fields (5,000 rows)...
[loan-book] Wrote 5,000 rows × 71 cols → out_smoke/loan_book.parquet
    done in 6.0s

=== Phase 2: ageing 24 cutoffs ===
[ageing] cutoff=2024-01-31 rows=  5,000  defaulted= 0.20%  -> green_lion_202401_1_synthetic_loan_tape.csv
...
[ageing] cutoff=2025-12-31 rows=  4,349  defaulted= 3.91%  -> green_lion_202512_1_synthetic_loan_tape.csv
    done in 3.9s

=== Phase 3: consolidating to parquet ===
    wrote 111,866 loan-month rows -> out_smoke/all_cutoffs.parquet
```

Inspect any cutoff:

```bash
head -2 out_smoke/cutoffs/green_lion_202401_1_synthetic_loan_tape.csv
```

## 5. Full hackathon run (500k loan IDs, ~5–10 min)

```bash
python run.py \
    --num-records 500000 \
    --n-cutoffs 24 \
    --first-cutoff 2024-01-31 \
    --out-dir ./out_full \
    --seed 42
```

Expected resources on a modern laptop (M-series Mac or 8-core x86):

| Phase | Time | Peak RAM | Output |
|---|---|---|---|
| Data Designer sampling (500k rows × 26 samplers) | 3–5 min | ~3 GB | `loan_book.parquet` (~120 MB) |
| Ageing (500k × 24 months) | 2–4 min | ~3.5 GB | 24× CSV (~250–350 MB each) |
| Consolidation | ~1 min | ~6 GB peak | `all_cutoffs.parquet` (~1.2 GB) |
| **Total** | **~10 min** | **~6 GB** | **~7 GB on disk** |

If RAM is tight, drop `--skip-consolidated` (omits the all-in-one parquet,
saves ~2 GB).

## 6. Output layout

```
out_full/
├── loan_book.parquet              # month 0 loan book, the seed for ageing
├── all_cutoffs.parquet            # all 24 cutoffs concatenated (long format)
└── cutoffs/
    ├── green_lion_202401_1_synthetic_loan_tape.csv
    ├── green_lion_202402_1_synthetic_loan_tape.csv
    ├── ...
    └── green_lion_202512_1_synthetic_loan_tape.csv
```

Each CSV has the **exact 71-column Hypoport header**, byte-for-byte
compatible with the reference Green Lion 2026-1 files.

## 7. Calibration knobs (in `age_to_panel.py`)

| Constant | Default | Where to tune |
|---|---|---|
| `ANNUAL_PREPAYMENT_HAZARD` | 0.07 | Anchor against Moody's UK RMBS / DBRS NL series |
| `TRANS_MATRIX` | 6×6 row-stochastic Markov | Reduce the Performing→1-29 cell to lower the cumulative default rate |
| `hpi_monthly_drift` | 1.03^(1/12) | Replace with an actual NL HPI series for stress scenarios |

And in `data_designer_loan_book.py`:

| Sampler | Default | Calibration source |
|---|---|---|
| `PROVINCE_WEIGHTS` | Hypoport empirical | CBS NL housing-stock per province |
| `NUTS3_BY_PROVINCE` | Illustrative subset | Eurostat NUTS-3 catalogue |
| `original_balance` (lognorm s=0.40, scale=300k) | NL prime median ~€300k | DNB NL mortgage portfolio data |
| `oltomv_original` (truncnorm μ=85) | NL prime prevalence | AFM / DNB OLTV reporting |
| `current_interest_rate_pct` (μ=3.10%) | 2024 NL mortgage rates | ECB / DNB monthly rate series |
| `_arrears_state` weights | 96.5% performing at t0 | DBRS NL RMBS performance reports |
| `nhg_flag` (45% Bernoulli; capped €435k) | 2024 NHG limit | NHG / Hypoport public stats |
| `epc_label` weights | NL housing-stock EPC mix | Rijksdienst voor Ondernemend Nederland |

## 8. Validation checks to run before showcase

```bash
python - <<'PY'
import pandas as pd, glob
for f in sorted(glob.glob('out_full/cutoffs/*.csv'))[:3] + sorted(glob.glob('out_full/cutoffs/*.csv'))[-3:]:
    df = pd.read_csv(f, nrows=1)
    assert len(df.columns) == 71, f
    print(f.split('/')[-1], '->', len(df.columns), 'cols ✓')

import pandas as pd
ref = open('green_lion_202602_1_synthetic_loan_tape.csv').readline().strip().split(',')
got = pd.read_csv(sorted(glob.glob('out_full/cutoffs/*.csv'))[0], nrows=0).columns.tolist()
assert ref == got, set(ref) ^ set(got)
print('Schema parity ✓')
PY
```

Plus a longitudinal coherence check — pick 10 random `loan_id` and verify
that across the 24 cutoffs: balance only decreases (or is constant for IO),
seasoning increments by 1, remaining term decrements by 1, and once
defaulted the loan stays defaulted.

## 9. Deliverable assembly for SOW items 2–5

| SOW item | How |
|---|---|
| (2) HuggingFace upload | `huggingface-cli login` then `huggingface-cli upload Algoritmica/rmbs-nl-synthetic ./out_full/cutoffs --repo-type dataset`. Include a `README.md` dataset card describing the schema, calibration anchors, and the 24-cutoff structure. |
| (3) Source-code PR to deeploans | New folder `synthetic-data-designer/` containing this directory's contents + a top-level `README.md`. |
| (4) Showcase (Fri 5 Jun) | Demo: run `python run.py --num-records 5000` live (10s), open one cutoff CSV side-by-side with Hypoport, then show a default-rate-vs-time plot across the 24 cutoffs. |
| (5) Nexus delivery | Out of scope per SOW. |

## 10. Known limitations & follow-ups

1. **Data Designer 0.6.0 quirk** — `convert_to='int'` on a CATEGORY
   sampler with string values raises `Expected numeric dtype, got object`.
   Workaround in code: pass int values directly to `CategorySamplerParams`.
2. **Markov chain calibration is hot** — at default settings the 24-month
   cumulative default rate is ~3.9% (vs ~1% empirical for Dutch prime).
   Reduce the `Performing → 1-29 DPD` cell of `TRANS_MATRIX` from 0.0060 to
   ~0.0025 to halve the default flow.
3. **HPI overlay is a constant drift** — swap `hpi_monthly_drift()` for a
   real NL CBS HPI series before the showcase if scenario realism matters.
4. **No new originations** — closed pool per the confirmed SOW. Pool size
   erodes from 500k at month 0 to ~430k at month 24 via prepayments and
   charge-offs.
5. **`property_valuation_type` is constant** — set to
   "Indexed/Origination Proxy" everywhere. In real data this can flip after
   a re-valuation. Add a small Bernoulli per cutoff to introduce variety.
6. **No multi-loan-part modelling** — `loan_part_count` is sampled but each
   `loan_id` is still one row (the Hypoport files appear to do the same).

## 11. End-to-end command cheat sheet

```bash
# 1. Install
pip install data-designer numpy pandas pyarrow

# 2. Smoke (10 s)
python run.py --num-records 5000 --out-dir ./out_smoke

# 3. Full hackathon dataset (10 min)
python run.py --num-records 500000 --out-dir ./out_full --seed 42

# 4. Validate schema parity
python - <<'PY'
import pandas as pd
ref = pd.read_csv('green_lion_202602_1_synthetic_loan_tape.csv', nrows=0).columns.tolist()
got = pd.read_csv('out_full/cutoffs/green_lion_202401_1_synthetic_loan_tape.csv', nrows=0).columns.tolist()
print('schema matches:', ref == got)
PY

# 5. Upload to HuggingFace
huggingface-cli login
huggingface-cli upload Algoritmica/rmbs-nl-synthetic ./out_full/cutoffs --repo-type dataset
```
