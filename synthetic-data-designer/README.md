# Synthetic Dutch RMBS Generator

A reproducible pipeline for generating large, ESMA Annex 2-aligned **Dutch
residential-mortgage** loan tapes as a **coherent monthly panel**, suitable
for pretraining credit foundation models, downstream cash-flow projection,
and back-testing.

The generator targets the Hypoport "Green Lion" RMBS schema byte-for-byte
(71 columns) and uses **Data Designer** for per-row primitive sampling plus a
vectorised numpy ageing pass for longitudinal dynamics (amortisation, Markov
delinquency, prepayments, HPI indexing).

Default scale: **500,000 loan IDs × 24 monthly cutoffs (Jan 2024 → Dec 2025)
≈ 11–12 M loan-month rows**. Zero LLM calls — every column is a sampler or
a deterministic derivation, so API cost is $0 and runtime is ~10 min on a
modern laptop.

---

## Table of Contents

- [Project layout](#project-layout)
- [Quick start](#quick-start)
- [Production run](#production-run--500000-loan-ids)
- [Architecture](#architecture)
- [Schema (71 columns)](#schema-71-columns)
- [Lifecycle semantics](#lifecycle-semantics)
- [Testing](#testing)
- [Calibration knobs](#calibration-knobs)
- [Known limitations](#known-limitations-and-follow-ups)
- [References](#references)
- [License](#license)

---

## Project layout

```
synthetic-data-designer/
├── README.md                          ← this file
├── RUN_PLAN.md                        ← detailed run plan & scaling guide
├── COLUMN_GLOSSARY.md                 ← per-column definitions (all 71)
├── data_designer_loan_book.py         ← Data Designer config + pandas
│                                        post-processor for the month-0 book
├── age_to_panel.py                    ← vectorised ageing pass (Markov
│                                        delinquency, prepayment, amortisation)
├── run.py                             ← end-to-end orchestrator (Phase 1 + 2 + 3)
└── tests/
    ├── README.md
    ├── test_panel.sql                 ← 53 SQL tests (DuckDB-flavoured)
    └── run_sql_tests.py               ← Python runner with PASS/FAIL summary
```

Run `python run.py --num-records 500000 --out-dir ./out_500k` to produce the
production dataset.

---

## Quick start

### 1. Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install data-designer numpy pandas pyarrow duckdb
```

Python 3.10–3.13. Tested with `data-designer==0.6.0`. No API key is needed —
the pipeline uses sampler + expression columns only.

### 2. Smoke test (~10 seconds)

```bash
python run.py --num-records 5000 --out-dir ./out_smoke
```

Inspect the first cutoff:

```bash
head -2 ./out_smoke/cutoffs/green_lion_202401_1_synthetic_loan_tape.csv
```

### 3. Run the SQL test suite

```bash
python tests/run_sql_tests.py --cutoff-dir ./out_smoke/cutoffs
```

53/53 tests should pass.

---

## Production run — 500,000 loan IDs

```bash
python run.py \
    --num-records 500000 \
    --n-cutoffs 24 \
    --first-cutoff 2024-01-31 \
    --deal-year 2024 \
    --out-dir ./out_500k \
    --seed 42
```

| Resource | Estimate (modern laptop) | Estimate (sandbox VM) |
|---|---|---|
| Phase 1: Data Designer + pandas derivations | 3–5 min | ~10 min |
| Phase 2: 24-cutoff ageing pass | 2–4 min | ~7 min |
| Phase 3: consolidation to `all_cutoffs.parquet` | ~1 min | ~2 min |
| **Total wall time** | **~7–10 min** | **~19 min** |
| Peak RAM | ~6 GB | ~6 GB |
| On-disk footprint | ~7–8 GB | ~7–8 GB |

Output layout:

```
out_500k/
├── loan_book.parquet                 ← month-0 origination book (~120 MB)
├── all_cutoffs.parquet               ← all 24 cutoffs concatenated (~1.2 GB)
└── cutoffs/
    ├── green_lion_202401_1_synthetic_loan_tape.csv
    ├── green_lion_202402_1_synthetic_loan_tape.csv
    ├── …
    └── green_lion_202512_1_synthetic_loan_tape.csv
```

Each CSV has the **exact 71-column Hypoport header**, byte-for-byte compatible
with the reference Green Lion files.

---

## Architecture

```
            ┌─────────────────────────────┐
            │    Data Designer config     │
            │ UUID / Category / Subcat /  │
            │ Gaussian / Lognorm /        │
            │ Bernoulli + tiny Jinja flag │
            │  expressions                │
            └────────────┬────────────────┘
                         │
                         ▼
            ┌─────────────────────────────┐
            │ Pandas post-processor       │
            │ • annuity formula           │
            │ • maturity date             │
            │ • LTVs, market values       │
            │ • 7 bucket columns          │
            └────────────┬────────────────┘
                         │
                         ▼
          ┌─────────────────────────────────────┐
          │   loan_book.parquet   (month 0)     │
          └─────────────────┬───────────────────┘
                            │
                            ▼
          ┌─────────────────────────────────────┐
          │ Vectorised numpy ageing pass        │
          │ • Bernoulli prepayment hazard       │
          │ • 6-state Markov delinquency chain  │
          │ • One-step iterative amortisation   │
          │   (Performing only)                 │
          │ • HPI drift → revalue properties    │
          │ • Charge-off after N months default │
          │ • Redeemed / Charged-Off terminals  │
          └───────────────┬─────────────────────┘
                          │
   ┌──────────────────────┼────────────────────────────────┐
   ▼                                                        ▼
green_lion_202401_1_synthetic_loan_tape.csv  ...  green_lion_202512_1_synthetic_loan_tape.csv
                                                            │
                                                            ▼
                                             all_cutoffs.parquet
```

### Why split the work this way?

Data Designer is per-row. It excels at realistic, correlated origination
snapshots (province → NUTS-3 region via `SUBCATEGORY`, lognormal balances,
employment-status-conditional NHG, EPC label distributions, …). It does not
model cross-row dynamics over time — that's the longitudinal panel. So Data
Designer creates the month-0 loan book, and a separate vectorised numpy
module ages that book month-by-month. The split keeps each component doing
what it's best at and runs ~100× faster at scale than per-row Jinja for the
derived numeric fields.

### Why zero LLM calls?

Data Designer has two families of column generators: **local** (samplers,
expression columns, custom Python callables) and **LLM-backed**
(`LLMTextColumnConfig`, `LLMStructuredColumnConfig`, `LLMJudgeColumnConfig`,
embeddings, images). The Hypoport schema has no free-text fields, so the
config uses only local generators. The Data Designer runtime confirms
`model usage summary: no model usage recorded`. Adding LLM richness later
(e.g. a synthetic underwriting note) is a single column-config addition.

---

## Schema (71 columns)

Identical to Hypoport's Green Lion 2026-1 reference CSVs, aligned to ESMA
Annex 2 *Underlying exposures — residential real estate*. Grouped roughly:

- **Identifiers & transaction (9):** `loan_id`, `transaction_name`,
  `esma_transaction_identifier`, `reporting_date`, `closing_date`,
  `originator_name`, `servicer_name`, `currency`, `country`.
- **Loan economics & terms (14):** `origination_year`,
  `maturity_date_proxy`, `original_balance`, `current_balance`,
  `repayment_type`, `interest_only_flag`, `current_interest_rate_pct`,
  `rate_type`, `remaining_interest_fixed_period_months`,
  `fixed_interest_period_end_in_months`, `seasoning_months`,
  `remaining_term_months`, `legal_maturity_months`, `loan_part_count`.
- **Borrower & property (14):** `debtor_count`, `property_type`, `province`,
  `economic_region_nuts3`, `construction_year`, `occupancy`,
  `property_usage`, `employment_status`, `self_employed_flag`,
  `borrower_type`, `loan_purpose`, `buy_to_let_flag`, `nhg_flag`,
  `guarantee_type`.
- **LTV / valuation (7):** `oltomv_original`, `cltomv_current`,
  `cltimv_current`, `original_market_value_at_origination`,
  `current_original_market_value`, `indexed_market_value`,
  `property_valuation_type`.
- **Affordability (4):** `loan_to_income`, `payment_due_to_income_pct`,
  `borrower_annual_income`, `scheduled_monthly_payment`.
- **Performance (8):** `arrears_bucket`, `arrears_amount`, `days_past_due`,
  `default_crr_flag`, `performing_status`, `foreclosure_flag`,
  `forbearance_flag`, `restructuring_flag`.
- **Energy / ESG (3):** `epc_label`, `epc_issue_year`,
  `primary_energy_demand_kwh_m2`.
- **Construction deposit (3):** `construction_deposit_flag`,
  `construction_deposit_pct`, `construction_deposit_amount`.
- **Payment frequencies (2):** `interest_payment_frequency`,
  `principal_payment_frequency`.
- **Pre-computed buckets for analytics (7):** `balance_bucket`,
  `cltomv_current_bucket`, `cltimv_current_bucket`,
  `oltomv_original_bucket`, `loan_to_income_bucket`,
  `payment_due_to_income_pct_bucket`, `construction_year_bucket`.

### ESMA conventions enforced

- `closing_date`: ISO 8601 (`YYYY-MM-DD`), first business day of January of
  the deal year — single value per transaction, derived from `--deal-year`.
- `esma_transaction_identifier`: 18-character SPV LEI prefix + `YYYYMM` of
  deal close.
- `transaction_name`: `"Green Lion <YYYY>-1 B.V."`.
- `reporting_date`: pool cut-off date for that cutoff (per-cutoff value).

---

## Lifecycle semantics

Each loan_id moves through an 8-state lifecycle:

```
            ┌────────────────────────────┐
            │           Performing       │ ◀───────────────┐
            └──────────┬─────────────────┘                 │
                       │                                   │ cure
                       ▼                                   │
            ┌────────────────────────────┐                 │
            │          1-29 DPD          │ ────────────────┤
            └──────────┬─────────────────┘                 │
                       ▼                                   │
            ┌────────────────────────────┐                 │
            │         30-59 DPD          │ ────────────────┤
            └──────────┬─────────────────┘                 │
                       ▼                                   │
            ┌────────────────────────────┐                 │
            │         60-89 DPD          │ ────────────────┤
            └──────────┬─────────────────┘                 │
                       ▼                                   │
            ┌────────────────────────────┐                 │
            │          90+ DPD           │ ────────────────┘
            └──────────┬─────────────────┘
                       ▼
            ┌────────────────────────────┐    9 months in default
            │         Defaulted          │ ─────────────────┐
            │       (absorbing)          │                  │
            └────────────────────────────┘                  ▼
                                              ┌────────────────────────┐
                                              │     Charged-Off         │
                                              │ (terminal, drops next) │
                                              └────────────────────────┘
            ┌────────────────────────────┐
            │           Redeemed         │   (terminal, drops next)
            │  Bernoulli prepayment      │
            │  hazard ≈ 7% annualised    │
            └────────────────────────────┘
```

| State | `current_balance` | `arrears_amount` | `days_past_due` |
|---|---|---|---|
| Performing | One-step annuity amortisation each month (IO: flat) | 0 | 0 |
| 1-29 / 30-59 / 60-89 / 90+ DPD | Frozen at prior month's balance | += 1 × `scheduled_monthly_payment` each non-Performing month | Bucket midpoint (15 / 45 / 75 / 120) |
| Defaulted | Frozen | Continues to accrue +1 payment / month | 200 |
| Charged-Off (terminal) | 0 (writes off in this final cutoff) | 0 | 200 |
| Redeemed (terminal) | 0 (paid off in this final cutoff) | 0 | 0 |

**Static fields** (origination_year, original_balance, province, employment_status,
NHG flag, etc. — 26 in total) are bit-identical for the same `loan_id`
across every cutoff. **Dynamic fields** (current_balance, seasoning_months,
remaining_term_months, arrears_*, LTVs, market values, dynamic buckets,
reporting_date) evolve per the rules above.

---

## Testing

`tests/test_panel.sql` is a 53-test SQL suite covering:

- **Schema** (4): row count, 24 cutoffs, 71 columns, loan_id uniqueness.
- **Deal-level constants** (8): currency, country, originator, servicer,
  single closing_date / transaction_name / ESMA ID, ISO 8601 format.
- **Domain constraints** (15): enums, ranges, NHG cap, mutual consistency.
- **Static-field coherence** (7): the same loan_id has identical
  origination_year / original_balance / province / NHG / legal_maturity /
  IO flag / scheduled_payment across all cutoffs.
- **Temporal consistency** (3): reporting_date strictly increases,
  seasoning_months +1 per month, remaining_term -1 per month.
- **Lifecycle properties** (11): all the per-state balance / arrears /
  flag invariants documented above.
- **Pool integrity** (2): no new loan_id after first cutoff (closed pool);
  terminal states stay terminal.
- **Distribution sanity** (3): first cutoff ≥ 90% Non-defaulted; final
  cutoff has Defaulted loans; some attrition occurred.

Run:

```bash
python tests/run_sql_tests.py --cutoff-dir ./out_500k/cutoffs --fail-on-violation
```

Latest validation result on the 44,610-row sample dataset: **53/53 PASS**.

---

## Calibration knobs

The most useful tunables, with calibration anchors:

| Constant | Where | Default | Anchor |
|---|---|---|---|
| `ANNUAL_PREPAYMENT_HAZARD` | `age_to_panel.py` | 0.07 | Moody's UK RMBS Forecast / DBRS NL series |
| `TRANS_MATRIX` (6×6 monthly transitions) | `age_to_panel.py` | Hand-set | Reduce the `Performing → 1-29 DPD` cell to halve cumulative default rate |
| `MONTHS_IN_DEFAULT_TO_CHARGEOFF` | `age_to_panel.py` | 9 | DBRS / Moody's NL recovery-timeline assumptions |
| `hpi_monthly_drift()` | `age_to_panel.py` | 1.03^(1/12) constant | Swap for actual CBS NL HPI series for stress scenarios |
| `PROVINCE_WEIGHTS` | `data_designer_loan_book.py` | Hypoport empirical | CBS NL housing-stock distribution |
| `original_balance` lognorm (s=0.40, scale=300k) | `data_designer_loan_book.py` | NL prime median ~€300k | DNB NL mortgage portfolio data |
| `oltomv_original` truncnorm (μ=85) | `data_designer_loan_book.py` | NL prime prevalence | AFM / DNB OLTV reporting |
| `current_interest_rate_pct` (μ=3.10%) | `data_designer_loan_book.py` | 2024 NL mortgage rates | ECB / DNB monthly rate series |
| `_arrears_state` weights | `data_designer_loan_book.py` | 96.5% performing at t0 | DBRS NL RMBS performance reports |
| `nhg_flag` Bernoulli (45%, capped €435k) | `data_designer_loan_book.py` | 2024 NHG limit | NHG / Hypoport public statistics |
| `epc_label` weights | `data_designer_loan_book.py` | NL housing-stock EPC mix | Rijksdienst voor Ondernemend Nederland |

---

## Known limitations and follow-ups

1. **Markov chain is hand-set.** Calibration against a real public seed
   (Freddie Mac single-family loan data is the closest free analog) is on
   the post-hackathon roadmap.
2. **HPI overlay is a constant drift.** Swap `hpi_monthly_drift()` for the
   CBS NL HPI series for realistic stress scenarios.
3. **No CTGAN polish step.** A deep-generator polish (CTGAN / TabDDPM / SDV)
   on top of the rule-based sample would soften tails — slot it between
   Phase 1 and Phase 2 once you have a real seed dataset.
4. **No new originations during the 24-month window.** Closed pool, per
   the confirmed SOW. Pool size erodes from 500k at month 0 to ~430k at
   month 24 via prepayments and charge-offs.
5. **`property_valuation_type` is constant.** Set to
   `"Indexed/Origination Proxy"` everywhere. Real data flips after a
   re-valuation; add a small Bernoulli per cutoff to introduce variety.
6. **`loan_part_count > 1` is sampled but not realised as multiple rows.**
   The Hypoport reference also collapses to one row per `loan_id`.

---

## References

- **Statement of Work** — `../SCOPE_CONFIRMATION.md` (alongside the SOW PDF
  and the stakeholder-feedback PDF).
- **Hypoport / Green Lion reference data** — three monthly cutoffs of
  `green_lion_<yyyymm>_1_synthetic_loan_tape.csv` used as the schema and
  calibration anchor.
- **Data Designer** — <https://github.com/NVIDIA-NeMo/DataDesigner>
  (Apache 2.0). Docs: <https://nvidia-nemo.github.io/DataDesigner/>.
- **deeploans** — <https://github.com/Algoritmica-ai/deeploans> (Apache 2.0).
  Repository home for ETLs and the eventual landing folder for this
  generator (`synthetic-data-designer/`).
- **ESMA Annex 2** — *Underlying exposures — residential real estate.*
  Securitisation Regulation (EU) 2017/2402 disclosure technical standards.
- **Calibration sources cited in the SOW** — Fitch NQM1 / RATE 2025-J1
  presale reports; S&P Blackwattle Series RMBS presale; Moody's UK RMBS
  Forecast (29 May 2024); Morningstar DBRS UK Residential Mortgage Market
  Update 2025; AFME European Securitisation Data Snapshot Q2 2025.

---

## License

Apache License 2.0, matching the upstream deeploans repository.

---

## Quick command reference

```bash
# Install
pip install data-designer numpy pandas pyarrow duckdb

# Smoke (10 s)
python run.py --num-records 5000 --out-dir ./out_smoke

# Production (~7-10 min on a laptop)
python run.py --num-records 500000 --out-dir ./out_500k --deal-year 2024 --seed 42

# Tests
python tests/run_sql_tests.py --cutoff-dir ./out_500k/cutoffs --fail-on-violation

# Per-component (advanced)
python data_designer_loan_book.py --num-records 500000 --out ./out_500k/loan_book.parquet
python age_to_panel.py --loan-book ./out_500k/loan_book.parquet --out-dir ./out_500k/cutoffs
```
