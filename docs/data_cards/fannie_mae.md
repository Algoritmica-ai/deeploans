# Data Card — Fannie Mae Single-Family Loan Performance (credit_fm reference)

*Apache-2.0 framework · data is public, redistributed under Fannie Mae's terms · card v1, 4 Jul 2026*

## Summary

Real-world US single-family (1–4 unit) fixed-rate mortgage servicing records used to pretrain and
evaluate the Fannie Mae reference credit foundation model. Monthly loan-level performance from
origination through termination or the data cut.

## Source and provenance

- **Publisher:** Fannie Mae — Single-Family Loan Performance Data (publicly released for credit-risk
  transparency). Access via Fannie Mae's data portal under their terms of use.
- **Coverage used:** 2000Q1–2024Q4 (25 years).
- **Ingestion:** `scripts/ingest_fannie_mae.py` reads the published quarterly parquet snapshots,
  renames to a canonical schema, and derives the modelling columns. No raw data is committed to the
  repository (gitignored); only fitted artifacts (bin edges, vocabularies) and aggregate reports.

## Composition

- **As ingested (4% loan sample):** ~2,264,282 loans · ~125M loan-month rows.
- **Pretraining corpus (capped at Dec-2022):** ~1.75M train / ~219k val loans · ~1.2B tokens.
- **Grain:** one row per loan per reporting month. A loan contributes its full history (up to 60
  most-recent months are tokenized).
- **Sampling:** deterministic hash on loan id keeps a whole loan or drops it — reproducible, and
  keeps each loan's history intact. 4% is a representative cross-section (all vintages, states,
  risk tiers proportionally).

## Fields

- **Static / profile (origination):** original interest rate, original UPB, loan term, LTV, CLTV,
  DTI, number of borrowers/units, borrower & co-borrower credit scores (origination/issuance),
  mortgage-insurance %, channel, first-time-buyer flag, loan purpose, property type, occupancy,
  property state, amortization type, and several program/eligibility flags.
- **Dynamic / event (monthly):** current interest rate, current actual UPB, interest-bearing UPB,
  remaining months to maturity, scheduled/total/unscheduled principal, current credit scores,
  MI-cancellation indicator, and loan age.
- **Time:** `reporting_date` (month), `loan_age`, and a `yearquarter` calendar coordinate.

## Labels

- `default_event` = loan reaches **180+ days delinquent (D180)** OR terminates as a credit-loss
  event (foreclosure, short sale, REO/deed-in-lieu, note sale). Derived at ingest from Fannie's
  recorded delinquency status and zero-balance codes.
- `is_performing`, `prepay_event` derived similarly. Labels are recorded real-world outcomes, not
  human annotations. Downstream label = default within 12 months of an observation cutoff.

## Leakage handling (critical)

~50 columns are **excluded** because they reveal the outcome or only exist after termination:
current delinquency status, zero-balance codes and dates, foreclosure/disposition dates, all loss
and expense fields, and the label itself. These are dropped from both the foundation model and the
XGBoost baseline. A leaky configuration scores ~0.93 ROC — a mirage that is never reported.

## Splits and temporal integrity

- **By loan, never by row:** a loan's entire history stays in one split (no peeking at future months
  of a training loan).
- **By origination time (temporal), never random:** older-originated loans train, newer test —
  matching real deployment (train on the past, score newer loans).
- **Out-of-time cap:** for the OOT program the processed corpus is truncated at Dec-2022, so the
  pretrained model cannot have observed the 2023–2024 evaluation period.
- Splits and an audit manifest (seed, source SHA-256, counts, origination ranges, git commit,
  resolved config) are written by `scripts/prepare_data.py`.

## Preprocessing

Numeric fields are quantile-bucketed with bin edges fit on the **train split only** (with anchors
at regulatory thresholds). Categoricals map to their training vocabulary (`UNK`/`NA` for
unseen/missing). Encoding is deterministic and reproducible via
`configs/fannie_mae/tokenizer_v2.json`.

## Privacy and sensitive attributes

- Fannie Mae's public files are already de-identified (no borrower names, addresses, or SSNs);
  the finest geography is state (`property_state`) and a truncated ZIP prefix (excluded as
  high-cardinality). No direct protected-class attributes are present.
- **However**, geography and other features can act as **proxies** for protected classes. Any
  lending use requires fair-lending / disparate-impact analysis (see the model card).

## Licensing and access

- The data is Fannie Mae's, provided under their terms of use; users must obtain it from Fannie
  Mae directly. This repository redistributes **no raw data** — only code, fitted tokenizer
  artifacts (aggregate statistics, no individual records), and evaluation reports.

## Limitations

- US conforming single-family fixed-rate mortgages only — not representative of other products,
  geographies, or non-conforming credit.
- 4% sample; loans sampled independently, so cross-loan effects (e.g. neighbourhood co-default) are
  thinned (irrelevant to the per-loan model but noted).
- Coverage ends Dec-2024, so the latest observation cutoff with a full 12-month label is Dec-2023.

## Maintenance

- Regenerate via `scripts/ingest.py -c configs/fannie_mae/ingest_2000_2024.yaml` then
  `prepare_data.py`. New Fannie releases extend the reporting range; re-fit the tokenizer on the
  new train split (DL-008) if the vintage span changes materially.
