---
title: Synthetic Data Designer
emoji: 📊
colorFrom: green
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
license: apache-2.0
short_description: Design, generate and age synthetic loan tapes from a schema
---

# Synthetic Data Designer

Give it a schema and, if you have one, sample data. It works out how the data
behaves, generates a synthetic portfolio, and ages it forward period by period —
loans paying down, falling behind, prepaying, defaulting.

A six-step wizard: **Upload → Review → Configure → Generate → Results → Download**.

Three calibrated packs are bundled, so you can see the whole thing work without
uploading anything:

- **European CLO — Leveraged Loans** (58 columns) — start here. An open
  portfolio of European corporate loans that trades: bullet repayment, credit
  migration through watchlist and distress, a nine-month workout after default,
  new collateral bought throughout a reinvestment period, and credit ratings
  that migrate on their own chain rather than being read off the credit state
- **Dutch Green Loans — Residential Mortgages** (ESMA Annex 2, 71 columns)
- **European Auto Loans — ESMA Annex 5** (44 columns, depreciating collateral,
  balloon payments, recovery on write-off)

Load one on the first screen, press through to Generate, and you have a
validated multi-period panel with charts and five download formats.

## What this does not do

It generates **the loans a fund owns**, not the fund. There are no tranches, no
interest or principal waterfall, no OC or IC tests, no management fees, no
tranche pricing or cash flows, and no equity returns. A CLO run gives you the
collateral pool month by month; turning that into AAA-through-equity outcomes is
a separate problem and a separate product.

It does not replicate any rating agency's model. Where a figure resembles one —
the average credit factor, the effective obligor count — it is computed from
this project's own published assumptions or from ordinary statistics, and the
pack file shows the arithmetic. Those numbers are not an agency's and should not
be read as an agency's.

**Nothing here is investment advice, and none of the data is real.** Every
portfolio is synthetic, generated from declared assumptions calibrated to
published market ranges. It is built for testing systems, training models and
demonstrating pipelines — not for valuing anything.

## ⚠️ This is a shared, public demo

Anything you upload is written to this Space's disk, into a workspace shared
with every other visitor, and whoever operates this Space can read it. Storage
is temporary — it is wiped whenever the Space restarts or goes to sleep.

**Do not upload confidential data here.**

The tool is designed to run locally, where nothing leaves your machine:

```bash
pip install 'synthetic-data-designer[web] @ git+https://github.com/sriramarun/synthetic-data-designer'
sdd ui
```

Source: <https://github.com/sriramarun/synthetic-data-designer>

## Limits on this instance

| | |
|---|---|
| Loans per run | 50,000 |
| Periods | 60 |
| Rows of output | 3,000,000 |
| Upload size | 50 MB |
| CTGAN / Hybrid methods | unavailable — they need PyTorch, which is too large and too slow for a basic CPU Space |

A loan is one contract; a **row** is one loan at one cut-off. 50,000 loans over
60 periods comes to roughly 2.5 million rows, which is why both are quoted —
either one alone would misstate what you get.

Run it locally for anything bigger. There are no limits there.

## What it does

- **Profiles** a sample: column types, static vs dynamic, distributions,
  the lifecycle and transition matrix, correlations — measured, not assumed
- **Generates** by one of six methods, from moment-matched normals to a deep
  tabular model, each written into the configuration as generators you can read
- **Ages** the portfolio forward with amortisation, indices, arrears, defaults,
  recoveries, and optionally new loans written during the window
- **Groups** entities under a shared parent where the data has one — several
  facilities behind a single obligor, several mortgages behind one household —
  so concentration means something
- **Migrates** a second state machine alongside the first, which is how a credit
  rating drifts while a loan is still performing rather than only when it is not
- **Validates** the result against invariants derived from the configuration
  itself, and shows you every check
- **Exports** CSV, Parquet, Excel, the configuration as YAML, and a standalone
  validation report

Apache 2.0.
