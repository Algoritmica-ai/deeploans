# Part 5 — The Dataset

## 5.1 The Fannie Mae Single-Family Loan Performance dataset

**Plain English:** Fannie Mae is a US government-sponsored company that buys mortgages from banks.
Since it owns millions of them, it publishes (anonymized) how every one of those loans behaved,
month by month, since the year 2000. It is the closest thing to a public "flight recorder" of the
US mortgage market — including the 2008 crash and COVID.

**The numbers:** ~25 years (2000–2024), tens of millions of fixed-rate loans, **~3.3 billion
loan-month rows**, 113 published columns. This repo's validated 4% loan-hash sample ≈ 160M rows;
the 10% sample used for the headline ≈ 400M; the streaming path (Part 17) unlocks 100%.

**Why this dataset:** it's *real* (not synthetic — actual defaults, actual crisis), *public*
(anyone can audit our results), *long* (spans two genuine stress regimes: 2008–10 and 2020), and
*panel-shaped* (exactly the sequence structure a foundation model needs).

The full column-by-column reference lives in `notebooks/00_data_bible.ipynb` and
`reference_implementations/fannie_mae/fannie_glossary.py`. Here are the concepts you must own.

## 5.2 Panel data and loan snapshots

**Panel data** = the same entities observed repeatedly over time. One **row = one loan in one
month** (a "loan-month" or *snapshot*):

```
loan_id        reporting_date   current_upb   dlq   ...
731942800123   2020-03-31       186,900       0          ← the loan, in March
731942800123   2020-04-30       186,211       1          ← the same loan, in April
731942800123   2020-05-31       186,211       2
```

Beginners' trap #1: treating rows as independent. They are 66 frames of one movie. Every leakage
rule in Part 8 exists because of this.

## 5.3 The two clocks: reporting time vs origination time

Every loan lives on two time axes, and confusing them causes real bugs:

- **Origination date** — when the loan was born (our loan: 2016-03). Determines *vintage*: loans
  born in 2006 (peak bubble underwriting) behave differently from 2012 vintages forever.
- **Reporting period** — the calendar month a row describes. Determines *regime*: in April 2020
  every vintage stumbled at once.

```
                 reporting time ──▶
                 2016 2017 2018 2019 2020 2021
origination      ┌────────────────────────────┐
   2016 vintage  │ ████████████████████████░  │  ← our loan: born 2016, dies (prepay) 2021
   2018 vintage  │        ██████████████████  │
   2020 vintage  │                  ████████  │
                 └────────────────────────────┘
                                    ▲
                              a CRISIS is a vertical stripe (hits all vintages);
                              a BAD VINTAGE is a horizontal stripe
```

The raw files are **hive-partitioned by reporting period**:
`fannie_by_reporting/reporting_year=2020/reporting_quarter=Q2/…parquet`. **Hive partitioning**
just means the directory *names* carry column values, so a reader can skip irrelevant quarters
without opening files. Consequence: one loan's rows are scattered across ~all quarters of its
life — which is why the split/encode stages must regroup by loan (Part 17's bucketing).

The split key is **origination** (train = oldest loans), while the pretrain cap and the OOT
evaluation are on **reporting** dates. Both clocks, used deliberately.

## 5.4 Delinquency — the early-warning ladder

`current_loan_delinquency_status`: how many payments behind the borrower is. `0` = current,
`1` = 30 days late, `2` = 60 days, … `"XX"` = unknown (servicer didn't report — becomes `NA`,
and every consumer must `.fillna(False)` on flags derived from it; this nullable-boolean subtlety
has bitten this repo before).

```
0 ──▶ 1 ──▶ 2 ──▶ 3 ──▶ 4 ──▶ 5 ──▶ 6+ ──────▶ (foreclosure pipeline)
current 30d  60d  90d  120d 150d  D180 = our default definition
      ◀──── "cure": catching up moves you back left ────
```

Most delinquencies cure (our Ohio loan did). The deep ladder rarely does — hence the industry
convention that ~180 days ("D180") marks a **default event**.

## 5.5 Zero-Balance Codes — how loans die

When a loan's balance hits zero, `zero_balance_code` says why. The adapter maps them
(`reference_implementations/fannie_mae/adapter.py`):

| ZBC | Meaning | Our interpretation |
|---|---|---|
| 01 | Prepaid or matured | **prepay_event** (the good exit — refinance/sale/payoff) |
| 02 | Third-party sale | default (credit event) |
| 03 | Short sale | default (credit event) |
| 09 | REO / deed-in-lieu (bank takes the property) | default (credit event) |
| 15 | Note sale | default (credit event) |

So the derived labels are:

- `default_event` = `dlq_num >= 6` **or** ZBC ∈ {02,03,09,15}
- `prepay_event` = ZBC == 01
- `is_performing` = `dlq_num == 0` **and** not terminated — the *gate* (§5.6)

Structural invariant (locked by a test): a row is never performing *and* defaulted, and a
termination is a default *or* a prepay, never both.

## 5.6 Observation windows and the gate

A prediction is anchored at a **cutoff** (observation date). Everything before it is *features*;
a fixed **horizon** after it is where the *label* may occur:

```
        history (visible)             │ cutoff        horizon (12 months)
────────────────────────────────────▶│◀──────────────────────────────▶
 ...all rows ≤ 2019-12-31...         2019-12-31      default anywhere in 2020 → label=1
```

The **gate** (`is_performing` at the cutoff): we only score loans that are *current* at
observation. Why? A loan already 150 days delinquent doesn't need a model — predicting its
default is reading, not forecasting. Gating makes the task "predict **new** trouble," which is
the decision a bank actually faces. (`observe_panel` in `inference/scoring.py` implements both
the truncation and the gate; the G2 label layer generalizes gates beyond booleans.)

## 5.7 The important fields, and why each matters

**Profile (static, known at origination — emitted once per loan):**

| Field | Why it matters |
|---|---|
| `original_ltv` (loan-to-value, %) | Borrower's skin in the game. 87 = 13% equity. Cliffs at 80/90/95/97 are regulatory/pricing thresholds — the tokenizer *anchors* bin edges there |
| `dti` (debt-to-income, %) | Payment burden. 43/45 are qualified-mortgage cliffs (also anchored) |
| `credit_score` (FICO) | The borrower's past behavior, compressed to 300–850 |
| `original_interest_rate`, `original_upb` | Pricing and size at birth |
| `channel` (R/C/B) | Retail/Correspondent/Broker — origination channels have different risk cultures |
| `loan_purpose` (P/C/R) | Purchase / Cash-out refi / Rate refi — cash-out historically riskier |
| `number_of_borrowers`, `first_time_home_buyer_indicator`, `occupancy_status`, `property_type` | Household structure and use (investor loans behave differently) |

**Event (dynamic, one value per month):**

| Field | Why it matters |
|---|---|
| `current_actual_upb` | The balance path — amortization speed, curtailments |
| `current_interest_rate` | With prevailing market rates, this *is* the refinance incentive (why our loan prepaid when rates hit 2.9%) |
| `loan_age` / remaining months | Seasoning: default hazard peaks around years 2–5 |
| `reporting_date` → `cal=` token | The macro regime — the single most load-bearing "field" for crisis learning |

**Leakage columns (44 of them — present in raw, banned as features):** current delinquency
status itself, all zero-balance/foreclosure/disposition fields, loss and modification amounts,
REO listing prices… Everything that *is* the outcome or is only populated on the road to it.
Part 8 explains the machinery that keeps them out; the canonical list is
`configs/fannie_mae/dataset.yaml`.

## 5.8 How rare is default? (the class-imbalance headline)

From the full 3.31B-row book: pooled default rate **≈ 0.65%** of loan-months; the 2010 crisis
peak ≈ 1.8%; COVID blip ≈ 0.73%. At a December-2022 observation snapshot, the 12-month default
rate is ~**0.14%** — 1-2 defaults per thousand performing loans. This single fact shapes half the
engineering: PR-AUC as the metric that matters (Part 14), negative downsampling + weight caps in
fine-tuning (Part 8), and calibration as a mandatory stage (Part 15).

## 5.9 The second dataset: Dutch mortgages (validation)

`configs/dutch_mortgages/` runs a synthetic Dutch RMBS panel (ESMA Annex 2 schema, 71 columns,
no origination column — it's *derived* as `reporting − seasoning`, DL-007) through the identical
scripts. Purpose: (a) prove the framework is YAML-only for a wildly different schema, and (b) a
controlled ceiling experiment — the panel hides a `_segment` variable that generates behavior,
so we can measure how much of the recoverable signal the model finds. Its numbers are never
compared with Fannie's (synthetic panels flatter every model).

### Things to remember

1. Panel data: rows are frames of one loan's movie — never independent samples.
2. Two clocks: origination (vintage — the split key) vs reporting (regime — the cap and the cal= token).
3. default = D180 delinquency OR a credit-event zero-balance code; prepay = ZBC 01; the gate = performing-at-cutoff.
4. Base rate ~0.65% of loan-months (~0.14% at an observation) drives PR-AUC, rebalancing, and calibration choices.
5. 44 raw columns are outcome-adjacent → the machine-enforced leakage list.

---
*Next: [Part 6 — Ingestion](06_ingestion.md): the code that tames all of the above.*
