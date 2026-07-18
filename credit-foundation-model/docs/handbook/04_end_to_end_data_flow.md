# Part 4 — End-to-End Data Flow: One Loan's Journey

Meet **Loan 731942800123** — "the Ohio loan." We follow it from raw CSV rows to a calibrated
probability of default, showing the data *before and after every stage*. Its story:

```
Mar 2016   originated: $200,000, 30-year fixed at 4.125%, LTV 87, DTI 41, FICO 715, Ohio
2016–2019  pays like clockwork
Apr 2020   misses a payment (COVID job loss) → 30 days delinquent
May 2020   60 days delinquent
Jun 2020   catches up (cure) → current again
Oct 2021   refinances at 2.9% elsewhere → loan PREPAYS (zero-balance code 01)
```

66 monthly rows. No default — but a visible stumble and an early exit. Exactly the kind of story
snapshot models can't read.

## 4.0 The map

```
raw quarterly rows ─▶ [ingest] contract panel ─▶ [validate] ─▶ [split] train/val/test
      ─▶ [tokenizer fit: train only] tokenizer.json ─▶ [encode] token-id shards
      ─▶ [pretrain: MLM] backbone.pt ─▶ [fine-tune] ft.pt ─▶ [score] 0.31 raw
      ─▶ [calibrate] PD 0.0042 ─▶ [serve]
```

## 4.1 Raw data: what arrives

Fannie Mae publishes performance data by **reporting period**. Our loan's rows are scattered
across ~22 quarterly hive partitions (`reporting_year=2020/reporting_quarter=Q2/…`). Raw
columns use the published names; dates are `MMYYYY` strings; codes are cryptic:

| loan_identifier | monthly_reporting_period | origination_date | current_loan_delinquency_status | zero_balance_code | current_actual_upb | original_ltv | ... (113 cols) |
|---|---|---|---|---|---|---|---|
| 731942800123 | 042020 | 032016 | 1 | | 186211.44 | 87 | ... |
| 731942800123 | 052020 | 032016 | 2 | | 186211.44 | 87 | ... |
| 731942800123 | 102021 | 032016 | 0 | 01 | 0.00 | 87 | ... |

**Why this shape is hostile to modelling:** dates aren't sortable strings, delinquency `"XX"`
means "unknown," a single concept (loan ended) hides inside `zero_balance_code`, and one loan's
history spans many files.

## 4.2 Ingest: raw → contract panel

`scripts/ingest.py` (asset-blind driver) calls `FannieMaeAdapter.load_source()` per quarter,
which runs `_derive()` — the only place Fannie's quirks are known:

```
BEFORE (raw, 1 row)                       AFTER (contract panel, same row)
loan_identifier: "731942800123"           loan_id: "731942800123"        (str, renamed)
monthly_reporting_period: "042020"        reporting_date: "2020-04-30"   (ISO month-end)
origination_date: "032016"                origination_date: "2016-03-31" (ISO, in place)
current_loan_delinquency_status: "1"      dlq_num: 1                     (numeric; "XX"→NA)
zero_balance_code: ""                     default_event: False           (dlq>=6 OR credit-event ZBC)
                                          prepay_event: False            (ZBC == "01")
                                          is_performing: False           (dlq==0 and not terminated)
(+ all 113 raw columns preserved — leakage is dropped LATER, at the schema stage)
```

For the October 2021 row: `zero_balance_code="01"` → `prepay_event=True`. Our loan's label
columns now tell its story mechanically.

The panel is written as **one shard per quarter** with a completion sidecar
(`part-2020Q2.parquet` + `_meta-2020Q2.json`) — kill the job, rerun, finished quarters skip.
A deterministic hash of `loan_id` decides whether the loan is in the 4%/10% sample at all
(same loans every run — no randomness to argue about).

**Why:** downstream code should never again think about MMYYYY or ZBC codes. One adapter, one
place, tested by `tests/test_ingest_fannie_mae.py` on hand-crafted rows covering every case.

## 4.3 Validation: trust, then verify

`validate_ingest.py` re-derives `reporting_date`, `dlq_num`, and all three flags *from the raw
columns kept in the panel* and compares — proving the artifact, not just the code. For our loan
it checks, e.g., that the April-2020 row's `is_performing` is `False` (dlq=1) and the flags are
mutually exclusive (a month is never both performing and prepaid). Part 7 walks every rule.

## 4.4 Split: which pile does the Ohio loan go to?

`prepare_data.py` orders **loans** (not rows) by origination date and cuts 80/10/10:

```
loans sorted by origination ──────────────────────────────────────────▶ time
[========== train 80% ==========][== val 10% ==][===== test 10% =====]
   2000 ..............  ~2018-ish    ......           newest loans
```

Originated 2016, our loan lands in **train**. Crucially, *all 66 of its rows* go to train —
a loan never straddles piles (that would let the model memorize it in training and "predict" it
in test). With `reporting_max: 2022-12-31`, any 2023+ rows are dropped first, keeping the
pretraining corpus blind to the out-of-time evaluation era. Our loan ended in 2021, so all 66
rows survive.

## 4.5 Tokenization: the loan becomes a sentence

The tokenizer (fit on train only — Part 9) turns each monthly row into fused `field=value`
tokens. Our loan's opening, as actual token strings:

```
[BOS] [USR]
  original_ltv=5  dti=6  credit_score=4  channel=R  loan_purpose=P  ...   ← profile: once
  [EVT_START] t=1 cal=2016Q1 current_interest_rate=7 current_upb=12 ... [EVT_END]   ← Mar 2016
  [EVT_START] t=1 cal=2016Q2 current_interest_rate=7 current_upb=12 ... [EVT_END]   ← Apr 2016
  ...
  [EVT_START] t=9 cal=2020Q2 current_interest_rate=7 current_upb=9  ... [EVT_END]   ← Apr 2020
  ...
[EOS]
```

Notes on what you're seeing: `original_ltv=5` means "LTV in train-quantile bin 5" (87 falls in
the 80–90 anchor band); `cal=2020Q2` places the month in *calendar* time — how the model can
learn "spring 2020 was special"; `max_events: 60` keeps the **most recent 60 months**, so a
360-month loan is represented by its freshest 5 years.

## 4.6 Encode: strings → integers, once

`encode_dataset.py` maps each token string to its vocabulary id and stores **four aligned
arrays** per loan (this is the data-layer contract the model consumes):

```
tokens:      [BOS] [USR] original_ltv=5 ... [EVT_START] t=1 cal=2016Q1 upb=12 [EVT_END] ...
input_ids:     1     5        217       ...      7        63    412      88       8     ...
event_index:  -1    -1        -1        ...      0         0     0        0       0     ... 1 1 ...
field_type:   -1    -1         0        ...     -1        11    12        6      -1     ...
branch:       -1    -1         0        ...     -1         1     1        1       1     ...
```

- `event_index` — which month a token belongs to (−1 = profile/structural) → drives per-month pooling
- `field_type` — which *field* it encodes → drives type-level masking
- `branch` — 0 profile / 1 event / −1 structural → routes tokens to encoder branches

One row per loan in `shard-00042.parquet`, plus `n_tokens` (ours: ~950) and `n_events` (60).
A `manifest.json` records shard list + counts. This is why pretraining epochs are cheap: the
expensive per-loan tokenization happened exactly once.

## 4.7 Pretraining: learning without labels

Each time our loan is drawn into a batch, the collator hides part of it (fresh randomness every
epoch): maybe `cal=2020Q2` and the whole May-2020 event get masked. The model must reconstruct
them from context — and to guess "the delinquency field in May 2020 says 60-days-late," it has
to have understood April's stumble. Multiply by millions of loans × 20,000 steps: the model
internalizes repayment grammar. Nothing here ever told it what "default" means. Loss: 6.56 →
0.14 (train), 0.33 (val). The output artifact is the **backbone checkpoint**.

## 4.8 Embedding: the loan becomes 768 numbers

At inference, no masking. The three branches (Part 11) compress our loan:

```
profile tokens ──▶ Profile encoder ──▶ 1 profile vector ┐
66 events      ──▶ Event encoder   ──▶ 66 event vectors ┼─▶ History encoder ─▶ [USR] vector (768,)
                                                        ┘        (reads the timeline order)
```

Somewhere in those 768 numbers: "stumbled in a crisis, cured fast, rate-sensitive." Nobody
programmed those concepts.

## 4.9 Fine-tune & score: the question gets asked

Fine-tuning (Part 13) trains `classification_head` (and, in `full` mode, nudges the backbone) on
labeled observations: *given history up to a cutoff, does the loan default within 12 months?*
Scoring our loan at cutoff **2019-12-31** (history truncated to ≤ that date — the model cannot
see 2020, that's the leakage guard in `observe_panel`):

```
raw score:  0.31        ← ranks the loan (riskier than ~85% of the book that month)
                          but NOT a probability (the model trained on rebalanced data)
```

## 4.10 Calibrate & serve: a number a bank can use

`calibrate.py` fitted an isotonic map on a held-out 2021 cutoff (never a test window — it
refuses). Applied:

```
raw 0.31 ──isotonic──▶ PD = 0.0042   (0.42% probability of default in 12 months)
```

Same ranking, honest level. `serve.py` exposes exactly this path over HTTP: POST the loan's
rows + a cutoff, get `{"score": 0.31, "pd": 0.0042, "rank": …}` — and a test proves the HTTP
number equals the batch number.

## 4.11 Why each transformation exists — the one-line recap

| Transformation | Without it |
|---|---|
| Derive ISO dates / label flags (ingest) | Every downstream stage re-parses MMYYYY and ZBC codes, inconsistently |
| Loan-disjoint temporal split | Metrics are fiction (memorization + hindsight) |
| Fit tokenizer on train only | Test-set value ranges leak into the vocabulary |
| Fused `field=value` tokens | The model can't tell *which field* a value came from |
| `event_index`/`field_type`/`branch` metadata | No per-month pooling, no structured masking, no branch routing |
| Encode once to shards | GPUs starve while CPUs re-tokenize every epoch |
| Masking during pretraining | Nothing to learn — reconstruction of visible input is copying |
| Cutoff truncation at scoring | The score "predicts" the future by reading it |
| Calibration | Scores rank correctly but overstate risk ~50× (rebalanced training) |

### Things to remember

1. One loan: 66 raw rows → ~950 tokens → ~950 vectors → 66 month vectors → 1 loan vector → score 0.31 → PD 0.0042.
2. Every transformation exists to prevent a specific failure — the recap table is worth re-reading whole.
3. `observe_panel`'s cutoff truncation is why a score cannot peek at the future (and a test proves it).
4. Encode-once shards are why pretraining epochs are cheap.

---
*Next: [Part 5 — The Dataset](05_dataset.md): what these mortgage fields actually mean.*
