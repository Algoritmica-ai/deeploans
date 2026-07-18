# Part 8 — Data Preparation: Splits, Labels, Leakage, Imbalance

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ [SPLIT] ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ serve


This part covers the concepts that make credit ML *different* — where naïve ML habits produce
beautiful, worthless numbers.

## 8.1 Future leakage — the cardinal sin

**Plain English:** letting the model peek at information that wouldn't exist at prediction time.
Like grading a weather forecaster who wrote the forecast after looking out tomorrow's window.

**Why it's THE issue in credit:** the raw data is *full* of outcome-adjacent columns.
`current_loan_delinquency_status` at scoring time practically *is* the label. `foreclosure_date`
only exists for loans that failed. A model given these scores 0.95+ ROC in backtests and
collapses in production — the most expensive failure mode in the field, because it's invisible
until deployed.

**The four leakage channels, and this repo's counter to each:**

```
channel                                    counter (all machine-enforced)
1. Outcome columns as features        →   dataset.yaml `leakage:` list (44 cols);
                                          classify_schema drops them BEFORE proposing fields;
                                          validate_dataset check F fails if one sneaks in
2. Same loan in train and test        →   temporal_loan_split assigns whole LOANS;
                                          validate_splits check A (disjointness)
3. Future rows in the history         →   observe_panel truncates to <= cutoff (a test proves
                                          poisoned post-cutoff rows can't change any score)
4. Statistics fitted on test data     →   tokenizer bins/vocab fit on TRAIN only (DL-008);
                                          calibrator REFUSES test-window cutoffs (G6.1)
```

## 8.2 The split: loan-disjoint AND temporal

`credit_fm/data/splits.py::temporal_loan_split` — deceptively small, doubly careful:

1. **By loan, never by row.** All 66 rows of the Ohio loan travel together.
2. **Ordered by origination.** Sort loans by (origination, id — deterministic tie-break), cut
   80/10/10 positionally: train = oldest loans, test = newest. This mirrors production reality:
   you always score loans newer than your training data.

```
        WRONG (random by row)              RIGHT (temporal by loan)
   train: loan A months 1-40           train: loans originated 2000–2017 (all months)
   test:  loan A months 41-66          val:   ~2018 vintages
   → memorization + hindsight          test:  newest vintages
```

Plus the **reporting cap**: `--reporting_max 2022-12-31` deletes all 2023+ rows before anything
else, so even the *unlabeled pretraining* never reads the era we evaluate on.

## 8.3 Labels: sliding windows over the panel

There is no "label column" in the data — labels are **constructed** from (cutoff, horizon):

```
label(loan, cutoff) = 1  iff  event_col fires in (cutoff, cutoff + horizon_months]
observed(loan, cutoff)   iff  gate_col ∈ gate_values at the loan's last row ≤ cutoff
```

`credit_fm/data/labels.py::forward_event_entities` implements it; the *definition* lives in
`dataset.yaml`:

```yaml
labels:
  default_12m: {type: forward_event, event_col: default_event, horizon_months: 12, gate_col: is_performing}
  prepay_12m:  {type: forward_event, event_col: prepay_event,  horizon_months: 12, gate_col: is_performing}
```

One loan yields **many observations** — the OOT protocol observes every loan each December it's
performing (2016…2021 for fitting, 2022/2023 for testing). Subtlety: the last usable *training*
cutoff must leave a full horizon before the test era, or training labels overlap test time — the
embargo idea. And loans observed in both eras are hash-assigned wholly to one side
(the loan-disjoint guard inside `finetune.py`, done with hashtable lookups because `np.isin` on
millions of strings runs for hours — a real war story in the code comments).

## 8.4 Class imbalance: training when 1 in 700 is positive

At a 2022 cutoff, ~0.14% of observations are defaults. Feed that raw to a classifier and the
lazy optimum is "predict 0 always" — 99.86% accurate, zero use. The M4 run genuinely collapsed
this way (raw inverse-frequency weighting at 0.11% base rate destabilized training).

The repo's three-part treatment (`finetune.py`, config `train:`):

1. **Negative downsampling — fit set only.** `neg_per_pos: 50` keeps all positives and 50
   negatives per positive *in the gradient-descent set only*. Ranking metrics stay honest
   because…
2. **…monitor/test sets are untouched** — a 10% monitoring split at the TRUE base rate reports
   val ROC every epoch, so a collapsing run is visible after epoch 1 (best epoch restored).
3. **Capped positive weight.** `pos_weight_cap: 50` bounds the loss weight on positives —
   uncapped inverse-frequency (~700×) makes single batches explode.

Cost, accepted knowingly: predicted probabilities become **uncalibrated by design** (the model
lives in a rebalanced world where defaults look ~50× more common). Ranking survives; levels are
fixed later by the calibration stage (Part 15). This "rank now, calibrate later" separation is
standard industrial practice and now you know why.

## 8.5 Hash sampling (determinism as a design principle)

Everywhere a subset is needed, the repo hashes stable ids instead of drawing randoms:

```python
hash(loan_id) % 100 < 10        # the 10% ingest sample
hash(loan_id) %   2 == 0        # OOT overlap → test side
hash(loan_id) % buckets         # streaming: which bucket dir a loan lands in
```

Properties worth internalizing: reproducible (no seed to lose), consistent across files/quarters
(a loan is in or out *everywhere*), composable (the 4% sample is a subset of the 10%), and
argument-free in an audit ("which loans?" → "exactly these, recompute the hash").

## 8.6 Train / validation / test / monitoring — four roles, not three

| Set | Made by | Used for | Never used for |
|---|---|---|---|
| train | `prepare_data` (oldest 80% of loans) | pretraining + tokenizer fitting | metrics |
| val | middle 10% | pretraining early-warning (val loss, best-checkpoint pick) | headline claims |
| test | newest 10% | *reserved*; the OOT protocol's test is the **future cutoffs** (2022/2023) | anything during development |
| monitoring | 10% of the fine-tune pool, TRUE base rate | per-epoch val ROC during fine-tuning; best-epoch restore | the final metric |

The distinction between "val loss during pretraining" (masked-token reconstruction, deterministic
masking seed so it's comparable across epochs) and "val ROC during fine-tuning" (the actual task)
confuses everyone once. They share a name and nothing else.

## 8.7 Streaming preparation (when the panel outgrows RAM)

`prepare_data --stream true` (v1.1 G3.2) computes the *identical* split without ever loading the
panel — pass 1 streams only id+origination columns to build the assignment; pass 2 streams all
rows into `<split>/bucket-<k>/` **loan-hash bucket** directories, so each bucket holds *whole
loans* and the encoder can process one bucket at a time. Equivalence to the in-RAM path is proven
by test (same `splits.csv`, same rows). Details in Part 17.

### Things to remember

1. Four leakage channels, four machine-enforced counters — memorize the mapping.
2. Split by loan, ordered by origination; `reporting_max` blinds even pretraining to the test era.
3. Labels are constructed from (cutoff, horizon, gate) and declared in dataset.yaml — not stored in the data.
4. Rebalance the FIT set only; monitor at the true base rate; accept uncalibrated probabilities and fix them later.
5. Anything that needs a subset gets a hash of the loan_id, never a random draw.

---
*Next: [Part 9 — Tokenization](09_tokenization.md): the spreadsheet becomes a language.*
