# Part 7 — Validation

> **You are here:**  raw ─▶ ingest ─▶ [VALIDATE] ─▶ split ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ serve


> Files: `scripts/validate_ingest.py`, `validate_splits.py`, `validate_dataset.py`,
> `validate_scores.py` — the four **artifact auditors**.

## 7.1 Why validation exists (and why it's a separate stage)

**Plain English:** the pipeline's stages hand each other files. A validator is the receiving
inspector who opens the crate and checks the goods against the packing list — instead of trusting
the sender's word.

**Why not just unit tests?** Unit tests prove the *code* is right on synthetic data, at
commit-time. Validators prove *this particular multi-hundred-GB artifact* is right, at run-time.
Different failure classes:

| Failure | Caught by unit tests? | Caught by validator? |
|---|---|---|
| Bug in `_derive`'s ZBC mapping | ✅ | ✅ (re-derivation mismatches) |
| You pointed the config at last month's split | ❌ | ✅ (counts/manifest disagree) |
| A crash left a truncated parquet | ❌ | ✅ |
| Upstream source silently changed a column's encoding | ❌ | ✅ |

**Why separate scripts (not asserts inside the producer)?** (1) An auditor that shares the
producer's code would share its bugs — validators deliberately **re-derive from raw** using
independent logic. (2) Read-only auditors can be pointed at *any* artifact any time ("is this
2-month-old split still trustworthy?"). (3) The exit code gates orchestration: `run_*.sh` scripts
use `set -e`, so a FAIL stops the multi-day pipeline before GPUs burn on bad data.

**The negative-control convention (repo law):** every validator must have a test proving it
**FAILS on corrupted input**. A validator that can't fail is a rubber stamp. You'll see tests
literally poison artifacts — inject a train loan into test, set a score to 1.5 — and assert the
auditor catches it.

## 7.2 `validate_ingest.py` — auditing the panel

Checks (default: first row-group for speed; `--full` for global counts):

| Check | Rule | If it failed, it would mean… |
|---|---|---|
| Non-empty | rows > 0 | ingest silently wrote nothing (bad source path) |
| Re-derivation | `reporting_date`, `dlq_num`, `default_event`, `prepay_event`, `is_performing` recomputed **from the retained raw columns** match the stored ones exactly | the adapter version that wrote this panel had different business logic than today's — labels untrustworthy |
| ISO dates | every `reporting_date` matches `YYYY-MM-DD` and is a month-end | date parsing regressed; `<= cutoff` string comparisons would silently misorder |
| Hash bound | sampled loan share ≈ `--sample-pct` (statistical bound) | the sample isn't the deterministic hash population — mixed panels |
| Mutual exclusivity | never performing ∧ (defaulted ∨ prepaid); termination is default XOR prepay | the label derivation is internally inconsistent — downstream labels are noise |

**Good vs bad example:**

```
GOOD  row: dlq="2",  zbc=""    → stored default_event=False, is_performing=False   PASS
BAD   row: dlq="0",  zbc="09"  → stored default_event=False                        FAIL
      (ZBC 09 = REO: re-derivation says default_event MUST be True → the writing
       code predates the credit-event mapping → re-ingest before anything else runs)
```

Sharded panels: point it at any shard (`--panel …/panel_2000_2024/part-2009Q1.parquet`) — the
rerun script audits one benign and one crisis quarter.

## 7.3 `validate_splits.py` — auditing the leakage guard

The most safety-critical auditor. Checks A–F over `{train,val,test}` (single parquets **or** the
streamed `bucket-*/` dirs — auto-detected):

- **A: disjoint loan sets** — `train ∩ val = train ∩ test = val ∩ test = ∅`. *The* leakage guard:
  one shared loan means the model can memorize in train what it's graded on in test.
- **C: completeness vs `splits.csv`** — the parquets' membership equals the recorded assignment;
  no loan lost or duplicated. (Ids compared as **strings** — numeric-looking ids again.)
- **D: temporal order** — max(train origination) ≤ min(val) ≤ … recomputed from the files.
  Failure = the "train on the past" story is false.
- **E: manifest agreement** — counts and origination ranges match `splits.meta.json`.
- **F: `reporting_max` respected** — no row after the cap; failure = the pretraining corpus saw
  the evaluation era.

Negative control in the tests: copy one train loan's rows into test → A must FAIL (it does).

## 7.4 `validate_dataset.py` — auditing the contract (v1.1 G1)

Runs the `dataset.yaml` contract against a real panel — the onboarding gate for new datasets:
contract columns present (A), ids string-typed (B), ISO month-ends (C), one row per (id, time)
(D), label event/gate domains sane (E), **tokenizer schema contains no leakage/exclude column**
(F — the machine enforcement of the no-peek list), gate/terminal consistency (G).

## 7.5 `validate_scores.py` — auditing predictions

Structural (always): schema; scores ∈ [0,1], no NaN; one row per loan; `n_events ≥ 1`; single
cutoff; manifest agreement. With `--labeled-panel` (a *past* cutoff whose outcomes exist):

- **Population reconciliation first** — scored ⊆ panel (check G), duplicate/coverage counts.
  Rationale, learned the hard way: *a plausible ROC on the wrong snapshot is a trap* — verify
  you scored the population you think you scored, then compute metrics.
- ROC/PR + a recall@K / lift table (the operational read at rare base rates), optional
  `--min-roc` gate (H).
- **Check I (calibration, v1.1 G6):** when a calibrated `pd` column is present — pd ∈ [0,1];
  Brier + reliability table printed; **calibration-in-the-large** gated (mean pd within 2× of
  the realized rate). Raw rebalanced-model scores fail this by ~50× — that's the point.

## 7.6 How this protects model quality — the chain of custody

```
ingest ──validate_ingest──▶ panel is faithful to the raw source
split  ──validate_splits──▶ no loan crosses the wall; time flows forward
schema ──validate_dataset─▶ no outcome column is a feature
scores ──validate_scores──▶ the right population, honest metrics, honest probabilities
```

Break any link and every downstream number is decoration. This chain is why the repo can claim
its 0.8468 with a straight face — and it's the part most ML projects skip.

### Things to remember

1. Unit tests audit the code; validators audit the artifact — different failure classes, you need both.
2. A validator that cannot fail is decoration: every one has a poisoned-input negative control.
3. validate_splits check A (train/val/test loan-disjointness) is the single most safety-critical check in the repo.
4. Exit codes gate orchestration: a FAIL stops the pipeline before GPUs burn on bad data.

---
*Next: [Part 8 — Data Preparation](08_data_preparation.md): splits, labels, leakage, and imbalance in depth.*
