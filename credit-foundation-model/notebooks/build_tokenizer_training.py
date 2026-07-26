# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generate ``notebooks/03_tokenizer_training.ipynb`` — the train_tokenizer stage walkthrough.

Kept as a builder (not a hand-written .ipynb) so the notebook is regenerated deterministically and
reviewed as plain Python. Run from anywhere::

    python notebooks/build_tokenizer_training.py

The notebook explains ``scripts/train_tokenizer.py`` (stage 4) — how the KVT vocabulary is **fit on
TRAIN only** (leakage rule DL-008), how each field becomes ``field=value`` tokens (numeric quantile
buckets with anchored regulatory cliffs, capped categoricals, the ``t=`` age coordinate and the
``cal=<YYYYQ#>`` macro-regime token), and how a loan is assembled into a token sequence. It renders
the real frozen ``configs/mortgage_performance/tokenizer.json`` (552 tokens) and runs small **live** demos of
the bucketer / categorical / full loan encoder — all from committed configs, no GCS.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "notebooks" / "03_tokenizer_training.ipynb"


def md(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_markdown_cell(text.strip("\n"))


def code(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_code_cell(text.strip("\n"))


CELLS = [
    md(r"""
# 03 · Train Tokenizer — turning a loan into `field=value` tokens

Stage 4 of the pipeline (`scripts/train_tokenizer.py`). The previous stage decided *which* columns
are features and whether each is numeric/categorical, static/dynamic. This stage learns **how to
turn each value into a token** — and freezes the whole vocabulary into one `tokenizer.json` that
train, val, test, and inference all reuse.

The output is the model's **alphabet**. A transformer can't read `original_ltv = 79.6`; it reads
integer token ids. The tokenizer is the dictionary that maps `original_ltv=4` ↔ id `137`, learned
once, on the training loans only.

**The one rule that matters:** the vocabulary — every bin edge, every kept category — is fit on the
**train split only** (leakage rule **DL-008**). If a bin boundary were placed using test-set values,
the model would have peeked at the future. So this stage reads `train.parquet` and nothing else.

**Contents**
1. What this stage produces
2. The KVT token — anatomy of one loan
3. Numeric fields → quantile buckets (+ anchored cliffs) &nbsp;·&nbsp; *live demo*
4. Categorical fields → capped category set (+ UNK/NA) &nbsp;·&nbsp; *live demo*
5. Time &amp; calendar tokens — `t=` and `cal=<YYYYQ#>`
6. Assembling the vocabulary — the frozen 552-token dictionary
7. Encoding a whole loan &nbsp;·&nbsp; *live demo*
8. The QA report the script writes
9. How to run it
10. Notes &amp; caveats
"""),

    # ---------------------------------------------------------------- setup
    md("## Setup — committed configs only (no GCS, no data)"),
    code(r"""
import json
import sys
from pathlib import Path

import pandas as pd
import yaml

# find the repo root (walk up until we see configs/)
ROOT = Path.cwd()
while not (ROOT / "configs" / "mortgage_performance").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
assert (ROOT / "configs" / "mortgage_performance").exists(), "run inside the credit-foundation-model repo"

# so `import credit_fm...` works when the notebook runs from notebooks/
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

CFG = ROOT / "configs" / "mortgage_performance"
SCHEMA = yaml.safe_load((CFG / "tokenizer.yaml").read_text())      # field schema + bins/anchors/calendar
FIT = yaml.safe_load((CFG / "tokenizer_fit.yaml").read_text())     # the fit recipe (train path, out, QA)
FROZEN = json.loads((CFG / "tokenizer.json").read_text())          # the FITTED, frozen tokenizer
print("schema time_field :", SCHEMA["time_field"], "| calendar:", SCHEMA["calendar"])
print("default n_bins    :", SCHEMA["n_bins"], "| max_events:", SCHEMA["max_events"])
print("frozen vocab size :", len(FROZEN["vocab"]), "tokens")
"""),

    # ---------------------------------------------------------------- what it produces
    md(r"""
## 1. What this stage produces

`train_tokenizer.py` reads the **train** panel and writes two files:

| File | What it is |
|---|---|
| `configs/mortgage_performance/tokenizer.json` | the **frozen tokenizer** — vocabulary + every bin edge + every kept category. This is the artifact everything downstream loads. |
| `reports/mortgage_tokenizer_report.md` | a **QA report** — vocab size, sequence-length distribution, and token-health rates (roundtrip lossless, `=UNK`, `=NA`). |

Three ideas make KVT ("key-value-time") what it is:

1. **Fused `field=value` tokens.** Not a generic number stream — each token *names its field*.
   `original_ltv=4` and `dti=4` are **different** tokens, so the model never confuses an LTV bucket
   with a DTI bucket. (NVIDIA-TFM style.)
2. **Time coordinates.** Every monthly event carries a `t=<age bin>` (how old the loan is) and a
   `cal=<YYYYQ#>` (the absolute calendar quarter) — so the History encoder can tell 2005 from 2008.
3. **Fit once on train, frozen forever.** Bins/categories are learned from training values only and
   serialized; val/test/inference reuse identical ids. No re-fitting, no drift.
"""),

    # ---------------------------------------------------------------- token anatomy
    md(r"""
## 2. The KVT token — anatomy of one loan

A loan is a **profile** (fixed origination facts, written once) followed by its **monthly events**
(the loan's history, one block per reporting month, most-recent `max_events=60` kept):

```
[BOS] [USR]
  original_ltv=4  channel=R  dti=6  fico=9 ...            ← PROFILE (static, once)
  [EVT_START] t=2 cal=2005Q1  current_interest_rate=7  current_actual_upb=11 ... [EVT_END]
  [EVT_START] t=3 cal=2005Q2  current_interest_rate=7  current_actual_upb=11 ... [EVT_END]
  ...
[EOS]
```

- `[BOS]`/`[EOS]` bracket the loan; `[USR]` is the slot whose final hidden state becomes the loan
  embedding (like `[CLS]` in BERT).
- The **profile block** feeds the *Profile* encoder (read once). Each **`[EVT_START]…[EVT_END]`**
  block feeds the *Event* encoder (read per month). This profile/event split is exactly the
  static/dynamic classification from notebook `02`.
- `t=` and `cal=` sit at the **head of every event** so time is a first-class token, not an
  afterthought.

Every token also carries three metadata streams the model needs — `branch` (profile/event/special),
`event_index` (which month), and `field_type` (which field) — produced by
`tokenizer.encode_with_meta()`. Notebook `04` (encode) uses those; here we focus on the tokens
themselves.
"""),

    # ---------------------------------------------------------------- numeric
    md(r"""
## 3. Numeric fields → quantile buckets (with anchored cliffs)

A continuous value like `original_ltv = 79.6` can't be its own token (thousands of distinct values →
a useless, sparse vocabulary). So each numeric field is **bucketed** into `n_bins` (default 16)
**quantile** bins learned on the training values. Quantile (not equal-width) means each bucket holds
roughly the same number of loans — no wasted buckets in empty ranges.

Special labels: **`0`** = an exact zero (kept distinct — "no co-borrower score" ≠ "a low score"),
**`NA`** = missing. A value **beyond the training range is clamped** into the edge bucket — it can
never invent a new one (that keeps the frozen vocab closed).

**Anchors — the regulatory-cliff fix.** Credit has hard thresholds: LTV **80** (PMI), **90/95/97**;
DTI **43/45** (QM). A pure quantile bin might blur `79.9` and `80.1` into one bucket, throwing away a
sharp signal an XGBoost split gets for free. `anchors:` **forces a bin boundary** exactly at those
cutpoints. the source's anchors:
"""),
    code(r"""
print("Per-field bin overrides (more resolution for high-signal fields):")
for f, n in SCHEMA.get("bins", {}).items():
    print(f"  {f:42s} {n} bins")
print("\nForced boundaries at regulatory cliffs (anchors):")
for f, cuts in SCHEMA.get("anchors", {}).items():
    print(f"  {f:42s} {cuts}")
"""),
    md(r"""
### Live demo — the bucketer, and the anchor at LTV 80

`NumericBucketer` is a small, self-contained class; we can fit one on toy values and watch `79.9`
and `80.1` land in **different** buckets purely because `80` is an anchor.
"""),
    code(r"""
from credit_fm.tokenizer.numeric_bucketer import NumericBucketer

vals = pd.Series([50, 60, 70, 75, 78, 82, 85, 90, 95, 97, 100, 0, 0])   # note the two exact zeros
nb = NumericBucketer(n_bins=8, anchors=[80, 90, 95, 97]).fit(vals)      # anchored like original_ltv
print("bucket labels this field can emit:", nb.vocab())
print()
for x in [79.9, 80.1, 0.0, float("nan"), 250.0]:
    print(f"  original_ltv={x!s:>5}  ->  bucket '{nb.transform(x)}'"
          + ("   <- anchor 80 splits 79.9|80.1" if x in (79.9, 80.1) else "")
          + ("   <- exact zero"  if x == 0.0 else "")
          + ("   <- missing"     if x != x else "")
          + ("   <- clamped (out of train range)" if x == 250.0 else ""))
"""),

    # ---------------------------------------------------------------- categorical
    md(r"""
## 4. Categorical fields → capped category set (+ `UNK`/`NA`)

A categorical field (`channel`, `loan_purpose`, `property_state`) keeps **one token per category
seen in training**, most-frequent first, capped at `max_categories` (256). Two escape hatches keep
the vocabulary closed and honest:

- **`UNK`** — a category never seen in training (or beyond the cap). At inference a brand-new
  servicer code doesn't crash; it maps to `field=UNK`.
- **`NA`** — the value was missing.

That's the whole leakage guarantee for categoricals: the category set is *learned on train*, so
test-only categories are literally unrepresentable except as `UNK`.
"""),
    code(r"""
from credit_fm.tokenizer.categorical import CategoricalTokenizer

# train sees R (x3), C (x2), B (x1), X (x1); cap at 3 -> only the top 3 survive
ct = CategoricalTokenizer(max_categories=3).fit(pd.Series(["R","R","R","C","C","B","X"]))
print("kept categories + escape hatches:", ct.vocab())
for v in ["R", "X", "Z", None]:
    print(f"  channel={v!s:>4}  ->  '{ct.transform(v)}'"
          + ("   <- seen, kept"            if v == "R" else "")
          + ("   <- seen but past the cap" if v == "X" else "")
          + ("   <- unseen in train"       if v == "Z" else "")
          + ("   <- missing"               if v is None else ""))
"""),

    # ---------------------------------------------------------------- time + calendar
    md(r"""
## 5. Time &amp; calendar tokens — `t=` and `cal=<YYYYQ#>`

Two different notions of time, both at the head of every event block:

- **`t=<bin>` — loan age.** `time_field: loan_age` is bucketed by the *same* quantile machinery as
  any numeric field (its own `t=` prefix). This is *relative* time: how many months into the loan.
- **`cal=<YYYYQ#>` — absolute calendar quarter.** Derived from `reporting_date` (`calendar:
  yearquarter`). This is the **macro-regime** signal — it's what lets the model distinguish a payment
  in 2005 from the same-age payment in 2008. Without it, two loans with identical internal histories
  but originated five years apart would look the same; with it, the History encoder can learn that
  "2008Q1…2009Q4" was a very different world.

**Fairness note:** `cal=` is a *time coordinate*, not an outcome — it never leaks the label. And when
real macro series (HPI, prevailing rate, unemployment) are joined into the panel later, they're just
additional `event` fields; the XGBoost baseline gets the same columns, so the comparison stays fair.
"""),
    code(r"""
# how reporting_date -> cal token (the exact rule the tokenizer uses)
from credit_fm.tokenizer.key_value_time import KVTTokenizer as _K
for d in ["2005-02-01", "2008-09-01", "2020-12-15"]:
    print(f"  reporting_date {d}  ->  cal={_K._calendar([d], 'yearquarter').iloc[0]}")
"""),

    # ---------------------------------------------------------------- vocabulary assembly
    md(r"""
## 6. Assembling the vocabulary — the frozen 552-token dictionary

After fitting every field, the tokenizer walks each field's labels and adds a fused token per
`(field, label)` pair, plus the `t=` age bins, the `cal=` quarters, and the 9 structural specials.
The result is the frozen `tokenizer.json`. Here's the real breakdown of the committed vocabulary:
"""),
    code(r"""
from collections import Counter

def kind(t):
    if t.startswith("[") and t.endswith("]"):
        return "structural specials ([BOS] [USR] [EVT_START] ...)"
    key = t.split("=", 1)[0]
    if key == "t":
        return "t=  (loan-age bins)"
    if key == "cal":
        return "cal=  (calendar quarters)"
    return "field=value  (profile + event fields)"

counts = Counter(kind(t) for t in FROZEN["vocab"])
rows = pd.DataFrame({"tokens": counts}).sort_values("tokens", ascending=False)
rows.loc["TOTAL"] = rows["tokens"].sum()
print(f"frozen vocabulary: {len(FROZEN['vocab'])} tokens")
display(rows)
"""),
    code(r"""
# how many DISTINCT fields, and how many value-tokens each contributes (top 12 widest fields)
field_tokens = [t for t in FROZEN["vocab"] if "=" in t and t.split("=", 1)[0] not in ("t", "cal")]
per_field = Counter(t.split("=",1)[0] for t in field_tokens)
n_fields = len(per_field)
print(f"{n_fields} distinct fields tokenized  ->  {len(field_tokens)} field=value tokens\n")
widest = pd.Series(dict(per_field)).sort_values(ascending=False).head(12)
widest.name = "value tokens"
display(widest.to_frame())
print("(numeric fields ~= n_bins+2 tokens for 0/NA; anchored/override fields carry more.)")
"""),
    code(r"""
# a peek at the actual tokens for one field, straight from the frozen vocab
show = "original_ltv"
toks = [t for t in FROZEN["vocab"] if t.startswith(show + "=")]
print(f"{show} ({len(toks)} tokens):")
print(" ", toks)
"""),

    # ---------------------------------------------------------------- encode a loan
    md(r"""
## 7. Encoding a whole loan — live, with the frozen tokenizer

We can load the **real frozen tokenizer** and encode a tiny synthetic 2-month loan. (Fields we don't
supply simply become `field=NA` — exactly what happens for genuinely missing data.) This needs no
GCS and no training data — just the committed `tokenizer.json`.
"""),
    code(r"""
from credit_fm.tokenizer import KVTTokenizer

tok = KVTTokenizer.load(str(CFG / "tokenizer.json"))

# a 2-row monthly panel for one loan (only a few fields filled; the rest -> =NA)
loan = pd.DataFrame([
    {"loan_id": "L1", "reporting_date": "2005-03-01", "loan_age": 2,
     "original_ltv": 79.6, "channel": "R", "current_interest_rate": 6.5},
    {"loan_id": "L1", "reporting_date": "2005-04-01", "loan_age": 3,
     "original_ltv": 79.6, "channel": "R", "current_interest_rate": 6.5},
])

toks = tok.tokens(loan)                       # the fused token strings
ids  = tok.encode(loan)                       # the integer ids the model actually sees
print(f"loan -> {len(toks)} tokens, {tok.vocab_size}-token vocab\n")
print("first tokens (profile block):")
print(" ", " ".join(toks[:20]))
print("\nfirst event block:")
start = toks.index("[EVT_START]")
print(" ", " ".join(toks[start:toks.index("[EVT_END]") + 1]))
print("\nsame block as ids:", ids[start:toks.index("[EVT_END]") + 1])
print("\nroundtrip lossless (decode(encode) == tokens):", tok.decode(ids) == toks)
"""),
    md(r"""
Notice `original_ltv=` gets a real bucket (79.6 is anchored below 80) and `channel=R` a real
category, while every field we didn't provide shows `=NA`. The two events share the same profile but
carry their own `t=`/`cal=` coordinates. That token stream — plus the per-token branch/event/field
metadata — is exactly what stage 5 (`encode`) freezes into shards for the model.
"""),

    # ---------------------------------------------------------------- QA report
    md(r"""
## 8. The QA report the script writes

`train_tokenizer.py` doesn't just save the tokenizer — it encodes a sample of `qa_loans` (2000)
loans and writes `reports/mortgage_tokenizer_report.md` so you can sanity-check the fit **without
training anything**:

- **Vocabulary** — total size + profile/event field counts.
- **Sequence length** — min / median / p95 / max tokens per loan (drives the model's context budget;
  `max_events=60` caps it).
- **Token health** — three numbers that must look right:

| Metric | What healthy looks like | Why it matters |
|---|---|---|
| **roundtrip lossless** | ~100% of loans | `decode(encode(loan))` returns the same tokens → no id corruption |
| **`=UNK` rate** | low (a few %) | high `UNK` = the category cap is too tight, or val/test drift |
| **`=NA` rate** | expected for sparse fields | flags fields that are mostly missing (candidates to drop) |

This is the artifact-level check for this stage (the code-level checks live in `tests/`).
"""),

    # ---------------------------------------------------------------- how to run
    md(r"""
## 9. How to run it

```bash
# fit on the train split, save tokenizer.json + write the QA report
python scripts/train_tokenizer.py -c configs/mortgage_performance/tokenizer_fit.yaml

# optional: cap the rows used to fit (faster; the fit is a quantile/count estimate)
python scripts/train_tokenizer.py -c configs/mortgage_performance/tokenizer_fit.yaml --max_fit_rows 500000
```

The recipe (`tokenizer_fit.yaml`) points `train:` at `${paths.processed}/train.parquet` — the
**train split only** — and the field schema comes from its `schema:` key (`tokenizer.yaml`). Output
paths:
"""),
    code(r"""
print("fit recipe:")
print("  train  :", FIT["train"])
print("  out    :", FIT["out"])
print("  report :", FIT["report"])
print("  qa_loans:", FIT["qa_loans"], "| max_fit_rows:", FIT["max_fit_rows"])
"""),

    # ---------------------------------------------------------------- caveats
    md(r"""
## 10. Notes &amp; caveats

* **Train-only, frozen forever (DL-008).** Every bin edge and category is fit on `train.parquet`.
  Val/test/inference **load** `tokenizer.json` — they never re-fit. This is the leakage guarantee
  that makes the OOT results trustworthy.
* **Bucketing is lossy *by design*.** `original_upb=137,500` → `original_upb=8`. The model reasons
  over *ranks/bands*, not exact dollars — so "roundtrip lossless" means **token-level** (the id
  stream is stable), not value reconstruction. That's the right target for a ranking model.
* **Anchors are where domain knowledge enters the tokenizer.** They're the deliberate concession that
  credit has hard regulatory cliffs a naive quantile grid would smear. Set them per field in
  `tokenizer.yaml`.
* **`cal=` is a coordinate, never a label.** It encodes *when*, not *what happened* — no leakage, and
  the baseline gets the same information for a fair fight.
* **This vocabulary is the frozen contract.** Changing fields, bins, or anchors re-mints
  `tokenizer.json` and invalidates every downstream shard and checkpoint — so it's frozen once the
  architecture locks (it's been stable since M1: 552 tokens).
* **Next:** notebook `04` (encode) takes this frozen tokenizer and turns the whole panel into the
  `(input_ids, branch, event_index, field_type)` shards the model trains on — the "encode once, train
  many" step.
"""),
]


def main() -> None:
    nb = nbf.v4.new_notebook()
    nb["cells"] = CELLS
    nb["metadata"] = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    }
    OUT.write_text(nbf.writes(nb))
    print(f"wrote {OUT}  ({len(CELLS)} cells)")


if __name__ == "__main__":
    main()
