# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generate ``notebooks/01_data_splits.ipynb`` — the loan-stratified temporal split walkthrough.

Kept as a builder (not a hand-written .ipynb) so the notebook is regenerated deterministically and
reviewed as plain Python. Run from anywhere::

    python notebooks/build_data_splits.py

The notebook explains what ``scripts/prepare_data.py`` does and renders the produced split's audit
manifest (``splits.meta.json``): per-split loan counts, origination ranges, temporal ordering, and
the fraction check. Full parquet-level validation (disjointness / completeness) is done by
``scripts/validate_splits.py`` — the notebook shows the command.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "notebooks" / "01_data_splits.ipynb"


def md(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_markdown_cell(text.strip("\n"))


def code(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_code_cell(text.strip("\n"))


CELLS = [
    md(r"""
# 01 · Data Splits — loan-stratified temporal train/val/test

Stage 2 of the pipeline (`scripts/prepare_data.py`). It takes the one ingested panel and cuts it
into **train / val / test**, enforcing the two leakage rules *physically* — as three separate
parquet files — so the evaluation mirrors production.

This notebook explains the split and renders the produced **audit manifest** (`splits.meta.json`):
per-split loan counts, origination ranges, the temporal ordering, and the fraction check. The full
parquet-level checks (disjointness, completeness) are done by `scripts/validate_splits.py`, shown
at the end.

**Contents**
1. What the split does
2. The invariants it must satisfy
3. The produced split (from the manifest)
4. Full parquet-level validation
5. How to run it
6. Notes &amp; caveats
"""),

    # ---------------------------------------------------------------- setup
    md("## Setup"),
    code(r"""
import json
from pathlib import Path

import pandas as pd
import yaml

# find the repo root (walk up until we see configs/)
ROOT = Path.cwd()
while not (ROOT / "configs" / "mortgage_performance").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
assert (ROOT / "configs" / "mortgage_performance").exists(), "run inside the credit-foundation-model repo"

CFG = yaml.safe_load((ROOT / "configs" / "mortgage_performance" / "prepare.yaml").read_text())

# the split writes splits.meta.json into its out_dir (on GCS); to render offline, drop a copy into
# one of these local paths (the manifest is tiny), or point META_PATH at it directly.
CANDIDATES = [ROOT / "reports" / "splits.meta.json",
              ROOT / "data" / "processed" / "splits.meta.json"]
META_PATH = next((p for p in CANDIDATES if p.exists()), None)
META = json.loads(META_PATH.read_text()) if META_PATH else None
print("prepare.yaml:", {k: CFG.get(k) for k in ("id_col", "origination_col", "fractions")})
print("manifest    :", META_PATH or "MISSING — see section 3 for how to produce/fetch it")
"""),

    # ---------------------------------------------------------------- what it does
    md(r"""
## 1. What the split does

Four steps:

1. **Load the panel** (and optionally cap it in time). If `reporting_max` is set (e.g.
   `2022-12-31`), every row *after* that date is dropped — this keeps the pretraining corpus blind
   to the future out-of-time (OOT) test era.
2. **Find one origination date per loan.** Two modes: use the real `origination_date` column
   (Mortgage), or *derive* `reporting_date − seasoning_months` (the Dutch panel, which has no
   origination column).
3. **Assign each loan to a split.** Sort loans by `(origination_date, loan_id)` and cut
   **positionally** — earliest 80% → train, next 10% → val, latest 10% → test.
4. **Write the outputs**: `train/val/test.parquet` (a loan's *entire* history travels together),
   `splits.csv` (`loan_id → split`), and `splits.meta.json` (the audit trail).

**The two rules, made physical:**

* **Split by loan, not by row** → a loan can never appear in two splits (else the model "recognizes"
  a test loan it already studied — a fake score).
* **Order by origination date** → train loans are *older* than test loans (train on the past,
  predict the future).
"""),

    # ---------------------------------------------------------------- invariants
    md(r"""
## 2. The invariants it must satisfy

| # | Invariant | Why it matters |
|---|-----------|----------------|
| **A** | train / val / test loan-sets are **disjoint** | the core leakage guard |
| **B** | each loan's **whole history** is in one split | no half-a-loan leaking across |
| **C** | **completeness** — splits ∪ = panel, matches `splits.csv` | no loan lost or duplicated |
| **D** | **temporal order**: train orig ≤ val orig ≤ test orig | train on the past, test on the future |
| **E** | counts &amp; ranges match `splits.meta.json` | the manifest doesn't lie |
| **F** | `reporting_max` respected (if set) | pretrain corpus stays blind to the test era |

Sections 3 (manifest) covers **D, E, F** and the fraction split; `validate_splits.py` (section 4)
proves **A, B, C** on the actual parquet files.
"""),

    # ---------------------------------------------------------------- produced split
    md(r"""
## 3. The produced split (from the manifest)

`splits.meta.json` is a small audit file the split writes next to the parquets. If it's missing
below, produce the split and copy the manifest down:

```bash
# produce the split (reporting_max keeps the pretrain corpus blind to the OOT test era)
python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
    --input gs://sriram-credit-fm-data/output/raw/mortgage_performance/panel_2000_2024.parquet \
    --run_name run_2000_2024 --reporting_max 2022-12-31

# fetch just the manifest for this notebook
gsutil cp gs://sriram-credit-fm-data/output/processed/mortgage_performance/run_2000_2024/splits.meta.json \
    reports/splits.meta.json
```
"""),
    code(r"""
if META:
    print(f"source panel : {META['source_panel']}")
    print(f"sha256       : {META['source_panel_sha256'][:16]}…")
    print(f"criterion    : {META['split_criterion']}")
    print(f"origination  : {META['origination_key']}")
    print(f"seed         : {META['seed']}   code_commit: {META.get('code_commit', '?')[:12]}")
    total = sum(META["n_loans"].values())
    rows = [{"split": s, "loans": META["n_loans"][s],
             "actual_frac": round(META["n_loans"][s] / total, 4),
             "target_frac": META["fractions"][i],
             "origination_min": META["origination_range"][s][0],
             "origination_max": META["origination_range"][s][1]}
            for i, s in enumerate(("train", "val", "test"))]
    SPLITS_DF = pd.DataFrame(rows).set_index("split")
    display(SPLITS_DF)
    rmax = (META.get("config") or {}).get("reporting_max")
    print(f"reporting_max cap: {rmax or '(none)'}")
else:
    SPLITS_DF = None
    print("No manifest — run prepare_data.py and copy splits.meta.json (see the commands above).")
"""),
    code(r"""
if SPLITS_DF is not None:
    import matplotlib.pyplot as plt
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4))
    # loan counts per split
    ax1.bar(SPLITS_DF.index, SPLITS_DF["loans"], color=["#4c72b0", "#dd8452", "#b3122f"])
    ax1.set_title("Loans per split")
    ax1.set_ylabel("loans")
    for i, v in enumerate(SPLITS_DF["loans"]):
        ax1.text(i, v, f"{v:,}", ha="center", va="bottom")
    # temporal ordering: origination range per split as horizontal bars (train oldest → test newest)
    lo = pd.to_datetime(SPLITS_DF["origination_min"])
    hi = pd.to_datetime(SPLITS_DF["origination_max"])
    y = range(len(SPLITS_DF))
    ax2.barh(list(y), (hi - lo).dt.days, left=lo.map(lambda d: d.toordinal()),
             color=["#4c72b0", "#dd8452", "#b3122f"])
    ax2.set_yticks(list(y))
    ax2.set_yticklabels(SPLITS_DF.index)
    ax2.set_title("Origination window per split (train oldest → test newest)")
    xt = ax2.get_xticks()
    ax2.set_xticklabels([pd.Timestamp.fromordinal(int(t)).year if t > 0 else "" for t in xt])
    plt.tight_layout()
    plt.show()
"""),
    code(r"""
# D) temporal-order check straight from the manifest (ISO date strings sort chronologically)
if SPLITS_DF is not None:
    r = META["origination_range"]
    ordered = r["train"][1] <= r["val"][0] and r["val"][1] <= r["test"][0]
    print("D: train orig <= val orig <= test orig  →", "PASS ✓" if ordered else "FAIL ✗")
    print(f"   train  {r['train'][0]} .. {r['train'][1]}")
    print(f"   val    {r['val'][0]} .. {r['val'][1]}")
    print(f"   test   {r['test'][0]} .. {r['test'][1]}")
"""),

    # ---------------------------------------------------------------- full validation
    md(r"""
## 4. Full parquet-level validation

The manifest proves **D/E/F** and the fractions. To prove **A/B/C** — that no loan leaks across
splits and the union is the whole panel — run the artifact validator on the actual files (it reads
only the id / origination / reporting columns, so it's cheap):

```bash
python scripts/validate_splits.py \
    --dir gs://sriram-credit-fm-data/output/processed/mortgage_performance/run_2000_2024
```

Expected output ends `ALL CHECKS PASSED`, with the key lines:

```
PASS  A: train/val/test loan-sets are disjoint
PASS  C: parquet membership matches splits.csv
PASS  C: no loan lost or duplicated across splits (partition of csv)
PASS  D: train orig <= val orig <= test orig (recomputed)
```

The same invariants are locked in `tests/test_prepare_data.py` (unit tests for the origination
logic + an end-to-end run through the validator, including a negative control that injects a leaked
loan and confirms the validator **fails**).
"""),

    # ---------------------------------------------------------------- how to run
    md(r"""
## 5. How to run it

The split is one command: read the ingested panel, write `{train,val,test}.parquet` + `splits.csv`
+ `splits.meta.json` under the run directory.

```bash
# the split used for the scaling run (10% panel, capped at 2022 so pretrain is blind to the OOT era)
python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
    --input  gs://sriram-credit-fm-data/output/raw/mortgage_performance/panel_2000_2024_10pct.parquet \
    --run_name run_2000_2022_10pct \
    --reporting_max 2022-12-31

# then always audit the produced files (disjoint + complete + temporally ordered)
python scripts/validate_splits.py \
    --dir gs://sriram-credit-fm-data/output/processed/mortgage_performance/run_2000_2022_10pct
```

Key flags (override any recipe key on the CLI):

| Flag | Meaning |
|---|---|
| `--input` | the ingested panel to split (stage 1's output) |
| `--run_name` | names the output dir `.../processed/mortgage_performance/<run_name>/` |
| `--reporting_max` | drop rows after this date (caps the pretrain corpus); omit for full history |
| `--fractions` | train/val/test split, e.g. `'[0.8, 0.1, 0.1]'` (default) |
| `--seed` | the loan-hash seed (default 42) — same seed → same split |

### Where it fits
```
ingest → [ PREPARE_DATA / split ] → train_tokenizer → encode → pretrain
```
It consumes stage 1's `panel*.parquet` and produces the `train.parquet` that the tokenizer fits on
(train only, DL-008) and that `encode` then turns into shards. To fetch the tiny `splits.meta.json`
for this notebook, copy it locally:
```bash
gsutil cp gs://.../processed/mortgage_performance/run_2000_2022_10pct/splits.meta.json reports/
```
"""),

    # ---------------------------------------------------------------- caveats
    md(r"""
## 6. Notes &amp; caveats

* **Whole history travels together.** Splitting is by `loan_id`, so all of a loan's monthly rows
  land in one split. Total rows are preserved (`train + val + test == panel`).
* **The label is not built here.** These splits carry the full panel columns (including the
  delinquency markers). The forward-looking `default_event` label — "did this performing loan
  default within the horizon?" — is assembled *later*, at task/fine-tune time, from the same panel.
  (See the Data Bible, section 3, for how the label is built despite leakage removal.)
* **`reporting_max` is a protocol choice.** Setting it (e.g. `2022-12-31`) caps the pretraining
  corpus so it never sees the OOT test era; leave it off for a full-history split. The validator
  only checks the cap if the manifest recorded one.
* **The XGBoost OOT baseline is a separate track.** `scripts/build_oot_baseline.py` builds its own
  observation table straight from the raw cohort files and does **not** use this split — don't
  cross-wire the two.
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
