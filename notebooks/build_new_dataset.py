# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generate ``notebooks/05_new_dataset.ipynb`` — bring your own dataset in 5 steps (v1.1 G1.6).

Kept as a builder (not a hand-written .ipynb) so the notebook is regenerated deterministically and
reviewed as plain Python. Run from anywhere::

    python notebooks/build_new_dataset.py

The notebook onboards a **toy "auto loans" asset live**: shape a panel to the dataset contract →
write ``dataset.yaml`` → run ``validate_dataset`` → generate the field schema with
``classify_schema`` → fit the KVT tokenizer and encode a shard. Everything runs offline in a temp
directory (no GCS, no GPU) — the real scripts, real validators, real tokenizer.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "notebooks" / "05_new_dataset.ipynb"


def md(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_markdown_cell(text.strip("\n"))


def code(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_code_cell(text.strip("\n"))


CELLS = [
    md(r"""
# 05 · Bring Your Own Dataset — onboarding a new asset in 5 steps

The framework is **asset-blind**: nothing in `credit_fm` knows about Fannie Mae or Dutch
mortgages (that's enforced by a test, `tests/test_asset_blind.py`). A new dataset plugs in through
**one contract file** (`dataset.yaml`) and — only if your raw data needs custom derivations — one
small adapter class. Every pipeline stage then runs unchanged.

This notebook onboards a **toy "auto loans" asset live**, end-to-end, offline:

| Step | What | Tool |
|---|---|---|
| 1 | Shape your panel to the **contract** | your code (or a `DatasetAdapter`) |
| 2 | Write **`dataset.yaml`** — columns, labels, leakage | one yaml file |
| 3 | **Audit** the panel against the contract | `scripts/validate_dataset.py` |
| 4 | **Generate the field schema** (leakage auto-dropped) | `scripts/classify_schema.py` |
| 5 | **Fit the tokenizer + encode** | `scripts/train_tokenizer.py` / `encode_dataset.py` |

After step 5 you're on the standard highway: `pretrain → finetune → score → validate` with stock
configs. No framework code is edited at any point.
"""),

    # ---------------------------------------------------------------- setup
    md("## Setup"),
    code(r"""
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# find the repo root (walk up until we see configs/)
ROOT = Path.cwd()
while not (ROOT / "configs" / "fannie_mae").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
assert (ROOT / "configs" / "fannie_mae").exists(), "run inside the credit-foundation-model repo"
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

WORK = Path(tempfile.mkdtemp(prefix="toy_auto_loans_"))   # everything lands here; no GCS
print("working dir:", WORK)

def run(script, *args):
    "Run a stage script exactly as you would from the shell; echo its output."
    cmd = [sys.executable, f"scripts/{script}", *args]
    print("$", " ".join(str(c) for c in cmd), "\n")
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                          env={"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"})
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr[-1500:])
    return proc.returncode
"""),

    # ---------------------------------------------------------------- step 1
    md(r"""
## Step 1 — Shape your panel to the contract

The contract is small. Your panel must be **one row per entity per period** with:

* an **id column** (always strings — numeric ids corrupt on CSV round-trips),
* a **time column** — ISO `YYYY-MM-DD` **month-end** strings,
* an **origination column** (the temporal-split key; or mark it `origination_derived`),
* your **label ingredients**: a boolean *event* column (did the bad thing happen this month?)
  and a *gate* column (is the entity healthy right now?).

If your raw source needs parsing/derivations to get there (like Fannie's `MMYYYY` dates and
delinquency codes), that logic goes into a `DatasetAdapter` under `reference_implementations/` —
see `reference_implementations/fannie_mae/adapter.py` for the worked example. Our toy asset is
synthesized directly in contract shape:
"""),
    code(r"""
rng = np.random.default_rng(7)
rows = []
for i in range(300):                                     # 300 toy auto loans, 12 months each
    fico = int(rng.integers(550, 820))
    term = int(rng.choice([36, 48, 60, 72]))
    apr = round(float(np.clip(22 - fico / 50 + rng.normal(0, 0.6), 2.5, 19.0)), 2)
    balance = float(rng.integers(8_000, 45_000))
    will_default = rng.random() < (0.25 if fico < 620 else 0.04)   # weak-FICO loans default more
    default_month = int(rng.integers(6, 12)) if will_default else None
    for m in range(12):
        defaulted_now = default_month is not None and m >= default_month
        rows.append({
            "loan_id": f"AUTO{i:04d}",                                        # str ids ✓
            "reporting_date": (pd.Timestamp("2021-01-31") + pd.offsets.MonthEnd(m)).strftime("%Y-%m-%d"),
            "origination_date": "2020-12-31",                                 # ISO month-end ✓
            "fico": fico, "term_months": term, "apr": apr,                    # static features
            "vehicle_type": str(rng.choice(["sedan", "suv", "truck"])),
            "months_on_book": m + 1,                                          # dynamic features
            "current_balance": round(balance * (1 - m / term), 2),
            "days_past_due": 95 if defaulted_now else int(rng.random() < 0.06) * 15,   # LEAKAGE
            "default_event": defaulted_now,                                   # label ingredient
            "is_performing": not defaulted_now,                               # gate ingredient
        })
panel = pd.DataFrame(rows)
panel_path = WORK / "auto_panel.parquet"
panel.to_parquet(panel_path, index=False)
print(f"{panel['loan_id'].nunique()} loans x 12 months = {len(panel):,} rows")
panel.head(3)
"""),

    # ---------------------------------------------------------------- step 2
    md(r"""
## Step 2 — Write `dataset.yaml` (the whole onboarding artifact)

Three blocks: the **column contract**, the **declarative labels** (what to predict — no code),
and the **leakage/exclude lists** (what may never be a feature). Two rules the loader enforces:
a label's `event_col`/`gate_col` must themselves be listed as leakage (the label is the answer),
and no column may be in both lists.
"""),
    code(r"""
from credit_fm.data.dataset_config import load_dataset_config

dataset_yaml = WORK / "dataset.yaml"
dataset_yaml.write_text(yaml.safe_dump({
    "dataset": {
        "name": "toy_auto_loans",
        "adapter": "generic",                # panel already conforms -> zero adapter code
        "id_col": "loan_id",
        "time_col": "reporting_date",
        "origination_col": "origination_date",
    },
    "labels": {
        "default_9m": {"type": "forward_event", "event_col": "default_event",
                       "horizon_months": 9, "gate_col": "is_performing"},
    },
    "leakage": ["days_past_due", "default_event", "is_performing"],   # the answer, in 3 flavours
    "exclude": [],
    "schema": str(WORK / "tokenizer.gen.yaml"),
}, sort_keys=False))
print(dataset_yaml.read_text())

# the contract parses + validates in one call — this is what every consumer reads
ds = load_dataset_config(dataset_yaml)
print(f"parsed: asset '{ds.name}', {len(ds.labels)} label(s), {len(ds.banned)} banned columns")
"""),

    # ---------------------------------------------------------------- step 3
    md(r"""
## Step 3 — Audit the panel against the contract

`validate_dataset.py` re-derives the invariants from the actual file — string ids, ISO month-end
dates, one row per (id, period), label domains, and (once it exists) that the feature schema
contains no banned column. Run it before anything else; it's cheap and it fails loudly.
"""),
    code(r"""
rc = run("validate_dataset.py", "--dataset", dataset_yaml, "--panel", panel_path)
assert rc == 0, "contract audit failed"
"""),

    # ---------------------------------------------------------------- step 4
    md(r"""
## Step 4 — Generate the field schema (leakage physically can't get in)

`classify_schema.py` reads the panel, classifies every column (static vs dynamic, numeric vs
categorical), and — because the recipe points at your `dataset.yaml` — **drops the leakage and
exclude columns before classification even starts**. `days_past_due` is the classic trap: it
predicts default almost perfectly *because it is the answer*; watch it get dropped.
"""),
    code(r"""
classify_yaml = WORK / "classify.yaml"
classify_yaml.write_text(yaml.safe_dump({
    "input": str(panel_path), "id_col": "loan_id", "time_col": "reporting_date",
    "dataset": str(dataset_yaml), "drop": [], "out": str(WORK / "tokenizer.gen.yaml"),
    "key": None,
}))
rc = run("classify_schema.py", "-c", classify_yaml)
assert rc == 0
schema = yaml.safe_load((WORK / "tokenizer.gen.yaml").read_text())
fields = {role: schema.get(role) for role in ("profile", "event")}
print("generated field schema:", fields)
banned_in_schema = {c for cols in fields.values() if cols for v in cols.values() for c in v} & ds.banned
assert not banned_in_schema, banned_in_schema
print("\n✓ no banned column reached the schema")
"""),

    # ---------------------------------------------------------------- step 5
    md(r"""
## Step 5 — Fit the tokenizer and encode

From here you're on the standard pipeline. At full scale you'd run the stage scripts
(`prepare_data` → `train_tokenizer` → `encode_dataset`); at toy scale we call the same library
code directly so the whole notebook executes in seconds. Bins/categories fit on **train only**
(DL-008) — here we fit on the first 80% of loans to keep the discipline visible.
"""),
    code(r"""
from credit_fm.data.encode import encode_panel
from credit_fm.tokenizer import KVTTokenizer

tok_cfg = {
    "id_col": "loan_id", "time_col": "reporting_date", "time_field": "months_on_book",
    "profile": {"numeric": ["fico", "term_months", "apr"], "categorical": ["vehicle_type"]},
    "event": {"numeric": ["current_balance"], "categorical": []},
    "n_bins": 8, "max_categories": 32, "max_events": 12, "calendar": "yearquarter",
}
train_ids = set(panel["loan_id"].drop_duplicates().iloc[:240])            # 80% of loans = train
tok = KVTTokenizer(tok_cfg).fit(panel[panel["loan_id"].isin(train_ids)])  # fit on TRAIN only
shard = encode_panel(tok, panel)
print(f"vocab {tok.vocab_size} tokens | encoded {len(shard)} loans, "
      f"{int(shard['n_tokens'].sum()):,} tokens")
print("\none loan as the model sees it:")
print(" ", " ".join(tok.tokens(panel[panel['loan_id'] == 'AUTO0000'])[:18]), "...")
"""),

    # ---------------------------------------------------------------- crib sheet
    md(r"""
## The crib sheet — the same 5 steps at full scale

```bash
# 0. (only if your raw source needs derivations) write reference_implementations/<asset>/adapter.py
#    with @register_adapter("<asset>") — see reference_implementations/fannie_mae/adapter.py

# 1+2. write configs/<asset>/dataset.yaml  (+ an ingest recipe if using an adapter)
python scripts/ingest.py -c configs/<asset>/ingest.yaml            # adapter assets only

# 3. audit the panel against the contract
python scripts/validate_dataset.py --dataset configs/<asset>/dataset.yaml --panel <panel.parquet>

# 4. split, then generate the field schema (leakage enforced from dataset.yaml)
python scripts/prepare_data.py    -c configs/<asset>/prepare.yaml
python scripts/classify_schema.py -c configs/<asset>/classify.yaml --out configs/<asset>/tokenizer.yaml

# 5. tokenizer -> shards -> the standard highway
python scripts/train_tokenizer.py -c configs/<asset>/tokenizer_fit.yaml
python scripts/encode_dataset.py  -c configs/<asset>/encode.yaml --split train
python scripts/encode_dataset.py  -c configs/<asset>/encode.yaml --split val
python scripts/pretrain.py        -c configs/<asset>/pretrain.yaml
```

**What you never did:** edit a line of `credit_fm`. That's the point.
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
