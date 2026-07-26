# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generate ``notebooks/04_encode.ipynb`` — the encode-once stage walkthrough.

Kept as a builder (not a hand-written .ipynb) so the notebook is regenerated deterministically and
reviewed as plain Python. Run from anywhere::

    python notebooks/build_encode.py

The notebook explains ``scripts/encode_dataset.py`` (stage 5) — how a split panel becomes token-id
**shards** the GPU streams (the four aligned ragged columns + ``n_tokens``/``n_events`` + manifest),
the sharding rule, the three encode engines, and how shards later become padded ``(B, L)`` batches.
It runs a small **live** encode of a synthetic panel — all from committed configs, no GCS.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "notebooks" / "04_encode.ipynb"


def md(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_markdown_cell(text.strip("\n"))


def code(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_code_cell(text.strip("\n"))


CELLS = [
    md(r"""
# 04 · Encode — turning the panel into token-id shards the GPU streams

Stage 5 of the pipeline (`scripts/encode_dataset.py`). The tokenizer (notebook `03`) can turn *one*
loan into tokens. This stage does it for **every loan, once**, and writes the result to disk as
**shards** — so training never re-tokenizes.

**Why "encode once"?** The model sees each loan hundreds of times during training (many epochs). If
we tokenized on every pass, the CPU would starve the GPUs — they'd sit idle waiting for tokens. So
we pay the tokenization cost **a single time** here, freeze the integer ids to parquet, and then
training just streams those ids straight to the GPU. It's the classic "prepare the ingredients once,
cook many times."

**Contents**
1. What this stage produces
2. What one shard row looks like &nbsp;·&nbsp; *live demo*
3. The sharding rule — loans kept whole &nbsp;·&nbsp; *live demo*
4. The three engines (cpu / vector / gpu)
5. From shards → padded `(B, L)` batches (what training consumes)
6. The manifest — lineage &amp; the resume signal
7. **How to run it** (both splits, all knobs, resume, validate)
8. Notes &amp; caveats
"""),

    # ---------------------------------------------------------------- setup
    md("## Setup — committed configs only (no GCS, no data)"),
    code(r"""
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
ENCODE = yaml.safe_load((CFG / "encode.yaml").read_text())
print("encode recipe:")
print("  input      :", ENCODE["input"])       # ${paths.processed}/${split}.parquet
print("  output     :", ENCODE["output"])      # ${paths.encoded}/${split}
print("  shard_size :", ENCODE["shard_size"], "loans/shard")
print("  workers    :", ENCODE["workers"], "| engine:", ENCODE["engine"])
"""),

    # ---------------------------------------------------------------- what it produces
    md(r"""
## 1. What this stage produces

For a split (e.g. `train`), `encode_dataset.py` reads `train.parquet` (the per-loan monthly panel
from stage 2) and writes, under `.../encoded/<run>/train/`:

| Output | What it is |
|---|---|
| `shard-00000.parquet`, `shard-00001.parquet`, … | the encoded loans, ~`shard_size` (50,000) loans per file |
| `manifest.json` | the index — resolved config, total loan/token counts, vocab size, and the shard list |

Each **row of a shard is one loan**, carrying six columns — the exact contract the model and the
MLM masking read:

| Column | Type | Meaning |
|---|---|---|
| `input_ids` | ragged int list | the token ids (the loan's "sentence"; see notebook `03`) |
| `event_index` | ragged int list | which month each token belongs to (`-1` = profile/special) |
| `field_type` | ragged int list | which field each token is (for type-level masking) |
| `branch` | ragged int list | `0`=profile, `1`=event, `-1`=special — routes tokens to encoders |
| `n_tokens` | int | sequence length (for batching / bucketing) |
| `n_events` | int | number of monthly blocks |

"Ragged" just means each loan has a different length — a loan with 3 months is shorter than one with
60. Padding to a common length happens later, per batch (section 5), not on disk.
"""),

    # ---------------------------------------------------------------- shard row live
    md(r"""
## 2. What one shard row looks like — live

Let's encode a tiny synthetic panel (5 loans × 3 months) with the **real frozen tokenizer** and look
at one row. This is exactly what `encode_panel` writes, just at toy scale.
"""),
    code(r"""
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.data.encode import encode_panel

tok = KVTTokenizer.load(str(CFG / "tokenizer.json"))

rows = []
for lid in range(5):
    for m in range(3):                       # 3 monthly rows per loan
        rows.append({"loan_id": f"L{lid}", "reporting_date": f"2006-0{m+1}-28", "loan_age": m + 1,
                     "origination_date": "2005-12-31", "original_ltv": 80, "channel": "R",
                     "current_interest_rate": 6.5, "current_actual_upb": 200000 - m * 500})
panel = pd.DataFrame(rows)

shard = encode_panel(tok, panel)             # one row per loan
print("shard columns:", list(shard.columns))
print(f"shard shape  : {shard.shape[0]} loans\n")

r = shard.iloc[0]
print(f"loan {r['loan_id']}:  n_tokens={r['n_tokens']}  n_events={r['n_events']}")
print("  input_ids  :", r["input_ids"][:14], "...")
print("  branch     :", r["branch"][:14], "...   (0=profile, 1=event, -1=special)")
print("  event_index:", r["event_index"][:14], "...   (-1 until an event block starts)")
print("  field_type :", r["field_type"][:14], "...   (which field; -1 for [BOS]/[USR]/etc.)")
"""),
    md(r"""
The four lists are **the same length and aligned** — position *i* in `input_ids`, `branch`,
`event_index`, `field_type` all describe the same token. That alignment is the whole contract: the
model uses `branch` to send each token to the right encoder, `event_index` to group a month's
tokens, and `field_type` for masking — all without re-parsing strings.
"""),

    # ---------------------------------------------------------------- sharding rule
    md(r"""
## 3. The sharding rule — loans are kept whole

Why shards at all? A single parquet with millions of loans is unwieldy to write in parallel and to
stream. So we cut the panel into files of ~`shard_size` loans. **The one hard rule: a loan is never
split across shards** — all of a loan's tokens live in one row, in one file. Loans are assigned to
shards in first-seen order.
"""),
    code(r"""
from credit_fm.data.encode import iter_shards

# 5 loans, shard_size=2  ->  shards of 2, 2, 1 (loans kept whole)
sizes = [len(s) for s in iter_shards(tok, panel, shard_size=2)]
print("loans per shard with shard_size=2:", sizes, "  (sums to 5 loans, none split)")
"""),
    md(r"""
`shard_size` is a throughput/memory knob, **not** a modelling choice — it changes how the work is
chopped up, never the tokens. 50,000 loans/shard is a good default (each shard is a few hundred MB).
Smaller shards = more parallelism + finer resume granularity; larger = fewer files.
"""),

    # ---------------------------------------------------------------- engines
    md(r"""
## 4. The three engines

Per-loan tokenization is CPU-bound, so encoding millions of loans needs parallelism. The `engine:`
key picks how:

| Engine | How it works | When to use |
|---|---|---|
| **`cpu`** (default) | a **process pool** of `workers` — each worker loads the tokenizer once and encodes whole shards | the proven full-corpus path; scales with cores (we used `workers=64`) |
| **`vector`** | single-threaded **vectorized NumPy** encode of the whole panel | smaller panels / the fine-tune observation encode; **token-identical** to cpu |
| **`gpu`** | RAPIDS cuDF/CuPy | **parked** — it broke the pinned numpy/pandas once; the vector path is token-identical and already ~10× cpu |

**A real gotcha, baked into the code:** the cpu pool uses `spawn`, **not** `fork`. The parent
process already opened a gRPC/gcsfs connection (to read the panel from GCS), and forking *after*
gRPC init deadlocks the workers when they write shards back. `spawn` gives each worker a clean
process that builds its own connection. (You saw the harmless `skipping fork() handlers` gRPC lines
in the run log — that's this, working as intended.)
"""),

    # ---------------------------------------------------------------- shards to batches
    md(r"""
## 5. From shards → padded `(B, L)` batches (what training actually consumes)

Shards store loans **unpadded** (each its own length). The `CreditDataModule` + `MLMCollator` turn a
list of loans into one rectangular batch at load time:

1. **Mask** (MLM): hide some tokens and ask the model to predict them. **Train** masking is *dynamic*
   — fresh random masks every batch (RoBERTa-style), so the model sees the same loan masked
   differently each epoch. **Val/test** masking is *fixed* (a seed), so the loss is comparable across
   epochs.
2. **Pad** every loan up to the batch's longest sequence — `input_ids` with `[PAD]` (0), the
   metadata (`branch`/`event_index`/`field_type`) with `-1`, and build an `attention_mask`
   (1 = real token, 0 = padding) so the model ignores the padding.

The batch is a **flat `(B, L)`** — batch × length — not a nested `(B, events, tokens)`, because the
shard already carries `event_index`/`branch`, so the model can reconstruct the hierarchy without a
padded event axis (less wasted padding). Two policy differences worth remembering:

- **train** loader: shuffled + dynamic masking.
- **val/test** loader: unshuffled + deterministic masking.

So: *encode once (here) → stream + mask + pad many (every training step)*.
"""),

    # ---------------------------------------------------------------- manifest
    md(r"""
## 6. The manifest — lineage &amp; the resume signal

Alongside the shards, `manifest.json` records everything needed to trust and reuse the output:
the tokenizer + `vocab_size`, the source panel, total `n_loans` / `n_tokens`, the shard list, and
the full resolved config (lineage). Two practical uses:

- **`vocab_size`** — the DataModule reads it from here, so training never has to be told the vocab
  size separately.
- **resume** — the orchestration script checks for `train/manifest.json` + `val/manifest.json` and
  **skips the whole encode** if they exist. That's why the scaling re-run jumps straight past the
  ~2–3 h encode once it's done (encode-once, literally).
"""),

    # ---------------------------------------------------------------- HOW TO RUN
    md(r"""
## 7. How to run it

Encoding is **per split** — run it once for `train` and once for `val` (test is encoded only when
you need it). Point the recipe at the split with `--split`; everything else has a sane default.

### The two commands you actually need
```bash
# train split (the big one) — cpu engine, 64 worker processes
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml \
    --run_name run_2000_2022_10pct --split train --workers 64

# val split
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml \
    --run_name run_2000_2022_10pct --split val   --workers 64
```
`--run_name` selects which split directory to read and where to write (it fills the `${paths.*}`
in the recipe). Omit it to use the config's default run.

### Knobs (override any recipe key on the CLI)
```bash
# fewer/more workers (match your core count); smaller shards for finer resume
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --split train \
    --workers 32 --shard_size 25000

# use the vectorized NumPy engine (token-identical; good for smaller panels)
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --split val --engine vector

# a quick smoke test on a tiny slice before the full run
python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --split val \
    --workers 4 --shard_size 5000
```

### Where it fits in the pipeline
```
ingest → prepare_data (split) → train_tokenizer → [ ENCODE ] → pretrain
```
It needs stage 2's `${paths.processed}/<split>.parquet` and stage 4's frozen `tokenizer.json`.

### Resuming / re-running
The encode is **not** checkpointed mid-shard, but it **is** idempotent at the stage level: the
orchestration script skips it when `train/manifest.json` **and** `val/manifest.json` already exist.
To force a fresh encode, delete the output directory (or point `--run_name` somewhere new). Because
shard names are deterministic (`shard-<id>.parquet`), re-running overwrites cleanly.
"""),
    code(r"""
# print the exact commands for THIS run's paths, resolved from the recipe
run = "run_2000_2022_10pct"
for split in ("train", "val"):
    print(f"python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml "
          f"--run_name {run} --split {split} --workers 64")
"""),

    # ---------------------------------------------------------------- caveats
    md(r"""
## 8. Notes &amp; caveats

* **Encode uses the frozen `tokenizer.json` — never re-fits.** All the leakage discipline lives in
  stage 4; encode just applies the frozen ids. Change the tokenizer and every shard is stale.
* **Loans are kept whole; row order is not preserved** across the cpu worker pool (shards come back
  as they finish). That's fine — the model shuffles anyway, and shard *names* are deterministic.
* **`workers` is a throughput knob, `shard_size` a file-size knob — neither changes the tokens.**
  The three engines are token-identical (that's tested); pick on speed/stability, not results.
* **Sequence length drives GPU memory.** `n_tokens` per loan (capped by the tokenizer's
  `max_events=60`) sets how long batches are — which is exactly what forced the micro-batch +
  gradient-accumulation choice in pretraining. The encode log prints tokens/shard so you can see it.
* **Two-layer validation:** logic is covered by `tests/test_encode*`/`test_datamodule.py`; the
  artifact-level check is the manifest counts (loans in == loans encoded) printed at the end of the
  run. **Next:** notebook `05` (pretrain) streams these shards to train the model.
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
