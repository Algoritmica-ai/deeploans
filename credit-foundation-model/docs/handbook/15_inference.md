# Part 15 — Inference: From Checkpoint to a Number a Bank Can Use

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ split ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ [SERVE]


> Files: `src/credit_fm/inference/scoring.py`, `calibration.py` · scripts
> `extract_embeddings.py`, `score_portfolio.py`, `calibrate.py` ·
> `reference_implementations/mortgage_performance/serve.py`.

## 15.1 Checkpoints and `.pt` files

A `.pt` file is a **zip of tensors plus Python objects**, written by `torch.save`. This repo's
checkpoints are dicts with a deliberate shape:

```python
# pretraining checkpoint (pretrain.py)              # fine-tuned checkpoint (finetune.py --save)
{ "model":  state_dict,                             { "model":  state_dict,
  "config": {vocab_size, dim, n_heads, layers…},      "config": {…same…},
  "run_config": <full resolved YAML>,  # lineage       "finetune": {mode, lora{rank,alpha},
  "tokenizer": path, "history": …}                                  task{label,gate…}, metrics…} }
```

A **state_dict** maps parameter names → tensors (`"embeddings.token.weight": (552,768)`, …).
Loading is always the same dance (`scoring.py::load_finetuned`): rebuild the architecture **from
the stored config** (never from your assumptions), re-insert LoRA adapters first if
`finetune.mode == "lora"` (otherwise the adapter keys have nowhere to land), then
`load_state_dict`, then **`model.eval()`** — the call beginners forget; it switches dropout off
(in train mode your "deterministic" scores would wobble). Practical notes: `map_location="cpu"`
so a GPU-trained file loads anywhere; `gs://` paths stream through fsspec; you can read *just*
the metadata of any checkpoint without a model: `torch.load(...)["finetune"]`.

## 15.2 The scoring pipeline (`score_panel`)

```
panel rows ─▶ observe_panel(cutoff, gate)     ← THE leakage guard: history ≤ cutoff;
                    │                            only loans performing at the cutoff
                    ▼
          encode_panel_parallel(frozen tokenizer)   ← same 552-token vocab as training
                    ▼
          MLMCollator(mask=False)             ← inference batches: nothing hidden
                    ▼
          model.classify(batch) → softmax[:,1] = raw score per loan
```

Output: one row per gated loan — `loan_id, score, n_events, cutoff` — plus a manifest sidecar
(lineage + score summary) that `validate_scores.py` audits. The property that makes this
trustworthy is *tested*: two panels identical up to the cutoff but with wildly different futures
produce **byte-identical** scores.

**Generating embeddings instead of scores:** `model.extract_embeddings(batch)` returns the
`(B, dim)` `[USR]` vectors; `extract_embeddings.py` caches them to parquet keyed by loan —
feedstock for the frozen probe, XGBoost-on-embeddings comparisons, or any downstream system that
wants "the loan as a vector."

## 15.3 Calibration: fixing the level (v1.1 G6.1)

Raw scores rank correctly but sit ~50× above true PDs (rebalanced training). The fix is a
monotone map fitted where outcomes are known:

```bash
# 1. score a PAST, non-test cutoff            2. fit the map                3. score for real
score_portfolio --cutoff 2021-12-31 …    →    calibrate.py -c calibrate.yaml →  score_portfolio --calibrator calibrator.json
```

`calibrate.py` joins realized 12-month outcomes to the scores, fits **isotonic** regression
(default: a non-parametric monotone step function — assumes only "higher score ⇒ higher risk")
or **Platt** (a 2-parameter sigmoid, for tiny calibration sets), and writes a plain-JSON
calibrator with its own lineage and before/after Brier. Two properties are load-bearing:

- **Monotone ⇒ rankings untouched** — ROC/recall@K on `pd` equal those on `score`, provably.
- **The embargo guard** — the script *refuses* to fit on any cutoff ≥ the protocol's test
  cutoffs. Calibrating on the window you report metrics on flatters Brier the way peeking
  flatters ROC; the refusal is negative-control tested.

Applied, the scores file gains a `pd` column (`raw 0.31 → pd 0.0042` for the Ohio loan) and
`validate_scores` check I gates its honesty (mean pd within 2× of realized; Brier + reliability
table printed).

## 15.4 Threshold selection

The pipeline deliberately ships **scores, not decisions** — a threshold encodes a business
tradeoff (cost of a missed default vs cost of a review), not a modelling truth. The tools for
choosing one: the recall@K/lift table ("with budget to review 1% of the book, you catch X% of
defaults at lift L"), and — once calibrated — expected-cost arithmetic directly on PDs
(flag when `pd × LGD × exposure > review_cost`). Set thresholds downstream, on calibrated
numbers, per portfolio; never bake one into the model.

## 15.5 Deployment shapes

**Batch** (the primary production pattern in credit): a scheduled `score_portfolio.py` run per
cutoff writes scores + manifest; `validate_scores --labeled-panel … --min-roc 0.8` on a past
cutoff doubles as a **drift monitor** — if the certified quality stops reproducing on fresh
months, the gate fails loudly.

**Online** (`serve.py` — explicitly an example: no auth/TLS/scaling): FastAPI app that loads
checkpoint + tokenizer + calibrator **once** at startup, then

```
POST /score  {"cutoff": "2023-12-31", "loans": [<panel rows>]}
        →    {"n_scored": 1, "calibrated": true,
              "scores": [{"loan_id": "…", "score": 0.31, "pd": 0.0042, "rank": 1}]}
GET  /health →  model + calibrator identity (what exactly is serving?)
```

Its core promise is tested: **an HTTP score equals the batch score exactly** — same
`observe_panel`, same encoder, same calibrator, one shared code path (`credit_fm.inference`).
When batch and online disagree in production systems, it's invariably because someone
re-implemented preprocessing; this repo structurally can't.

**Packaging a release:** `publish_model.py` bundles checkpoint + tokenizer + card material into
a distributable layout (HF-weights publication itself is deferred).

### Things to remember

1. Always rebuild the model from `ckpt["config"]`, re-insert LoRA first, then `eval()`.
2. `observe_panel` = truncation + gate; a test proves post-cutoff rows cannot change any score.
3. Calibrate on a held-out PAST cutoff — the script refuses test windows; monotone maps keep rankings intact.
4. Thresholds are business decisions made downstream on calibrated PDs — never baked into the model.
5. HTTP score == batch score by construction (one shared inference path, proven by test).

---
*Next: [Part 16 — Configurations](16_configurations.md): the YAML engine that drives every stage.*
