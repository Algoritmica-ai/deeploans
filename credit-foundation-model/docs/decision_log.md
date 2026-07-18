# Decision Log

Running record of locked decisions. Each entry: decision, rationale, status.

| ID | Decision | Status |
|----|----------|--------|
| DL-001 | Encoder-only + MLM (not decoder/causal) | locked |
| DL-002 | Three-branch encoders (Profile / Event / History) | locked |
| DL-003 | Key-value-time disentangled tokenization | locked |
| DL-004 | 30M default model size (Chinchilla-honest on ~600M tokens) | locked |
| DL-005 | Apache 2.0 | locked |
| DL-006 | HuggingFace primary, NeMo optional | locked |
| DL-007 | Loan-stratified temporal split; derived origination | locked |
| DL-008 | Tokenizer vocab + numeric bins fit on `train` only | locked |
| DL-009 | Training telemetry: pluggable logger, nothing phones home by default | locked (14 Jul) |
| DL-010 | Field selection: drop 11 constant + 16 redundant/derived | locked |
| DL-011 | Per-event calendar/macro-regime token `cal=<YYYYQ#>` | locked |
| DL-012 | Threshold-anchored + per-field numeric bins | locked |
| DL-013 | Hierarchical three-branch realization; architecture frozen at M2 | locked |
| DL-014 | Encode-once shards + flat `(B,L)` batches + 3-source MLM masking | locked |
| DL-015 | Pretraining is data-bound; gate the FM on downstream OOT, not MLM loss | locked |

## DL-007 — Loan-stratified temporal split
**Decision.** Split by `loan_id` (every cutoff of a loan stays in one split), ordered by
**origination date**, 80/10/10, train < val < test in time.

**Why.** Row splitting leaks loans (same loan, ±1 month, in train and test → fake test score).
Origination-ordered splitting mirrors production: a model trained on older loans is tested on
newer ones. Label-horizon leakage is handled at the label-generator layer.

**Artifacts.** `scripts/prepare_data.py` writes `data/processed/{train,val,test}.parquet` +
`splits.csv` + `splits.meta.json` (seed, source SHA-256, counts, ranges, git commit).

## DL-008 — Vocab on train only
Tokenizer vocabulary and numeric bin edges are fit on `train.parquet` only; never on
val/test/full panel (else test distribution leaks into the tokenizer).

## Note — data-module gitignore bug (2026-06-20)
`src/credit_fm/data/` (7 files) was never committed: the old unanchored `.gitignore` rule
`data/` matched `src/credit_fm/data/`. Restored; `.gitignore` is now anchored (`data/*`).

## DL-010 — Field selection (validated against the column glossary)
Final feature set = **42** (29 static → Profile, 13 dynamic → Event). Dropped **11 constant**
columns (no signal) and **16 redundant/derived** columns (deterministic functions of a kept
field, incl. the 7 pre-computed `*_bucket` columns — keep the raw field and let the tokenizer
bucket it).

## DL-011 — Calendar / macro-regime token
Each event block carries an absolute-time token `cal=<YYYYQ#>` (config `calendar: yearquarter`)
alongside the relative `t=<loan_age bin>`. Loan-internal tokens are regime-blind: 2005 and 2008
look identical, yet default behaviour is dominated by the macro cycle. The calendar token lets the
History encoder condition on the era; real macro series (HPI / rate / unemployment) will later
enter as ordinary `event` fields. **Fairness:** give the OOT baseline the same calendar/macro
features, or an FM win isn't apples-to-apples. (Reviewer #2.)

## DL-012 — Threshold-anchored + per-field numeric bins
Numeric fields default to 16 quantile bins, with two overrides: `bins:` raises resolution for
high-signal fields (LTV/CLTV/DTI/FICO/rate → 24), and `anchors:` forces a bin boundary exactly at
regulatory cliffs (LTV 80/90/95/97, DTI 36/43/45) so a hard underwriting threshold is never
blurred inside one bucket. Edges are still fit on `train` only (DL-008). (Reviewer #1.)

## DL-013 — Hierarchical realization; architecture frozen at M2
The three branches (DL-002) are realized **hierarchically**: the Event encoder contextualizes a
month's field tokens into one per-event vector; the History encoder attends across those event
vectors (length = #months, not #tokens) plus the profile vector; `[USR]` pools the loan
embedding. The full architecture is built and debugged at **toy scale in M2, then frozen** — M3
changes only data volume / compute, never architecture. This separates the "does it train?" risk
(M2) from the "does it scale/converge?" risk (M3). Reviewer #3 (downstream class imbalance) and #4
(length-bucketed batching) are deferred to Phases E and D.

## DL-014 — Encode-once shards + flat batches + 3-source masking
**Encode once.** The panel is tokenized a single time into token-id **shards** (one row per loan)
with four aligned arrays — `input_ids`, `event_index`, `field_type`, `branch` — so the DataLoader
never re-tokenizes (it would otherwise dominate GPU time). A `manifest.json` records tokenizer
version + source for reproducibility.

**Flat `(B, L)` batches.** Despite the hierarchy, batches are flat token sequences plus
`event_index`; the Event encoder pools per month from that index rather than using a nested
`(B, events, tokens)` axis. Less padding (loans vary widely in length), and the contract already
carries the indices. A varlen/packed variant (`PackedCollator`) is deferred to M3 for throughput.

**3-source MLM masking.** 15% token / 10% whole-event / 10% whole-field-type, BERT 80/10/10
corruption, specials never masked. Dynamic per batch for train; deterministic (seeded) for
val/test so loss is comparable across epochs. The three strategies each exercise a different
branch.

## DL-015 — Pretraining is data-bound; judge by downstream, not MLM loss
**Finding (28 Jun, M2 diagnostics).** On the `run_2016_2017` slice at **100k loans (~25M tokens)**
the model overfits regardless of size or regularisation: train MLM loss → ~0.1 while **validation
MLM loss bottoms early (~step 150) at ~2.6–2.8, then rises**. Tested 25.5M (dim 384), 25.5M +
dropout 0.1, and 1.4M (dim 128) — all plateau at val ~2.6–2.8. 100k loans is ~20× below the
Chinchilla budget for 25.5M params (~500M tokens ≈ ~2M loans; DL-004), so memorisation is expected.

**Decisions.**
1. **Data scale is the lever** — not model size or dropout. Stop tuning hyperparameters at 100k.
2. **Parallel-encode the full corpus** before the real pretrain (single-stream `encode_dataset.py`
   is the bottleneck — 155k loans took ~57 min); then train 25.5M on ~2M loans.
3. **Best-val checkpointing + early stop** are standard here (overfitting starts by ~step 150 at
   small data); `train_mlm` restores the best-val weights.
4. **MLM val loss is a proxy with an entropy floor** (exact FICO/UPB buckets are inherently
   unpredictable). The FM's gating metric is the **downstream OOT eval vs ROC 0.757** (Phase E),
   not MLM loss.

## DL-009 — Training telemetry (resolved 14 Jul 2026, v1.1 G4c)

**Decision.** Structured metrics go through a pluggable logger (`credit_fm.training.loggers`,
config block `logging:`), and **no backend phones home unless explicitly configured to**:

- default `backend: null` — stdout printing only (pre-G4c behavior, byte-identical);
- `jsonl` — zero-dependency local JSON-lines file (the sovereign-cloud workhorse);
- `tensorboard` — local event files;
- `wandb` — strictly opt-in and **offline by default** (`mode: offline`; sync later with
  `wandb sync` if a hosted/self-hosted instance is ever approved).

**Why.** The sovereign-cloud requirement rules out default hosted telemetry; the framework goal
rules out hard-coding any one vendor. A four-line interface (`log_config` / `log_metrics` /
`finish`) keeps backends swappable, imports lazy, and rank-0-only under DDP.
