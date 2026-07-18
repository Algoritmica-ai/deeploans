# Architecture

An encoder-only (PRAGMA-style) **credit foundation model**: it reads a borrower's full
credit-event history as one token sequence and learns, by masked-language-modelling (MLM)
pretraining, a representation that beats point-in-time tabular baselines (XGBoost) on
downstream credit tasks. This document is the detailed map of how the pieces fit together;
see `tokenization.md` for the token scheme and `decision_log.md` for the rationale behind each
locked choice (DL-NNN references below).

## 1. Pipeline at a glance

```
 raw panel (one row per loan-month)
     │  scripts/prepare_data.py            loan-stratified TEMPORAL split (DL-007)
     ▼
 processed/{train,val,test}.parquet
     │  scripts/train_tokenizer.py         fit KVT tokenizer on TRAIN only (DL-008) → M1
     ▼
 configs/<asset>/tokenizer.json            frozen vocab (552 tokens for the mortgage reference)
     │  scripts/encode_dataset.py          ENCODE-ONCE → token-id shards (DL-014)
     ▼
 encoded/<run>/{train,val,test}/shard-*.parquet  + manifest.json
     │  credit_fm.data.CreditDataModule    dataset → MLMCollator (pad+mask) → DataLoaders
     ▼
 batch {input_ids, attention_mask, labels, event_index, field_type, branch, n_events}
     │  credit_fm.models.CreditFM          HIERARCHICAL three-branch encoder (DL-002/013)
     ▼
 [USR] loan embedding  ──▶  MLM head (pretrain)  /  downstream head (default, prepay, …)
```

Every stage above is **built, tested, and validated end-to-end** — pretraining has run at full
corpus scale and the downstream out-of-time verdict is in (see §8).

## 2. Locked architectural decisions

| | Decision | Why | Ref |
|---|---|---|---|
| 1 | **Encoder-only + MLM** (not decoder/causal) | targets are discriminative; PRAGMA +130% PR-AUC | DL-001 |
| 2 | **Three-branch encoders** (Profile / Event / History) | a dedicated Profile encoder gave PRAGMA +31.8% | DL-002 |
| 3 | **Key-value-time tokenization** | preserves field identity (`LTV=85` ≠ `DPD=85`) | DL-003 |
| 4 | **30M params default** | Chinchilla-honest on ~600M tokens; 50M only with extra public data | DL-004 |
| 5 | **Apache 2.0** | open weights/code/tokenizers | DL-005 |
| 6 | **HuggingFace primary**, NeMo optional | portability | DL-006 |
| 7 | **Hierarchical realization, frozen at M2** | debug architecture at toy scale; M3 scales data only | DL-013 |
| 8 | **Encode-once shards + flat `(B,L)` batches** | dataloader never re-tokenizes; event pooling via `event_index` | DL-014 |

## 3. Tokenizer (KVT) — see `tokenization.md` for full detail

Each loan becomes one sequence of **fused `field=value` tokens** routed to a **branch** and
anchored in time:

```
[BOS] [USR]
  <profile tokens: original_ltv=17, channel=R, property_state=GA, ...>     # static, said ONCE
  [EVT_START] t=<loan_age bin> cal=<YYYYQ#> <event tokens: current_interest_rate=21, ...> [EVT_END]
  ...                                                                       # up to max_events=60 months
[EOS]
```

- **Profile branch** — static origination facts (emitted once).
- **Event branch** — per-month dynamic facts (one block per month).
- **`t=`** discrete `loan_age` bin; **`cal=<YYYYQ#>`** absolute calendar token (the macro-regime
  signal, DL-011); numeric fields use threshold-anchored quantile bins (DL-012).
- Vocab + bin edges fit on **train only** (DL-008). Mortgage reference: **552 tokens**, 100% lossless roundtrip.

## 4. Data layer (M2 Brick 1) — built

The model trains over each loan many times; re-tokenizing every epoch would starve the GPUs. So
the panel is **encoded once** into token-id shards, then read cheaply. Five components:

### 4.1 The shard contract (the frozen interface)
`KVTTokenizer.encode_with_meta()` + `scripts/encode_dataset.py` write one row per loan with four
**aligned** ragged arrays — this is the contract the model and masking both read:

| Field | Meaning | Consumed by |
|---|---|---|
| `input_ids` | fused-token ids | embedding layer |
| `event_index` | month index per token (`-1` = profile/structural; markers included) | **Event encoder pooling** + whole-event masking |
| `field_type` | stable id per field key (incl. `t`, `cal`); `-1` for specials | whole-field-type masking |
| `branch` | `0` profile / `1` event / `-1` structural | **branch routing** in the model |

Plus `n_tokens`, `n_events` for batching. A `manifest.json` records tokenizer version, source,
vocab size, and the shard list.

### 4.2 The five files
| File | Role |
|---|---|
| `training/masking.py` | 3-source MLM masking (below) — pure NumPy, one loan in/out |
| `scripts/encode_dataset.py` | encode-once → shards + manifest (local or `gs://`) |
| `data/dataset.py` `CreditSequenceDataset` | random-access reader → one loan's **unpadded** tensors |
| `data/collators.py` `MLMCollator` | pad batch to max len (**flat `(B,L)`**), mask, build `labels`/`attention_mask` |
| `data/datamodule.py` `CreditDataModule` | train/val/test `DataLoader`s; vocab from manifest |

### 4.3 Flat `(B, L)` layout (DL-014)
A hierarchical model could use a nested `(B, events, tokens)` batch, but we keep a **flat `(B, L)`**
sequence plus `event_index`, and let the Event encoder pool per month using that index. Less
padding (Fannie loans vary a lot in length), and the shard already carries the indices. The
varlen/packed alternative (`PackedCollator`) is deferred to M3 for throughput.

### 4.4 Masking policy
- **train** — shuffled, **dynamic** masking (fresh each batch, RoBERTa-style).
- **val/test** — unshuffled, **deterministic** masking (fixed seed) → comparable loss across epochs.

## 5. Model — hierarchical three-branch encoder

Built and **frozen** at small scale (M2), then scaled — data and compute only, never
architecture — for the full-corpus pretrain (DL-013).

```
 input_ids (B,L) ──embed──┐
                          │ split by branch / pool by event_index
        ┌─────────────────┴───────────────────┐
        ▼                                       ▼
 Profile encoder (~3L)                 Event encoder (~4–5L)   ← attends WITHIN each month's
   static tokens → profile vector        per-event pooling        field=value tokens → per-event vector
        │                                       │
        └──────────────┬────────────────────────┘
                       ▼
            History encoder (~4–6L)   ← attends ACROSS event vectors + profile
                       │
                  [USR] pooling → loan embedding
                       │
        ┌──────────────┴───────────────┐
        ▼                               ▼
   MLM head (→ vocab)            classification head (default / prepay / …)
```

- **Blocks** (`models/base.py`): attention + RoPE, RMSNorm, SwiGLU.
- **Hierarchy benefit**: the History encoder runs over *event vectors* (length = #months ≤ 60),
  not raw tokens (≤ ~1000) — cheaper and more faithful than a flat transformer.
- **Size**: ~30M params (DL-004); context window **1024** (full-corpus loans approach the
  `max_events=60` cap ≈ ~1000 tokens). Vocab is tiny (552) so the embedding table is negligible.
- **`[USR]`** is entity-agnostic: today it pools one loan; fed a multi-product stream later it
  becomes a customer embedding with no architecture change.

## 6. Pretraining objective (MLM)

Hide part of each sequence, predict it. *What* we hide is deliberate — three complementary
strategies, each exercising a different branch:

| Strategy | Rate | Hides | Forces the model to learn |
|---|---|---|---|
| token | 15% | individual field tokens | local field↔value structure |
| event | 10% | a whole month | temporal dynamics (History) |
| type | 10% | a field across all months | cross-field structure (Event) |

Selected positions are corrupted BERT-style (80% `[MASK]` / 10% random / 10% unchanged) so the
model never assumes a slot is literally `[MASK]`. Specials are never masked. `labels` hold the
original id at masked positions and `-100` (ignore) elsewhere.

## 7. Worked example — a real Fannie loan

Loan `103017066080` (Georgia, retail, 30-yr FRM) → **130 tokens**: `[BOS][USR]` + 31 profile
tokens + 6 monthly event blocks (16 tokens each: `[EVT_START]` + `t` + `cal` + 11 numerics + 1
categorical + `[EVT_END]`) + `[EOS]`. The `cal` token ticks `2016Q1 → 2016Q2` at month 3, and at
month 6 `current_actual_upb=0` — the loan pays off. That stable-then-transition trajectory is
exactly what the History encoder reads and what whole-event masking ("predict month 6 from 1–5")
trains.

## 8. Status

| Layer | State |
|---|---|
| Split / baselines (G1, OOT) | ✅ done — validated by `scripts/validate_splits.py` |
| Tokenizer (M1) | ✅ done — 552 tokens (full-corpus fit), calendar + anchored bins |
| Data layer (M2 Brick 1) | ✅ done — encode-once shards, dataset, MLMCollator, datamodule |
| Hierarchical model (M2 Brick 2) | ✅ done — ~26M @ dim384; architecture frozen |
| Training loop | ✅ done — AdamW+cosine, dropout, best-val checkpointing (`train_mlm`, `pretrain.py`) |
| Pretraining at scale (M3/M5) | ✅ done — full 25-year corpus (4% loan sample), parallel encode |
| Embeddings + downstream eval | ✅ done — **OOT verdict: FM 100M full 0.8468/0.0175 beats XGB 0.7913/0.0057** (2022–23 obs → 2023–24 defaults; 26M: 0.8257/0.0113) |
| Multi-GPU DDP · batch scoring · calibration · serving example | ✅ done (v1.1 G4b / #6 / G6.1 / G6.2) |
