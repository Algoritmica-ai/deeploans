# Tokenization

Key-value-time (KVT) scheme. Each loan becomes one sequence of **fused `field=value` tokens**
(TabBERT / transaction-FM lineage — one token per field/value pair, *not* separate key + value
tokens), routed to a **branch** and anchored in time. Fit on `train` only (DL-008).

## Sequence layout

```
[BOS] [USR]
  <profile tokens: original_ltv=4, channel=R, ...>          # emitted once, from the loan's first row
  [EVT_START] t=<loan_age bin> cal=<YYYYQ#> <event tokens: current_interest_rate=7, ...> [EVT_END]
  [EVT_START] t=<loan_age bin> cal=<YYYYQ#> ...                                          [EVT_END]
  ...                                                        # most-recent max_events months (default 60)
[EOS]
```

- **Profile branch** — static origination facts (LTV, channel, FICO, …), emitted once per loan.
- **Event branch** — per-month dynamic facts (current rate, UPB, current FICO, …), one block per row.
- **Specials** (ids 0–8): `[PAD] [BOS] [EOS] [MASK] [UNK] [USR] [EVT] [EVT_START] [EVT_END]`.
  The `[USR]` slot pools the loan-level embedding.

## Token types

- **Numeric** → quantile buckets fit on `train` (default 16 bins; per-field overrides via the
  `bins:` map). Reserved buckets: `=0` (exact zero) and `=NA` (missing); out-of-range values at
  inference clamp to the top/bottom bucket, never create a new token.
  - **Anchored cut-points** (`anchors:` map) force a bin boundary exactly at regulatory cliffs
    (LTV 80/90/95/97, DTI 36/43/45) so `79.9` vs `80.1` is never blurred into one bucket.
- **Categorical** → one token per category; unseen → `=UNK`, missing → `=NA`; capped by
  `max_categories` + `min_count`.
- **Time coordinate** `t=<bin>` — discrete `loan_age` bucket at the head of each event block.
- **Calendar / macro-regime** `cal=<YYYYQ#>` — absolute reporting-quarter token per event, so the
  History encoder can tell **2005 from 2008** (the macro signal pure loan-internal tokens lack).
  Set by `calendar: yearquarter|year|none`. Real macro series (HPI / prevailing rate /
  unemployment), once joined into the panel, are just additional `event` fields — apply the same
  features to the OOT baseline so any FM win stays apples-to-apples.

## Vocabulary fitting (leakage rule)

Vocabulary and all numeric bin edges are fit on **`train` only** (DL-008) — fitting on
val/test/full leaks distribution into the tokenizer. Build the split first
(`scripts/prepare_data.py`), then `scripts/train_tokenizer.py` writes `configs/<asset>/tokenizer.json`
(config + bin edges + categories + calendar + vocab) plus a QA report (roundtrip %, OOV %, length).

**Mortgage reference corpus (mortgage performance data), fit on the full 2000–2022 train split:** **552 tokens**
— 9 special + 431 `field=value` + 18 `t=` + 94 `cal=` (one per year-quarter across 25 years).
100% lossless roundtrip, 0% OOV. On the full multi-year corpus loans hit the `max_events=60` cap
(~1000 tokens) → size the model context at 1024. (An earlier 440-token vocab fit on the
2016–2017 dev slice was superseded by this full-corpus fit.)

## Field classification & config generation

Per-asset field roles live in `configs/<asset>/tokenizer.yaml`. `scripts/classify_schema.py`
derives the classification **reproducibly from the data** — for the Dutch panel the file is
fully generated; for the mortgage reference it is **curated on top of the classifier's
suggestions** (leakage-list exclusion, ARM/IO drops for a fixed-rate book, regulatory bin
anchors — see the honesty note in `notebooks/02_schema_classification.ipynb`). For each column
the classifier determines:

- **role** — `id` / `static` (constant within a loan → Profile branch) / `dynamic` (varies per
  cutoff → Event branch).
- **type** — `numeric` (→ buckets), `categorical` / `bucket` / `flag` (→ single token),
  `temporal`, or `constant`.

It then drops two groups (`find_redundant`): `drop_constant` (single value across the panel) and
`drop_redundant` (exact-duplicate columns and `*_bucket` discretizations of a kept numeric field,
plus opted-in functional-dependency candidates).

**Dutch mortgages (validation):** 71 columns → **42 features** (29 static, 13 dynamic); 11 constant
+ 15 redundant dropped (validated against the ESMA column glossary, 70/71 match — see DL-010).
