# Part 9 — Tokenization: Turning a Spreadsheet into a Language

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ split ─▶ [TOKENIZE] ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ serve


> Files: `src/credit_fm/tokenizer/` (`key_value_time.py`, `numeric_bucketer.py`, `categorical.py`,
> `vocabulary.py`) · fit by `scripts/train_tokenizer.py` · schema proposed by
> `scripts/classify_schema.py` · frozen artifact `configs/mortgage_performance/tokenizer.json` (552 tokens).

## 9.1 Why structured data needs tokenization at all

**Plain English:** a transformer is a machine that reads sequences of symbols from a fixed
alphabet. Text arrives as symbols already. A spreadsheet doesn't — so we must *invent the
alphabet* and rewrite every loan as a sentence in it.

**Why not feed the numbers directly** (the TFT/PatchTST route)? Three reasons this repo chose
symbols (DL-003):

1. **Credit data is mostly categorical and missing-riddled.** `channel=R`, `purpose=C`, dlq codes,
   NA everywhere. Dense numeric encodings force one-hot blowups and imputation fictions; tokens
   treat category, number, and missing uniformly (missing is just the token `field=NA`).
2. **MLM needs a vocabulary.** "Predict the hidden token" requires a finite set of things to
   predict. That's what makes BERT-style pretraining possible on this data.
3. **Thresholds beat smoothness.** Risk isn't smooth in LTV — it cliffs at 80/90/95/97. Bins with
   *anchored* edges represent cliffs natively (a scorecard insight, kept).

## 9.2 Credit tokenizer vs text tokenizer

| | Text (GPT/BERT) | Credit (KVT) |
|---|---|---|
| Alphabet | learned subwords (~30k–100k) | **constructed** `field=value` tokens (**552**) |
| Open/closed | open (any new word decomposes) | closed (every possible token is enumerable from schema × bins) |
| Order means | grammar | *within* a month: nothing (fields are a set); *across* months: time |
| Position | one axis (word position) | three axes: position, **month** (`event_index`), **field** (`field_type`) |
| OOV handling | subword pieces | numeric: clamp to edge bin; categorical: `OTHER`; both fit on train only |

## 9.3 The KVT (Key-Value-Time) design

Every cell becomes one **fused token** `key=value` — "fused" meaning key and value form a single
vocabulary entry (`original_ltv=5` is one token, not two). Why fused? So the model always knows
*which field* a value belongs to — `=7` alone would be ambiguous across fields — and MLM predicts
field-and-value jointly, which is the actually-useful unit. (NVIDIA's transaction blueprint made
the same call; PRAGMA too.)

The full grammar of a loan (from `key_value_time.py`):

```
[BOS] [USR]                                          ← sequence start + the loan-summary slot
  <profile tokens>                                   ← static fields, ONCE (from the first row)
  [EVT_START] t=<age_bin> cal=<YYYYQ#> <event tokens> [EVT_END]    ← month 1
  [EVT_START] t=<age_bin> cal=<YYYYQ#> <event tokens> [EVT_END]    ← month 2
  ...                                                   (last `max_events`=60 months kept)
[EOS]
```

Nine special tokens own ids 0–8: `[PAD] [BOS] [EOS] [MASK] [UNK] [USR] [EVT] [EVT_START] [EVT_END]`.
They're structural — never masked, never predicted.

## 9.4 Numeric encoding: quantile bins with anchors

`NumericBucketer` — the value part of a numeric token is a **bin label**, not the number:

- Fit (train only): take non-zero training values, cut at `n_bins` (default 16) quantiles —
  equal-*population* bins, so resolution concentrates where data lives.
- **Anchors** force extra edges at known cliffs: `anchors: {original_ltv: [80,90,95,97], dti: [36,43,45]}`
  (from `tokenizer.yaml`). Without them, a quantile bin could straddle LTV 80 and blur the
  cliff a scorecard sees natively.
- Labels: `"0"` = exactly zero (zero balance is a *state*, not a small number), `"1".."k"` =
  bins, `"NA"` = missing.
- Transform (forever after): a value beyond the training range **clamps** into the edge bin —
  inference can never mint a new token.

Worked example — Ohio loan's LTV 87 with edges `[…, 80, 85.2, 90, …]` (quantiles ∪ anchors):
87 falls in (85.2, 90] → token `original_ltv=5`. A 2031 loan with LTV 103? Clamps to the top
bin. Information lost, vocabulary stable — the trade is deliberate.

## 9.5 Categorical encoding

`CategoricalTokenizer`: keep up to `max_categories` (256) values seen in training; everything
else → `OTHER`; missing → `NA`. So `channel=R` is a token; a channel code first seen in 2024
becomes `channel=OTHER`. High-cardinality identifiers (zip, MSA) never get here — the contract
excludes them as structural non-features.

## 9.6 Time encoding — the two clocks again, now as tokens

Each event block opens with two time tokens:

- **`t=<bin>`** — loan **age** (the `time_field`, bucketed like any numeric): "this is a
  month-9-of-life event." Relative time → seasoning patterns.
- **`cal=<YYYYQ#>`** — **calendar** quarter (`calendar: yearquarter`): "this happened in 2008Q4."
  Absolute time → macro regimes. This one token is the difference between a model that can and
  cannot learn what a crisis is; it's how the crisis-OOT result (0.782 on 2008–10) is possible.
  Real macro series (HPI, rates), once joined into the panel, are just more event fields.

## 9.7 Sequence encoding: the metadata triple

`encode_with_meta` emits, aligned with `input_ids`:

```
token            input_id   event_index   field_type   branch
[BOS]                1          -1            -1          -1     structural
[USR]                5          -1            -1          -1
original_ltv=5     217          -1             0           0     profile
[EVT_START]          7           0            -1          -1     ┐
t=1                 63           0            11           1     │ month 0
cal=2016Q1         412           0            12           1     │
current_upb=12      88           0             6           1     │
[EVT_END]            8           0            -1          -1     ┘
[EVT_START]          7           1            -1          -1     month 1 …
```

These three arrays are why the model can pool per month, mask per field-type, and route tokens
to branches — the tokenizer and the model share this contract via the shard files.

## 9.8 Fitting, freezing, and the leakage rule

`train_tokenizer.py` fits on the **train split only** (DL-008) and serializes *everything* —
vocab list, every bucketer's edges, every category table — to `tokenizer.json`. From then on the
vocabulary is **frozen** (the reference: 552 tokens over 43 curated fields). Why freezing
matters: ids are the model's embedding-row indices; change the vocab and every checkpoint is
garbage. Why train-only matters: bin edges computed on test data would import the test
distribution into the representation — leakage channel 4.

Where the 43 fields come from: `classify_schema.py` proposes profile/event/numeric/categorical
routing from the train split — after dropping the contract's leakage+exclude lists *first* —
then a human reviews the proposal into `tokenizer.yaml` (bins, anchors, semantic overrides like
"original_ltv is structurally dynamic in the raw data but semantically a profile field").
Machine enforcement + human judgment, in that order.

## 9.9 Common mistakes

- **Refitting the tokenizer "because data grew."** That's a new model lineage, not an update —
  every downstream artifact invalidates. The 10%-corpus experiments deliberately reused the
  frozen vocab.
- **Reading `t=` as calendar or `cal=` as age.** Two clocks (§5.3). Both, always.
- **Expecting the raw number back.** `decode()` returns `original_ltv=5`, a bin. Precision was
  spent to buy a vocabulary; the bin *edges* are in `tokenizer.json` if you need ranges.
- **Forgetting `max_events=60`.** Old months beyond 60 are gone; a 2003 vintage scored in 2024
  is represented by its last 5 years.

### Things to remember

1. Every cell becomes one fused `field=value` token; the whole language is 552 closed, human-readable tokens.
2. Numerics: train-quantile bins with ANCHORED edges at regulatory cliffs; categoricals: capped set + OTHER.
3. Two time tokens per month: `t=` (loan age) and `cal=` (calendar regime — the crisis-learning signal).
4. The metadata triple (event_index / field_type / branch) is the tokenizer↔model contract.
5. Fit on train only; frozen forever — a refit is a new model lineage.

---
*Next: [Part 10 — The Transformer](10_transformer.md): what actually reads these sentences.*
