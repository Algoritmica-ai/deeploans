# Part 21 — Engineering Notes: "Why Didn't You Just Use…?"

> Architectural reasoning, made explicit. Every alternative below was considered (several were
> measured). Knowing *why they lost* teaches more than knowing what won. Decision records:
> DL-001 (encoder-only), DL-002 (three-branch), DL-003 (KVT), plus the E8–E12 ladder.

## Why not just XGBoost?

**We did.** It's the permanent baseline (`build_oot_baseline.py`), tuned honestly with 57
leakage-free features — and any FM number is only ever quoted next to it. XGBoost's limit is
structural, not effort: it consumes one fixed-width row, so history must be pre-digested into
hand-made summary features, and whatever pattern you didn't think to encode does not exist for
it. The FM reads the raw sequence. Verdict from the same protocol: 0.7913 vs 0.8468 ROC, 3.1×
PR-AUC. *Keep using XGBoost* when data is truly snapshot-shaped, labels are plentiful, and you
need training in minutes on a CPU.

## Why not an LSTM / RNN?

The pre-transformer answer to sequences (reads left to right, carrying a memory). Three
disqualifiers here: (1) **long-range decay** — signal from month 3 must survive 57 memory
updates to matter in month 60; attention reads month 3 directly. (2) **No parallel training** —
step t waits for t−1, so GPUs idle; transformers process all 950 tokens at once. (3) **No
pretraining story** — MLM-style objectives fit bidirectional encoders naturally. LSTMs remain
fine for small, short-sequence problems on modest hardware — none of which describes 3.3B
loan-months.

## Why not literally BERT (from HuggingFace)?

We *are* BERT-shaped (encoder + MLM) — that lineage is deliberate. But the pretrained artifact
`bert-base` is useless here: its WordPiece vocabulary and weights encode *English*, not loans;
our alphabet is 552 constructed tokens, so 30k-vocab machinery is dead weight; and loans need
three position axes (sequence, month, field) plus branch structure that flat BERT lacks. We
kept the *idea* (bidirectional encoder, masked reconstruction, [CLS]→[USR] summary slot) and
rebuilt the body to fit the data (three branches, RoPE, RMSNorm, SwiGLU — the modern stack).

## Why not GPT-style (decoder-only)?

Decoders read left-to-right and are built to *generate*. Two mismatches: (1) our product is a
**representation** — when embedding a loan you already have its whole visible history, and
letting every token see both directions is strictly more information than causal masking
allows; (2) we never generate anything — synthesizing plausible loan histories is a
different (and compliance-fraught) product. The cutoff discipline people associate with
causality lives in the *data layer* (`observe_panel` truncation), not the attention mask.
(DL-001.)

## Why not TFT / PatchTST / a forecasting model?

Temporal Fusion Transformer and PatchTST treat time series as **dense numeric channels** —
great for electricity demand. Credit panels are mostly **categorical, missing-riddled, and
event-like** (channel codes, purpose flags, NA-heavy servicing fields). Forcing them into
numeric channels means one-hot blowups and imputation fictions; and neither architecture comes
with a natural fill-in-the-blanks pretraining objective over categorical structure. Our KVT
tokens handle category/number/missing uniformly and make MLM possible. (The same argument
applies to any framework reaching for an LSTM/TFT/PatchTST zoo on credit panels: the data
type is fighting the architecture.)

## Why not TabNet (or other deep-tabular nets)?

TabNet et al. are *row* models — learned feature selection within one snapshot, competing with
XGBoost on tabular benchmarks (and often losing). They don't address the sequence axis at all,
so they'd inherit the snapshot ceiling while giving up XGBoost's robustness. If deep-tabular
ever beats trees convincingly on snapshots, it would replace our *baseline*, not our model.

## Why not fine-tune an actual LLM on loans-as-text?

Tempting ("serialize the history into a prompt, ask GPT"). Four reasons no: **token economics**
— our 950-token loan becomes ~8–10k text tokens; at 3.3B loan-months pretraining-scale exposure
is unaffordable. **Vocabulary waste** — 100k-token embeddings to represent a 552-symbol
language. **Determinism & audit** — regulated scoring needs bit-reproducible, explainable
pipelines, not sampled text. **Data sovereignty** — banks can't ship loan tapes to an API.
(Part 23 does the full side-by-side.)

## Why not one flat transformer (no branches)?

It works — it's the ablation baseline in spirit — but wastes capacity rediscovering structure
we know: static facts vs monthly facts vs timeline. The hierarchy also buys the compute win
(history attention over 62 positions, not 950) and the *per-month bottleneck* that makes event
vectors meaningful objects (Part 11.1). PRAGMA reported the same conclusion; DL-002 froze it.

## Why not a bigger model? (asked constantly)

Measured, not opined: 65M on unchanged data was **flat** (0.8223 vs 0.8257 — E10). Parameters
only pay when data grows with them (100M + 10% corpus → 0.8468 — E11), and the attribution
split showed ~71% of that gain came from the data axis alone (E12). "Feed the model before you
grow it" (DL-015) is this repo's own miniature scaling law — and the reason a 400M model isn't
next until the 100% corpus is.

## The meta-lesson

Every "why not X" above resolves to one of three questions: *does it read sequences?* (kills
XGBoost/TabNet for the main model), *does it fit categorical, missing-riddled events?* (kills
dense forecasting nets), *does it support label-free pretraining at our scale?* (kills LSTM
practically, decoder-only philosophically, rented LLMs economically). When someone proposes the
next architecture, ask the same three questions before benchmarking anything.

### Things to remember

1. XGBoost isn't a rejected alternative — it's the permanent, honest bar.
2. Encoder-only won because the product is a representation, not generated text (DL-001).
3. Tokens won over dense channels because credit data is categorical + missing + event-like.
4. Bigger models without more data measurably do nothing (E10) — scale both or neither.
5. Three screening questions beat any benchmark: sequences? categorical events? pretrainable at scale?

---
*Next: [Part 22 — Debugging the Pipeline](22_debugging_the_pipeline.md).*
