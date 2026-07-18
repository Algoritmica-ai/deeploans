# Part 1 — Introduction

## 1.1 What is this project?

**Plain English:** software that teaches a computer to read a borrower's payment history the way
you read a story — beginning, middle, plot twists — and then asks it: *"how does this story end?"*

**Technically:** an open-source (Apache 2.0) framework, the Python package `credit_fm`, for
training **credit foundation models**: encoder-only transformers pretrained with masked-language
modelling on tabular credit *panel data* (one row per loan per month), plus a complete reference
implementation on the public Fannie Mae mortgage dataset (~3.3 billion loan-months, 2000–2024).

It is two things at once:

1. **A framework** — reusable pipeline stages (ingest → split → tokenize → encode → pretrain →
   fine-tune → score → serve), each a config-driven script, each with an artifact validator.
   Bring a new dataset with one YAML file.
2. **A proof** — a validated result on real data: the foundation model beats a strong,
   leakage-free XGBoost baseline out-of-time (0.8468 vs 0.7913 ROC-AUC; 3× the PR-AUC).

## 1.2 Why build a Credit Foundation Model?

### The problem with how credit risk is modelled today

A bank deciding "will this loan default in the next 12 months?" typically builds a **snapshot
model**: take the loan *today* — balance, interest rate, borrower score at origination — one row
of numbers, feed it to logistic regression or XGBoost, get a probability.

The snapshot throws away the movie and keeps one frame. Two loans can look identical today:

```
Loan A today:  balance $190k, rate 4.1%, age 48 months, current
Loan B today:  balance $190k, rate 4.1%, age 48 months, current
```

But their histories differ completely:

```
Loan A history:  paid like clockwork for 48 straight months
Loan B history:  30 days late in month 12, 60 days late in month 29,
                 caught up both times, refinance application denied in month 40
```

Any human underwriter says Loan B is riskier. A snapshot model *cannot see the difference* unless
someone hand-crafts features like "number of past delinquencies" — and hand-crafting never
captures everything (the *pattern* of stumbling before holidays; recovering slower each time;
the interaction with a rising-rate environment).

### The foundation-model idea

Large language models proved something general: **pretrain a big network on enormous amounts of
unlabeled sequence data, and it learns reusable structure you can cheaply adapt to many tasks.**
GPT reads billions of sentences and learns grammar, facts, style. Nobody labeled anything — the
model just learned to fill in blanks.

Loans are also sequences. Every month, every loan emits an "event": balance moved, rate changed,
delinquency status ticked. 25 years × millions of mortgages = billions of events — a corpus the
size of a respectable text dataset. So the bet (validated by the PRAGMA line of research and by
this repo's own experiments) is:

> Pretrain a transformer to *fill in blanks* in loan histories, and it will learn "the grammar of
> credit" — what normal repayment looks like, what trouble looks like brewing, how a 2008 differs
> from a 2019. Then a small amount of labeled data adapts that understanding to any concrete task.

### What problems does it solve?

| Problem | How the FM addresses it |
|---|---|
| Hand-crafted features miss temporal patterns | The model reads the raw monthly sequence itself |
| Every new prediction task = new feature-engineering project | One pretrained backbone; a new task is a small fine-tune (in this repo, literally one YAML line) |
| Labels are scarce (defaults are ~0.65% of loan-months) | Pretraining needs **no labels** — it learns from all 3.3B rows |
| Models trained in calm years break in crises | The calendar token (`cal=2008Q4`) lets it learn *regimes*; the crisis stress-test shows it holds up (0.782 vs XGB 0.757 on 2008–10) |
| Vendors sell this as a black box | Everything here — code, weights, tokenizer, evaluation — is open and auditable |

## 1.3 How is it different from …?

**Logistic Regression** — a weighted sum of features pushed through a squashing function.
Transparent, tiny, the regulator's favorite. But strictly *linear*: it cannot represent "high LTV
is only dangerous when combined with falling house prices" unless you hand it that combination as
a feature. Snapshot-only.

**Credit Scorecards** — logistic regression dressed for the office: features are binned, each bin
gets points, points sum to a score (the FICO idiom). Maximally explainable, minimally expressive.
Everything above about LR applies. Notably, this project *borrows* the binning idea — our
tokenizer buckets numerics into quantile bins with forced boundaries at regulatory cliffs (LTV
80/90/95/97) — but feeds the bins to a transformer instead of a points table.

**Random Forest / XGBoost** — ensembles of decision trees; XGBoost builds them sequentially, each
tree fixing the previous ones' mistakes. On snapshot *tabular* data these are ferociously strong —
which is exactly why this repo's baseline is a carefully tuned, leakage-free XGBoost, not a straw
man. What trees can't do is *read a sequence*: they see one fixed-width row. Our XGBoost bar gets
57 engineered features including summary statistics of history; the FM gets the raw history. The
FM wins by +0.055 ROC and 3× PR-AUC out-of-time.

**Transformers** — not a competitor but the engine. A transformer is a neural architecture built
around *attention* (Part 10). This project **is** a transformer — the difference from the famous
ones is what it reads (loan events, not words) and how it's shaped (three specialized encoder
branches, Part 11).

**GPT / LLMs** — GPT-style models are **decoder-only**: they read left-to-right and predict *the
next* token, which makes them generators (of text). Ours is **encoder-only** (BERT-style, DL-001):
it reads the *whole* sequence bidirectionally and predicts *hidden* tokens, which makes it an
*understander* — ideal when the product is a representation (an embedding you score), not
generated text. Also: LLMs have 10⁹–10¹² parameters and open vocabularies of ~100k word pieces;
ours has 26M–100M parameters and a closed vocabulary of **552** tokens, because the "language of
loans" has a small, known lexicon.

## 1.4 The seven concepts, in plain language

**Foundation Model**
- *Plain:* one big model trained once on everything, reused for many jobs — like hiring a broadly
  experienced analyst instead of training a new intern per task.
- *Why it exists:* labeled data is scarce and expensive; unlabeled data is abundant. Learn general
  structure from the cheap stuff, spend labels only on the last mile.
- *Analogy:* medical school (pretraining: years, general) then residency (fine-tuning: months,
  specialized). You don't redo medical school to switch from cardiology to dermatology.
- *Technical:* a high-capacity network trained with a self-supervised objective on a broad corpus,
  whose learned representations transfer to downstream tasks with modest adaptation.
- *Here:* `CreditFoundationModel`, pretrained by `scripts/pretrain.py` on the label-free token
  shards; adapted by `scripts/finetune.py` to default / prepayment prediction.

**Pretraining**
- *Plain:* the "reading phase" — the model studies millions of loan histories with parts hidden
  and learns to guess the hidden parts. No one tells it what a default is.
- *Why:* forces it to internalize how fields relate (rate↔balance), how months evolve, what
  eras look like — knowledge every downstream task needs.
- *Technical:* self-supervised masked-language modelling (MLM): ~15% of tokens (plus whole months
  and whole field-types, Part 12) are hidden; the model minimizes cross-entropy reconstructing
  them. Loss falls from ~6.5 (random guessing over 552 tokens ≈ ln 552 ≈ 6.3) to ~0.14.
- *Here:* `scripts/pretrain.py` + `credit_fm/training/trainer.py::train_mlm`.

**Fine-tuning**
- *Plain:* the "specialization phase" — take the well-read model and teach it one specific
  question with a comparatively small labeled dataset.
- *Technical:* continue training some or all weights with a supervised loss (here: 2-class
  cross-entropy on "defaulted within 12 months: yes/no"). Three intensities — frozen / LoRA /
  full — traded off in Part 13.
- *Here:* `scripts/finetune.py --mode full` produced the 0.8468 headline.

**Transfer Learning**
- *Plain:* knowledge learned on job A making you better at job B. Pretrain→fine-tune is its
  industrial form.
- *Here, measured:* the same architecture trained *from scratch* on the fine-tune labels alone
  can't come close — the pretrained backbone contributes the sequence understanding that the tiny
  labeled set could never teach.

**Representation Learning**
- *Plain:* instead of humans deciding which numbers describe a loan ("age, balance, #lates…"),
  the model *invents its own description* — a list of numbers that best captures everything
  relevant.
- *Technical:* learning a mapping from raw input to a vector space where task-relevant structure
  is linearly accessible. Quality test: can a trivial classifier on the vectors do well?
- *Here:* the whole point of the three-branch encoder; the "frozen" fine-tune mode is exactly
  that quality test.

**Embeddings**
- *Plain:* the model's numeric summary of a thing. A loan becomes a list of 384 (or 768) numbers;
  loans with similar risk stories end up with similar lists.
- *Analogy:* GPS coordinates for meaning — "similar" becomes "nearby," so downstream math works.
- *Technical:* dense vectors. Two kinds here: **token embeddings** (a learned row per vocabulary
  entry, `models/base.py::Embeddings`) and the **loan embedding** — the output of the `[USR]`/
  `[LOAN]` slot after the History encoder, `(B, dim)`, the single vector all downstream heads
  consume.
- *Here:* `model.extract_embeddings(batch)`; cached to parquet by `scripts/extract_embeddings.py`.

**Hidden Features**
- *Plain:* the useful signals inside the embedding that nobody programmed — e.g. some direction
  in the 384-dim space may act like "borrower under stress," discovered, not designed.
- *Technical:* the coordinates of intermediate activations ("hidden states"). Individually they
  are rarely interpretable; *collectively* they are the representation. You verify they exist by
  probing (fit a linear model on frozen embeddings → 0.7309 ROC out-of-time, already near the
  XGBoost bar with zero feature engineering).

## 1.5 The receipts (why you should believe any of this)

All numbers are **calendar out-of-time**: fine-tune on observations from Dec-2016…Dec-2021, test
on Dec-2022/Dec-2023 snapshots whose 12-month outcome windows (2023–24) the model never saw.

| Model | Data | OOT ROC-AUC | OOT PR-AUC |
|---|---|--:|--:|
| XGBoost bar (57 leakage-free features) | 4% sample | 0.7913 | 0.0057 |
| FM 26M, frozen probe | 4% | 0.7309 | — |
| FM 26M, LoRA | 4% | 0.8068 | — |
| FM 26M, full fine-tune | 4% | 0.8257 | 0.0113 |
| FM 65M, full (params only ↑) | 4% | 0.8223 (flat) | — |
| FM 26M, full (data only ↑) | 10% | 0.8406 | 0.0145 |
| **FM 100M, full (both ↑)** | **10%** | **0.8468** | **0.0175** |

Two lessons the ladder teaches: fine-tuning intensity matters (0.73 → 0.81 → 0.83), and **data
must grow with the model** (65M on the same data was flat; DL-015). Part 18 covers how these
experiments were run and recorded.

### Things to remember

1. A foundation model = pretrain once on unlabeled sequences, cheaply fine-tune per task.
2. vs XGBoost: the FM reads the whole movie (the monthly sequence), not one frame (a snapshot row).
3. Encoder-only ≠ GPT: this model *understands* loans (bidirectional, fills blanks); it never generates.
4. The receipts are out-of-time: FM 0.8468 / 0.0175 vs XGBoost 0.7913 / 0.0057 — and 65M-without-more-data was flat.
5. An embedding is the model's learned numeric summary; the `[USR]` vector is 'the loan as 768 numbers.'

---
*Next: [Part 2 — Big Picture Architecture](02_big_picture.md), where the pipeline becomes a map.*
