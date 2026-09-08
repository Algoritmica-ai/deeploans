---
license: apache-2.0
library_name: credit_fm
pipeline_tag: feature-extraction
tags:
  - credit-risk
  - tabular
  - panel-data
  - foundation-model
  - masked-language-modeling
  - finance
---

# Credit Foundation Model — 100M pretrained backbone

*by [Algoritmica](https://github.com/Algoritmica-ai) · Apache-2.0 · card v1, 12 Aug 2026*

A **sequence foundation model for credit risk**: an encoder-only transformer pretrained with a
masked-token objective over the full month-by-month repayment history of mortgage loans. It reads a
loan's history the way a language model reads a sentence, and emits a single vector summarising the
borrower's trajectory.

> **This repository contains the pretrained backbone only.** It has **no task head** and predicts
> nothing out of the box. It produces per-loan embeddings; you fine-tune it (or probe it) for a task.

## What is in this repository

| File | Purpose |
|---|---|
| `model.safetensors` | Weights — **100,850,474 parameters**, dim 768 |
| `config.json` | Architecture + the reproducible training recipe |
| `tokenizer.json` | Frozen 552-token key-value-time vocabulary — **required** to encode inputs |
| `load_example.py` | Minimal snippet: rebuild the model, load weights, embed a loan |
| `LICENSE` | Apache-2.0 |

## Model details

- **Architecture:** three-branch encoder — Profile (3 layers, static origination fields) + Event
  (5 layers, per-month dynamics) + History (6 layers, fuses the sequence) → a `[USR]` position that
  yields a **768-dim per-loan embedding**. RoPE positional encoding, RMSNorm, SwiGLU, SDPA
  (FlashAttention).
- **Tokenization (key-value-time):** every field becomes a fused `field=value` token; each monthly
  block is anchored by a relative time token and an absolute `cal=<YYYYQn>` calendar token (the
  macro-regime signal). Numeric fields use quantile bins with boundaries forced at regulatory
  cliffs (LTV 80/90/95/97, DTI 36/43/45). Vocabulary: 552 tokens, 45 field types, fit on the
  training split only and then frozen.
- **Pretraining objective:** masked-language-modelling with three simultaneous corruption sources —
  15% of tokens, 10% of whole monthly events, 10% of entire field types (80/10/10 replace/random/keep).
  No labels are used.
- **Framework:** [`credit_fm`](https://github.com/Algoritmica-ai/deeploans/tree/main/credit-foundation-model) (PyTorch, Apache-2.0).

## Training

| Setting | Value |
|---|---|
| Pretraining steps | **20,000** |
| Effective batch | 256 (micro-batch 64 × gradient accumulation 4) |
| Optimiser | AdamW, lr 3e-4, weight decay 0.01, warmup 1,000, grad-clip 1.0 |
| Precision | bf16 |
| Corpus | ~3.0B tokens (10% loan-hash sample) |
| Seed | 42 |

**Training data.** A public panel of US single-family mortgage performance data covering
2000–2024 — roughly 3.3B loan-month observations across tens of millions of fixed-rate loans. This
model was pretrained on a deterministic 10% loan-hash sample. The pretraining corpus is **capped at
reporting date Dec-2022**, so the model has never seen the 2023–2024 period used for evaluation.

Static fields (LTV, CLTV, DTI, credit scores, term, rate, loan purpose, property type and state,
occupancy) and dynamic monthly fields (current rate, current balance, remaining term, loan age) are
used. **Outcome and contemporaneous-state columns are excluded** — current delinquency status,
zero-balance codes, foreclosure and disposition dates, loss and expense fields. Including them would
be circular; they are dropped from the model *and* from every baseline it is compared against.

## Evaluation

Evaluated **out-of-time**, the way deployment actually tests a credit model: a head is fit on
observation snapshots from 2016–2021 and tested on 2022/2023 snapshots whose 12-month default
windows fall in 2023–2024 — years neither backbone nor head has seen. Test population: 1,782,453
loan observations at a **0.14% default rate**. Splits are by loan (never by row), temporal by
origination, with loan-disjoint and embargo guards.

| Model | ROC-AUC | Average precision |
|---|--:|--:|
| Gradient-boosting baseline (57 leakage-audited features) | 0.7913 | 0.0057 |
| Credit FM 26M, full fine-tune | 0.8257 | 0.0113 |
| **This backbone, full fine-tune** | **0.8447** | **0.0160** |

At a 0.14% base rate, **average precision is the operational metric** — of the loans you flag as
riskiest, how many truly default. The lift there is roughly **2.8×** over the baseline.

Adaptation mode matters: at 26M, frozen embeddings reach 0.7309, LoRA 0.8068, full fine-tuning
0.8257. Frozen embeddings alone do *not* beat good features — the representation pays once the
backbone is allowed to adapt.

**Scaling.** Growing the backbone 2.6× on unchanged data was **flat** (0.8223 vs 0.8257); growing
the data recovered most of the gain. The practitioner's rule is *feed the model before you grow it*.

## Provenance and an honest note

These weights are the checkpoint from the **independently reproduced** run: the full pipeline was
re-executed end to end from raw data, on fresh output paths, after nine refactoring pull requests.
Deterministic stages matched exactly (identical split loan counts, 2,997,726,396 tokens, and an
identical 1,782,453-observation evaluation population), and the fine-tuned endpoint landed at
0.8447 / 0.0160.

An earlier run of the same configuration reported **0.8468 / 0.0175**. Its backbone weights were
subsequently overwritten at their storage location and no longer exist. We therefore publish the
reproduction checkpoint, which is the one that can be verified end to end, and we report its own
numbers above rather than the slightly higher original. The two runs differ by less than the
run-to-run band that was pre-registered for this experiment.

## Intended use

**Primary:** research and benchmarking of sequence foundation models for consumer-credit risk;
producing per-loan embeddings for downstream tasks (default scoring, prepayment, segmentation) via a
frozen probe, LoRA, or full fine-tuning.

### Out of scope / prohibited

- **Not for production credit decisions or adverse action** — approve/deny, pricing, credit-line
  changes — without independent validation, fair-lending review, and model-risk governance
  (e.g. SR 11-7). Outputs are probabilistic and learned from historical patterns.
- Not a regulatory-compliant scorecard. `property_state` is a feature and geographic proxies for
  protected classes are possible; **fair-lending (ECOA/FCRA) analysis is required before any
  lending use.**
- Trained on US conforming single-family mortgages. Do not apply to other asset classes or
  geographies without revalidation.

## Limitations

- **Single corpus.** All results come from one national mortgage market.
- **Benign test years.** 2023–2024 had a low default rate (0.14%), so absolute AP is small for every
  model; the *relative* lift is the claim, not the absolute value.
- **Statistical power.** With roughly 2,500 test positives, a ROC increment of ±0.01 is about one
  standard error. The margin over the baseline (+0.053) is far outside that; small within-family
  increments are weaker evidence.
- **A backbone, not a product.** No calibration is applied here. Converting scores to probabilities
  of default requires the calibration stage in the framework, fit on a window that does not touch
  your test period.
- **Scale ceiling.** Tested to 100.9M parameters and ~3.0B tokens; larger models on the full corpus
  are untested.

## Usage

The three-branch architecture is custom, so this is not an `AutoModel`. Install the framework, then
load the weights:

```bash
pip install "credit_fm @ git+https://github.com/Algoritmica-ai/deeploans.git#subdirectory=credit-foundation-model"
```

```python
import json, torch
from safetensors.torch import load_file
from credit_fm.models import CreditFoundationModel

cfg = json.load(open("config.json"))
model = CreditFoundationModel(
    vocab_size=cfg["vocab_size"], n_field_types=cfg["n_field_types"], dim=cfg["dim"],
    n_heads=cfg["n_heads"], profile_layers=cfg["profile_layers"],
    event_layers=cfg["event_layers"], history_layers=cfg["history_layers"],
)
model.load_state_dict(load_file("model.safetensors"))
model.eval()   # -> per-loan [USR] embeddings; attach a head to predict
```

See `load_example.py` for a runnable version, and the framework's handbook for the encoding path
that turns a raw loan panel into model inputs.

## Citation

```bibtex
@software{credit_fm_2026,
  title  = {credit_fm: an open framework for training credit foundation models},
  author = {Algoritmica GmbH},
  year   = {2026},
  url    = {https://github.com/Algoritmica-ai/deeploans/tree/main/credit-foundation-model},
  license = {Apache-2.0}
}
```
