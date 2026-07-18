# Model Card — Credit Foundation Model (Fannie Mae reference, M5)

*finevals.ai · Apache-2.0 · card v2, 16 Jul 2026 (adds the 100M/10% headline model)*

## Model details

- **Developed by:** finevals.ai.
- **Model type:** encoder-only transformer, masked-language-modelling (MLM) over tabular
  credit-event sequences. Produces a per-loan embedding, not text.
- **Architecture:** three-branch encoder (Profile / Event / History) with key–value–time (KVT)
  tokenization. RoPE positional encoding, RMSNorm, SwiGLU. `[USR]` token yields a 384-dim
  per-loan summary embedding.
- **Size:** two reference configurations, same architecture and tokenizer:
  - 26M parameters · dim 384 · 8 heads · 3/5/6 layers — pretrained on the 4% sample;
  - **100M parameters · dim 768 · 8 heads · 3/5/6 layers — pretrained on the 10% sample
    (~3B tokens; the current headline model, `m_100m*.pt`).**
- **Tokenizer:** KVT, 552-token vocabulary (fit on the 2000–2022 train split only), up to 60
  monthly events per loan, `yearquarter` calendar token, quantile bins with anchors at regulatory
  cliffs (LTV 80/90/95/97, DTI 36/43/45).
- **Checkpoint:** `runs/m5_full.pt` (config + weights + lineage). Requires
  `configs/fannie_mae/tokenizer_v2.json` to encode inputs.
- **License:** Apache-2.0. **Framework:** `credit_fm` (PyTorch).

## Intended use

- **Primary:** research and benchmarking of sequence foundation models for consumer-credit risk;
  generating per-loan embeddings for downstream tasks (default scoring, prepayment, segmentation)
  via a frozen probe, LoRA, or full fine-tuning.
- **Reference implementation** demonstrating the `credit_fm` framework on real mortgage data.

### Out-of-scope / prohibited uses

- **Not for production credit decisions or adverse action** (approve/deny, pricing, credit-line
  changes) without independent validation, fair-lending review, and model-risk governance
  (e.g. SR 11-7). Predictions are probabilistic and trained on historical patterns.
- Not a substitute for regulatory-compliant scorecards. `property_state` is a feature and
  geographic proxies for protected classes are possible; **fair-lending (ECOA/FCRA) analysis is
  required before any lending use.**
- Trained on US conforming single-family mortgages; do not apply to other asset classes or
  geographies without revalidation.

## Training data

- **Source:** Fannie Mae Single-Family Loan Performance Data (public), 2000Q1–2024Q4.
- **Sample:** 4% of loans (deterministic hash on loan id) → ~2.26M loans, 125M monthly rows.
- **Pretraining corpus:** capped at reporting date Dec-2022 (so the model never sees the
  2023–2024 evaluation period) → 1.75M train loans, ~1.2B tokens.
- **Features (~43):** static origination facts (LTV, CLTV, DTI, credit scores, term, rate, loan
  purpose, property type/state, occupancy, …) and dynamic monthly facts (current rate, current
  UPB, remaining term, principal components, current credit scores). See the data card.
- **Excluded (leakage):** ~50 outcome/post-termination columns (current delinquency status,
  zero-balance codes, foreclosure/disposition dates, loss and expense fields, the label itself).
  Using these would be circular; they are dropped from both the model and the baseline.

## Training procedure

- **Objective:** masked-language-modelling with 3-source masking (15% token / 10% whole-event /
  10% whole-field-type; 80/10/10). No labels used in pretraining.
- **Schedule:** 30,000 steps, batch 32, AdamW + cosine (lr 3e-4, warmup 1000), dropout 0.1, bf16,
  single H100, ~3h38m. Best validation MLM loss **0.2303** (best-val checkpoint restored).
- **Adaptation (fine-tuning):** frozen head / LoRA (r=8, α=16) / full. Class imbalance (~0.1–0.14%
  positives) handled by fit-set negative downsampling (`neg_per_pos=20`, test untouched) and a
  capped class weight; per-epoch validation ROC at true class balance with best-epoch restore.

## Evaluation

Task: predict whether a performing loan defaults (D180 or credit-loss termination) within 12
months of an observation cutoff. Compared against a leakage-free XGBoost baseline on the same
features. Two regimes:

**Benign (in-period, Dec-2016 → 2017 defaults, full population):**

| Model | ROC-AUC | AP (PR-AUC) |
|---|--:|--:|
| XGBoost (features) | 0.8530 | 0.0142 |
| FM full fine-tune | 0.8417 | 0.0121 |

Features win narrowly — expected when there is no regime shift to exploit.

**Calendar out-of-time (train 2016–2021 → test 2022–2023, defaults in 2023–2024, unseen):**

| Model | ROC-AUC | AP (PR-AUC) |
|---|--:|--:|
| XGBoost baseline (same window) | 0.7913 | 0.0057 |
| FM 26M frozen | 0.7309 | 0.0052 |
| FM 26M LoRA | 0.8068 | 0.0087 |
| FM 26M full fine-tune | 0.8257 | 0.0113 |
| FM 65M full (params only ↑) | 0.8223 | — |
| FM 26M full (data only ↑, 10% corpus) | 0.8406 | 0.0145 |
| **FM 100M full fine-tune (10% corpus)** | **0.8468** | **0.0175** |

On genuinely unseen future loans the model **beats the baseline on both metrics** — ROC +0.056,
AP **3.1×** at 100M. The 65M/26M-on-10% rows are the measured scaling decomposition: parameters
alone did nothing; growing data and parameters together produced the headline (DL-015). The
behavioural sequence generalises across a shift into new years better than a point-in-time
snapshot. (Full technical report: `docs/technical_report.md`.)

## Limitations and caveats

- 2023–2024 were low-default years (0.13% base rate); absolute AP is small for all models — the
  FM's *relative* AP lift is the meaningful signal.
- The OOT baseline used a 20% sample and the FM a 4% sample (representative, not identical loans);
  effect sizes are far larger than plausible sampling noise but an identical-loan rerun would
  tighten confidence intervals (~1,000–5,800 test positives; ROC margin ≈ ±0.01).
- Validated on Fannie Mae only. The 2008–2010 crisis regime (hardest shift) is not yet tested.
- At 25.7M parameters the model is Chinchilla-matched to ~0.5B tokens; larger models on the full
  dataset are untested headroom.

## Responsible use

Mortgage default modelling has direct fair-lending and consumer-protection implications. This
model is a **research artifact**. Any deployment touching credit decisions must add: fair-lending
testing (disparate impact across protected classes and geographic proxies), model-risk governance
and documentation, human review, and adverse-action explainability — none of which this card
provides.

## Lineage / citation

- Checkpoint config, tokenizer, resolved run config, git commit, and source data checksums are
  stored with every artifact. Reproduction commands: `docs/technical_report.md` §10.
- Framework: `credit_fm` (Apache-2.0). Data: Fannie Mae Single-Family Loan Performance Data.
