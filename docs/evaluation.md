# Evaluation

Two layers, deliberately separate:

1. **Pretraining health check** — masked-token validation loss on held-out loans. A dial, not a
   result; never quoted as a model claim.
2. **The verdict — calendar out-of-time (OOT) default prediction.** Train on the past, test on
   genuinely unseen future years. This is the number the model is judged by.

## The OOT protocol

Observe every loan at **Dec 31 of each year** in the train range; keep only loans *performing*
at that date (the gate — predict **new** defaults, not ones in progress); label = default
(180+ days delinquent or a credit-event zero-balance code) **within the next 12 months**.
Guards:

- **loan-disjoint** — a loan is wholly in train or test, never both (span-both loans assigned
  by hash);
- **embargo** — train years whose forward label window reaches the test period are dropped
  (e.g. train 2016–2020, buffer 2021, test 2022–2023 → defaults in 2023–24);
- **val** — a loan-disjoint 10% of train loans, for early stopping only;
- negatives are downsampled on **train only**; test stays at the true base rate.

`scripts/build_oot_baseline.py` builds the XGBoost bar (57 no-leakage features, identical
window); `scripts/finetune.py` + `scripts/evaluate_downstream.py` run the FM through the same
split/label/metric.

## Metrics

**ROC-AUC** (population ranking) and **PR-AUC / AP** (tail sharpness — the operational metric
at ~0.1% base rates: of the loans flagged riskiest, how many truly default). AP is judged
first. Calibration is reported separately from ranking and is a tracked follow-up for the
release (a risk decision needs calibrated PDs, not just rank order).

## Feature regimes compared

XGBoost on raw features (the bar) · frozen `[USR]` embeddings + XGBoost · raw + embeddings
combined (PCA-compressed) · linear probe · fine-tuned head (frozen / LoRA / full).

## Reference results (mortgage corpus, OOT 2022–23 observations → 2023–24 defaults)

| Model | ROC-AUC | AP |
|---|--:|--:|
| XGBoost baseline | 0.7913 | 0.0057 |
| FM 26M frozen head | 0.7309 | 0.0052 |
| FM 26M LoRA | 0.8068 | 0.0087 |
| FM 26M full fine-tune | 0.8257 | 0.0113 |
| FM 65M full (params only ↑, 4% corpus) | 0.8223 | — |
| FM 26M full (data only ↑, 10% corpus) | 0.8406 | 0.0145 |
| **FM 100M full (10% corpus — headline)** | **0.8468** | **0.0175** |

Frozen embeddings alone don't beat strong tabular features — adaptation unlocks the win
(full > LoRA > frozen), and scale pays only when data grows with the model (65M flat; 100M+10%
is the headline — the scaling decomposition is §7.4 of the technical report). On a benign in-distribution window (no regime shift) the tabular
baseline wins narrowly, as expected; the FM's edge appears exactly where it should — crossing
into unseen years. Full detail: `technical_report.md`.
