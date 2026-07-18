# Part 14 — Metrics: How to Grade a Rare-Event Model

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ split ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ [SCORE] ─▶ calibrate ─▶ serve


> Used in: `finetune.py`, `evaluate_downstream.py`, `validate_scores.py` (recall@K/lift + check I),
> `calibration.py` (Brier, reliability). The base rate that shapes everything: **~0.14%** of
> observations default at a 2022 cutoff.

## 14.1 The confusion matrix — the atom of everything

Pick a threshold (say: flag every loan scoring above 0.5). Four outcomes exist:

```
                        actually defaults      actually fine
model flags it     │  TP  true positive   │  FP  false alarm      │
model passes it    │  FN  missed default  │  TN  true negative    │
```

Every metric below is arithmetic on these four numbers.

- **Accuracy** = (TP+TN)/all. **Useless here**: predicting "nobody defaults" scores 99.86%.
  Delete it from your vocabulary for this project.
- **Precision** = TP/(TP+FP) — *of the loans I flagged, what fraction actually defaulted?*
- **Recall** = TP/(TP+FN) — *of the loans that defaulted, what fraction did I flag?*
- **F1** = harmonic mean of the two — a single-number compromise when you must pick a threshold.
  Rarely used here because we mostly evaluate *rankings*, not thresholds.

Precision and recall fight: flag more loans → recall ↑, precision ↓. Every threshold is one
point in that tradeoff, which is why threshold-free curves come next.

## 14.2 ROC and ROC-AUC

Sweep the threshold from strict to loose; at each point plot **true-positive rate** (recall)
vs **false-positive rate** (FP/all-negatives). The area under that curve (**ROC-AUC**) has a
beautiful interpretation:

> Pick a random defaulter and a random non-defaulter. ROC-AUC = the probability your model
> scores the defaulter higher.

0.5 = coin flip, 1.0 = perfect ordering. Our 0.8468: given a random (defaulter, survivor) pair,
the model ranks them correctly ~85% of the time.

```
 TPR 1 ┤          ╭────────
       │      ╭───╯   ← model (AUC 0.85)
       │   ╭──╯
       │ ╭─╯   ╱  ← coin flip (AUC 0.5)
       │╭╯   ╱
     0 ┼───╱──────────── FPR
       0                1
```

## 14.3 Why ROC alone lies at 0.14% base rates

ROC's x-axis divides by the number of *negatives* — which is enormous. A model can look great on
ROC while being operationally useless. Concrete example with our numbers (1,000,000 loans, 1,400
defaults):

> A model with a superb-sounding 1% false-positive rate flags 10,000 innocent loans. Even
> catching *every* defaulter (recall 100%), precision = 1,400/11,400 ≈ **12%** — 7 of 8 flags
> are false alarms. ROC never showed you that, because 10,000 false alarms is "only 1%" of a
> million negatives.

**PR curve / PR-AUC (average precision)** fixes the denominator: plot precision vs recall as the
threshold sweeps. Both axes now care about the rare class; the no-skill baseline is the base rate
itself (0.0014), not 0.5. This is why the repo treats PR-AUC as the headline-worthy metric:

```
XGBoost bar   PR-AUC 0.0057   (4× better than random)
FM 100M       PR-AUC 0.0175   (12× better than random; 3.1× the bar)
```

— while the ROC gap (0.7913 → 0.8468) *sounds* modest. At rare events, PR-AUC is where the
business value shows. (Aside: **Gini**, the credit-industry favorite, is just 2·ROC-AUC − 1 —
our 0.8468 ≈ Gini 0.69.)

**Recall@K / lift — the operational translation** (printed by `validate_scores.py`): "if you can
only review the riskiest K% of the book, what share of defaults do you catch?" Lift = precision
in the flagged set ÷ base rate. This is the language of a collections/review team's budget, and
the honest way to present a rare-event model to a business owner.

## 14.4 Probability vs ranking — two different promises

Everything above grades **ranking** (who's riskier than whom). A **probability** is a stronger
promise: "loans I score 2% default 2% of the time." Our fine-tuned model deliberately breaks
that promise (rebalanced training, Part 8) — mean raw score ~0.4 vs realized ~0.5%.

**Calibration** metrics grade the promise:

- **Brier score** = mean (predicted − outcome)² — lower is better; punishes both bad ranking and
  bad levels.
- **Reliability table** (a calibration curve in numbers): bin by predicted PD, compare each
  bin's mean prediction to its realized default rate. Perfect = diagonal.
- **Calibration-in-the-large**: does the *average* prediction match the *overall* rate? The
  crudest and most important check — `validate_scores` check I gates it (mean pd within 2× of
  realized), which raw scores fail by ~50×.

Ranking metrics survive any monotone rescaling; calibration doesn't. That's why the pipeline can
rank first and calibrate later (Part 15) without ever retraining.

## 14.5 The evaluation protocol behind every number

A metric is only as honest as its test set. Repo law, one more time: **calendar out-of-time,
loan-disjoint, embargoed** — fit on Dec-2016…2021 observations, test on Dec-2022/2023 whose
outcome windows the model never saw, loans spanning both eras hash-assigned to one side, against
an XGBoost bar built on the *identical* windows. Any number not produced under this protocol
does not go in a report. (`validate_scores --min-roc` exists so even the *scoring* of a
deployed artifact can be gated on reproducing its certified quality.)

### Things to remember

1. Delete 'accuracy' from your vocabulary at a 0.14% base rate.
2. ROC-AUC = P(random defaulter ranks above random survivor); PR-AUC is where rare-event truth lives.
3. Recall@K / lift is the translation a review team can act on.
4. Ranking and probability are different promises; Brier/reliability/in-the-large grade the second.
5. No number counts unless it's calendar-OOT, loan-disjoint, and next to the XGBoost bar.

---
*Next: [Part 15 — Inference](15_inference.md): from checkpoint to a served, calibrated PD.*
