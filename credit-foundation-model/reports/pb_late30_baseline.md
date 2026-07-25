# Payment-behaviours rolling-stats baseline — late30_3m

Honest XGBoost bar on identical observations/labels/split as the FM fine-tune.
Observation windows: 2000-04-30..2004-01-31 -> 2000-10-31, 2003-01-31

- **test: 488,108 loans, 4.05% positive**
- **ROC-AUC 0.7699 | PR-AUC 0.0815**
- lift over base rate: 2.01x

Top features by gain:
  1. n (0.508)
  2. dpd_max (0.104)
  3. dpd_mean (0.096)
  4. dpd_std (0.081)
  5. dpd_mean3 (0.071)
  6. cnt_gt30 (0.040)
  7. dpd_max3 (0.027)
  8. ontime_streak (0.022)

## Head-to-head vs foundation model
- FM:       ROC 0.6871 | PR-AUC 0.0594
- baseline: ROC 0.7699 | PR-AUC 0.0815
- **verdict: baseline matches/leads FM** (ROC Δ -0.0828, AP Δ -0.0221)
