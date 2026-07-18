# Fannie Mae — FM Downstream Eval (loan-holdout probe)

Observed at **2016-12-31**, label = default within **12 months** after. 150,000 performing loans; loan-disjoint 30% held out. Features = tokenizer profile+event fields as-of cutoff (same info the FM saw).

| model | ROC-AUC | PR-AUC | dROC vs features |
|---|--:|--:|--:|
| features (XGB) | 0.7459 | 0.0171 | +0.0000 |
| FM embeddings (XGB) | 0.6871 | 0.0088 | -0.0588 |
| combined (XGB) | 0.7189 | 0.0103 | -0.0270 |
| FM embeddings (linear probe) | 0.6755 | 0.0104 | -0.0704 |

## Read
- If **FM embeddings** or **combined** beats **features**, the FM adds signal beyond the raw as-of-cutoff features — the thesis, on real loans.
- This is a loan-holdout probe on one window, not the calendar-OOT vs 0.757/0.784 (which needs a multi-year panel + train-years/test-years split).