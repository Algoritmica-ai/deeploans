# Fannie Mae — Out-of-Time Gate-G1 Baseline

Calendar split — **train 2000–2006**, **test 2008-2010** (val = 10% of train loans). 20% loan sample; 12-month default horizon; 57 no-leakage features. Loan split: **disjoint**; clean train/test gap.

Each loan observed every Dec it is performing; label = default (D180 / Zero-Balance credit event) within the horizon. True out-of-time: the test years are never trained on.

## Population

| Split | observations | defaults | default rate |
|---|--:|--:|--:|
| train | 6,026,978 | 8,734 | 0.14% |
| val | 401,879 | 962 | 0.24% |
| test | 4,090,842 | 29,691 | 0.73% |

## Result (out-of-time test)

| Metric | value |
|---|--:|
| ROC-AUC | 0.7571 |
| PR-AUC | 0.0241 |
| test default rate | 0.73% |

## Test default rate by year

| obs_year | observations | default rate |
|---|--:|--:|
| 2008 | 1,192,944 | 0.99% |
| 2009 | 1,391,552 | 0.67% |
| 2010 | 1,506,346 | 0.56% |

## Notes
- Real-world Fannie Mae loan performance — default rates and lift reflect an actual portfolio (no synthetic inflation). Numbers depend on the ingested reporting span and observation date.
- Out-of-time by calendar year is the honest generalization test (train on the past, score the future). The foundation model must beat this ROC/PR-AUC.
- **Guards:** loan-disjoint = `disjoint` (no loan appears in both train and test); embargo = train label windows end before the test period (no macro bleed).