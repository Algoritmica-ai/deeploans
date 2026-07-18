# Fannie Mae — Out-of-Time Gate-G1 Baseline

Calendar split — **train 2016–2020**, **test 2022-2023** (val = 10% of train loans). 20% loan sample; 12-month default horizon; 57 no-leakage features. Loan split: **disjoint**; embargo dropped train years [2021].

Each loan observed every Dec it is performing; label = default (D180 / Zero-Balance credit event) within the horizon. True out-of-time: the test years are never trained on.

## Population

| Split | observations | defaults | default rate |
|---|--:|--:|--:|
| train | 9,093,392 | 41,931 | 0.46% |
| val | 703,882 | 2,759 | 0.39% |
| test | 4,404,806 | 5,827 | 0.13% |

## Result (out-of-time test)

| Metric | value |
|---|--:|
| ROC-AUC | 0.7913 |
| PR-AUC | 0.0057 |
| test default rate | 0.13% |

## Test default rate by year

| obs_year | observations | default rate |
|---|--:|--:|
| 2022 | 2,168,016 | 0.14% |
| 2023 | 2,236,790 | 0.13% |

## Notes
- Real-world Fannie Mae loan performance — default rates and lift reflect an actual portfolio (no synthetic inflation). Numbers depend on the ingested reporting span and observation date.
- Out-of-time by calendar year is the honest generalization test (train on the past, score the future). The foundation model must beat this ROC/PR-AUC.
- **Guards:** loan-disjoint = `disjoint` (no loan appears in both train and test); embargo = train label windows end before the test period (no macro bleed).