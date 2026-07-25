# Length-stratified FM vs baseline — late30_3m

Test obs: 488,108 (4.05% positive). Both models scored on the identical set.

| history (invoices) | obs | %pos | base ROC | FM ROC | base AP | FM AP | winner |
|---|--:|--:|--:|--:|--:|--:|:--|
| 1-3 | 166,442 | 0.0% | nan | nan | 0.0000 | 0.0000 | — |
| 4-8 | 80,602 | 0.0% | nan | nan | 0.0000 | 0.0000 | — |
| 9-16 | 146,330 | 9.2% | 0.7272 | 0.6941 | 0.1803 | 0.1667 | baseline |
| 17-32 | 28,513 | 0.0% | nan | nan | 0.0000 | 0.0000 | — |
| 33-64 | 66,221 | 9.5% | 0.7670 | 0.6832 | 0.2836 | 0.1725 | baseline |

**Overall**: baseline ROC 0.7699 / AP 0.0815  ·  FM ROC 0.6871 / AP 0.0594

