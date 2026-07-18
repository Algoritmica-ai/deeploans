# Fannie Mae — KVT Tokenizer Report (M1)

Fitted on `gs://sriram-credit-fm-data/output/processed/fannie_mae/run_2000_2022/train.parquet` (30,000,000 rows). Schema `configs/fannie_mae/tokenizer.yaml`; saved to `configs/fannie_mae/tokenizer_v2.json`.

## Vocabulary

- **552 tokens** (9 special + field value tokens).
- **Profile** 31 fields (16 numeric / 15 categorical); **Event** 12 fields (11 numeric / 1 categorical); time field `loan_age`.

## Sequence length (QA sample: 2,000 loans)

| stat | tokens / loan |
|---|--:|
| min | 66 |
| median | 754 |
| p95 | 994 |
| max | 994 |

## Token health

| metric | value |
|---|--:|
| roundtrip lossless | 100.0% of loans |
| unseen-category tokens (`=UNK`) | 0.00% |
| missing tokens (`=NA`) | 48.26% |

## Notes
- Bins/categories fit on TRAIN only; unseen values map to `=UNK`, missing to `=NA`.
- Roundtrip is token-level lossless (fused `field=value` tokens); numeric exact values are bucketed by design, so the QA target is losslessness + low OOV, not value reconstruction.