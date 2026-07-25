# Mortgage Performance — KVT Tokenizer Report (M1)

Fitted on `/workspace/data/payment_behaviours/processed/run_pb_v1/train.parquet` (20,117,130 rows). Schema `configs/payment_behaviours/tokenizer.yaml`; saved to `configs/payment_behaviours/tokenizer.json`.

## Vocabulary

- **47 tokens** (9 special + field value tokens).
- **Profile** 0 fields (0 numeric / 0 categorical); **Event** 1 fields (1 numeric / 0 categorical); time field `seq_index`.

## Sequence length (QA sample: 2,000 loans)

| stat | tokens / loan |
|---|--:|
| min | 7 |
| median | 35 |
| p95 | 515 |
| max | 515 |

## Token health

| metric | value |
|---|--:|
| roundtrip lossless | 100.0% of loans |
| unseen-category tokens (`=UNK`) | 0.00% |
| missing tokens (`=NA`) | 0.00% |

## Notes
- Bins/categories fit on TRAIN only; unseen values map to `=UNK`, missing to `=NA`.
- Roundtrip is token-level lossless (fused `field=value` tokens); numeric exact values are bucketed by design, so the QA target is losslessness + low OOV, not value reconstruction.