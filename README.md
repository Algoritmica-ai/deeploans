# Credit Foundation Model Framework

An **open-source framework (Apache 2.0) for training credit foundation models** on tabular
credit panel data. Built by **[finevals.ai](https://finevals.ai)** so banks and financial
institutions can train their own credit foundation models on their own data without building
the underlying infrastructure from scratch.

The thesis: a **sequence foundation model** — one that reads a borrower's full month-by-month
history — beats point-in-time tabular models (XGBoost) on credit tasks. The framework ships
with a reference implementation on 25 years of real-world mortgage performance data, where
that thesis is validated **out-of-time**: trained on the past, tested on genuinely unseen
future years.

## Headline result (out-of-time)

Fine-tuned on observation snapshots from 2016–2021 and tested on **2022/2023 snapshots whose
12-month default windows (2023–24) neither the model nor its head ever saw**:

| Model | ROC-AUC | PR-AUC (AP) |
|---|--:|--:|
| XGBoost baseline (57 no-leakage features, identical window) | 0.7913 | 0.0057 |
| Credit FM 26M (full fine-tune, 4% corpus) | 0.8257 | 0.0113 |
| **Credit FM 100M (full fine-tune, 10% corpus)** | **0.8468** | **0.0175** |

Both metrics beat the baseline decisively — ROC +0.056, **AP 3.1×** — on real, rare-event data
(~0.14% default rate). The 26M→100M step is a measured scaling result: parameters alone did
nothing (65M on unchanged data was flat); data + parameters together paid. See
[`docs/technical_report.md`](docs/technical_report.md).

## Approach

Encoder-only (BERT-style) architecture with masked-language-modelling pretraining over
credit-event sequences, following the PRAGMA line of work (encoder-only, three-branch,
published +130% PR-AUC on credit scoring) with the transaction-foundation-model blueprint as
the engineering baseline.

The framework is **schema-agnostic and config-driven**: adapt to a new asset class by writing
YAML recipes, then running the same scripts — no code changes.

**New to foundation models (or to credit data)?** Start with the
[Handbook](docs/handbook/00_README.md) — a self-study reference that teaches the whole system
from zero, one traced loan at a time.

## Bring your own dataset

Onboarding is one contract file — `configs/<asset>/dataset.yaml` — declaring your id/time
columns, task labels, and the machine-enforced leakage list. If your panel already conforms,
`adapter: generic` means **zero code**; if the raw source needs parsing, you write one adapter
class outside the core package. Then the identical scripts run split → tokenize → encode →
pretrain → finetune, with an artifact validator auditing each stage. New task = one YAML block
(prepayment on the reference corpus is literally `label: prepay_12m`).

Start with [`docs/extending.md`](docs/extending.md) and the runnable walkthrough
[`notebooks/05_new_dataset.ipynb`](notebooks/05_new_dataset.ipynb); the recipe grammar
(includes, `${...}` interpolation, dotted CLI overrides) is
[`docs/configuration.md`](docs/configuration.md).

## Architecture (three-branch encoder)

```
static fields ─▶ Profile Encoder (3L) ──┐
                                        ├─▶ History Encoder (6L) ─▶ [USR] loan embedding
monthly events ─▶ Event Encoder (5L) ───┘
```

- **Key-value-time (KVT) tokenization** — every field becomes a fused `field=value` token,
  each monthly event block anchored by a `t=<age>` coordinate and a `cal=<YYYYQ#>` calendar
  token (the macro-regime signal). Numeric fields use quantile bins with forced boundaries at
  regulatory cliffs (e.g. LTV 80/90/95/97).
- **Pretraining**: MLM with three masking sources (15% tokens / 10% whole events / 10% field
  types); vocabulary and bins fit on the train split only.
- **Downstream**: frozen `[USR]` embeddings + XGBoost, or a classification head fine-tuned
  frozen / LoRA / full — interchangeable heads on the same backbone.
- Two reference sizes: 26M @ dim 384 and 100M @ dim 768 (Chinchilla-honest for their corpora);
  RoPE, RMSNorm, SwiGLU; FlashAttention (SDPA); 8-GPU DDP with checkpoint/resume.

## Data

The **reference corpus** is the public
[Fannie Mae Single-Family Loan Performance dataset](https://capitalmarkets.fanniemae.com/credit-risk-transfer/single-family-credit-risk-transfer/fannie-mae-single-family-loan-performance-data)
— ~25 years of US fixed-rate mortgages (2000–2024, ~3.3B loan-month rows; pretraining uses a
validated 4% loan-hash sample). A synthetic Dutch RMBS panel (ESMA Annex 2) serves as a
controlled validation/ablation set.

Every stage enforces **leakage discipline**: splits are by loan (never by row) and temporal by
origination; outcome/contemporaneous-state columns are excluded from features; the vocabulary
is fit on train only; evaluation is calendar-out-of-time with loan-disjoint and embargo guards.
Start with the data bible: [`notebooks/00_data_bible.ipynb`](notebooks/00_data_bible.ipynb).

## What's in here

| Component | Location | Description |
|-----------|----------|-------------|
| Framework (`credit_fm`) | `src/credit_fm/` | KVT tokenizer, three-branch model, data layer, training, utils |
| Pipeline scripts | `scripts/` | one config-driven script per stage (ingest → … → finetune) + artifact validators |
| Recipes | `configs/fannie_mae/`, `configs/dutch_mortgages/` | YAML per asset class; stage recipes + generated schemas |
| Notebooks | `notebooks/` | `00_data_bible` … `05_new_dataset` (builder-generated) |
| Reference implementations | `reference_implementations/` | per-asset adapters + runbooks |
| Docs | `docs/` | **handbook/** (teach-from-zero reference) · architecture · configuration · extending · tokenization · training · evaluation · decision log · cards |

## Differentiation

1. **Open source** — weights, code, tokenizers, references all Apache 2.0 (comparable
   industrial systems are proprietary).
2. **Real-world corpus, honest evaluation** — public 25-year mortgage data; calendar
   out-of-time verdicts with loan-disjoint + embargo guards, against a strong no-leakage
   XGBoost bar.
3. **Sovereign-cloud-deployable** — runs entirely on customer infrastructure, no external APIs.
4. **Validated pipeline** — each stage ships unit tests plus an artifact validator that
   re-derives the produced output (`scripts/validate_*.py`).

## Quickstart

Every script follows one grammar: `-c <recipe.yaml>` plus dotted overrides
(`--key.path value`). On a GPU container see
[`docs/container_setup.md`](docs/container_setup.md); otherwise:

```bash
pip install -e ".[gcs,baselines]"     # extras: [gcs] gs:// backend · [baselines] xgboost ·
                                      #         [logging] wandb/tensorboard · [dev] tests+lint

# 1. ingest the raw source into a per-loan monthly panel (labels derived);
#    sharded + resumable — a killed run reruns the same command and skips finished quarters
python scripts/ingest.py -c configs/fannie_mae/ingest.yaml

# 2. loan-stratified temporal split (+ artifact audit)
python scripts/prepare_data.py -c configs/fannie_mae/prepare.yaml
python scripts/validate_splits.py --dir <out_dir>

# 3-4. field schema + fit the KVT tokenizer (train split only)
python scripts/classify_schema.py -c configs/fannie_mae/classify.yaml
python scripts/train_tokenizer.py -c configs/fannie_mae/tokenizer_fit.yaml

# 5-6. encode-once shards + MLM pretraining (multi-GPU: python -m torch.distributed.run
#       --standalone --nproc_per_node 8 scripts/pretrain.py -c <recipe>)
python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml
python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml

# 7-8. embeddings + the out-of-time verdict
python scripts/extract_embeddings.py -c configs/fannie_mae/extract.yaml
python scripts/build_oot_baseline.py --train-years 2016-2021 --test-years 2022-2023
python scripts/finetune.py -c configs/fannie_mae/finetune_oot.yaml
```

## Repository layout

See [`docs/architecture.md`](docs/architecture.md) for the full map.

```
src/credit_fm/   tokenizer/ models/ data/ training/ utils/
configs/         fannie_mae/ · dutch_mortgages/          (YAML recipes per asset class)
scripts/         ingest · prepare · classify · tokenizer · encode · pretrain ·
                 extract · evaluate · finetune · baselines · validators · publish
notebooks/       00_data_bible · 01_data_splits · 02_schema_classification (+ builders)
reference_implementations/   per-asset runbooks
models/          packaged checkpoints
docs/            architecture · tokenization · training · evaluation · decision_log ·
                 technical_report · model_cards/ · data_cards/
tests/           unit tests + artifact-validator tests
```

## References

- PRAGMA — Ostroukhov et al., 2026 (arXiv:2604.08649) — primary architectural reference
- [Transaction foundation model blueprint](https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model) — engineering baseline
- BERT — Devlin et al., 2019 · LoRA — Hu et al., 2021 · Chinchilla — Hoffmann et al., 2022
- Synthetic Dutch RMBS: [`Algoritmica/green-lion-2024-2025`](https://huggingface.co/datasets/Algoritmica/green-lion-2024-2025) · ESMA Annex 2 — (EU) 2020/1224

## License

Apache 2.0 — see [LICENSE](LICENSE).
