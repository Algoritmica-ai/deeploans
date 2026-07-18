# Part 3 — Project Structure

## 3.1 The tree

```
credit-foundation-model/
├── src/credit_fm/                 ← THE PACKAGE (pip-installable, asset-blind)
│   ├── tokenizer/                 KVT tokenizer: vocabulary, buckets, categories
│   │   ├── key_value_time.py        the main KVTTokenizer (fit/encode/save/load)
│   │   ├── numeric_bucketer.py      quantile bins + anchor cut-points
│   │   ├── categorical.py           category → token label (rare → OTHER)
│   │   └── vocabulary.py            token string ↔ id; the 9 special tokens
│   ├── models/                    the three-branch transformer
│   │   ├── base.py                  RMSNorm, RoPE, attention (SDPA), SwiGLU, Embeddings, masks
│   │   ├── profile_encoder.py       static-facts branch  → one profile vector
│   │   ├── event_encoder.py         monthly-facts branch → one vector per month
│   │   ├── history_encoder.py       timeline branch      → the [USR] loan embedding
│   │   ├── mlm_head.py              pretraining head (local+segment+loan → vocab logits)
│   │   ├── classification_head.py   downstream head (loan embedding → class logits)
│   │   └── credit_fm.py             CreditFoundationModel: wires it all together
│   ├── data/                      everything between parquet and tensors
│   │   ├── dataset_config.py        the dataset CONTRACT loader (dataset.yaml)
│   │   ├── adapter.py               DatasetAdapter protocol + registry + generic adapter
│   │   ├── labels.py                declarative labels (forward_event_entities, resolve_label_spec)
│   │   ├── splits.py                temporal_loan_split (loan-disjoint, by origination)
│   │   ├── streaming.py             bigger-than-RAM path: fragments, pass-1/pass-2, buckets
│   │   ├── encode.py                encode-once shards (+ the parallel worker pool)
│   │   ├── dataset.py               CreditSequenceDataset: shards → per-loan tensors
│   │   ├── collators.py             MLMCollator: pad + mask into (B, L) batches
│   │   ├── datamodule.py            CreditDataModule: loaders (DistributedSampler-aware)
│   │   └── schema.py                CreditPanelSchema helpers
│   ├── training/
│   │   ├── masking.py               three-source MLM masking (token/event/type)
│   │   ├── optimizers.py            AdamW groups + warmup-cosine schedule
│   │   ├── trainer.py               train_mlm: the loop (accum, resume, DDP, logging hooks)
│   │   ├── distributed.py           DistInfo, init/cleanup, barrier (DDP plumbing)
│   │   └── loggers.py               Null/Jsonl/TensorBoard/Wandb metrics backends
│   ├── inference/
│   │   ├── scoring.py               load_finetuned, observe_panel (the cutoff!), score_panel, LoRA
│   │   └── calibration.py           isotonic/Platt score→PD mapping, Brier, reliability
│   └── utils/
│       ├── config.py                the YAML engine (include/${}/dotted CLI)
│       ├── storage.py               fsspec I/O: local/gs://; retry; exists/isdir/read_text
│       └── reproducibility.py       set_seed
├── scripts/                       ← ONE SCRIPT PER STAGE (thin; logic lives in src/)
│   ├── ingest.py                    asset-blind sharded ingest driver
│   ├── prepare_data.py              split (in-RAM or --stream)
│   ├── classify_schema.py           propose field routing (leakage dropped first)
│   ├── train_tokenizer.py           fit KVT vocab on TRAIN only
│   ├── encode_dataset.py            panel → token-id shards
│   ├── pretrain.py                  MLM pretraining (single-GPU or torchrun DDP)
│   ├── extract_embeddings.py        loans → cached [USR] vectors
│   ├── evaluate_downstream.py       probes on frozen embeddings vs feature baselines
│   ├── finetune.py                  frozen/LoRA/full task adaptation
│   ├── train_baseline.py / build_oot_baseline.py   the honest XGBoost bar
│   ├── score_portfolio.py           batch scoring (+ --calibrator)
│   ├── calibrate.py                 fit the score→PD calibrator (refuses test windows)
│   ├── validate_{ingest,splits,dataset,scores}.py  the artifact auditors
│   ├── profile_fannie_dataset.py / compare_profiles.py   data profiling
│   ├── publish_model.py             package a checkpoint for release
│   ├── run_*.sh                     end-to-end experiment orchestration (nohup-able)
│   └── setup_container.sh           idempotent H100-container bring-up
├── configs/
│   ├── fannie_mae/                  the reference recipes (one YAML per stage)
│   │   ├── dataset.yaml               THE CONTRACT: columns, labels:, exclude:, leakage:
│   │   ├── common.yaml                paths defined once (${gcs_root}, ${run_name})
│   │   ├── tokenizer.yaml / tokenizer.json   field schema / the frozen 552-token vocab
│   │   └── ingest, prepare, encode, pretrain*, finetune*, scoring, calibrate .yaml
│   └── dutch_mortgages/             second asset: proof the framework is YAML-only
├── reference_implementations/
│   └── fannie_mae/                  ALL Fannie-specific code lives here, not in src/
│       ├── adapter.py                 FannieMaeAdapter (derivations, hive paths, sampling)
│       ├── fannie_glossary.py         the 113-column published layout
│       ├── serve.py                   FastAPI serving example
│       └── README.md                  runbook + curl demo
├── notebooks/                     00_data_bible … 05_new_dataset — GENERATED from
│   └── build_*.py                 builder scripts (edit the builder, never the .ipynb)
├── tests/                         unit tests + script-level tests + negative controls
├── docs/                          architecture, tokenization, training, evaluation,
│                                  configuration, extending, decision_log, technical_report,
│                                  model_cards/, data_cards/
├── models/  reports/              packaged checkpoints; canonical run reports
└── pyproject.toml                 the package: lean core deps + [gcs][baselines][logging][serving][dev]
```

## 3.2 What belongs where — and what must never

| Folder | Belongs here | Must NEVER be here |
|---|---|---|
| `src/credit_fm/` | Generic, reusable, importable logic; type-hinted public APIs | **Anything asset-specific.** No "fannie" in any name; no hardcoded columns beyond the contract's parameters; no file paths. `tests/test_asset_blind.py` fails the build otherwise |
| `scripts/` | Thin orchestration: parse config → call `src/` → print/write artifacts | Business logic you'd want to unit-test directly (put it in `src/`, script imports it); state (scripts must be re-runnable) |
| `configs/` | Declarative facts: paths, hyperparameters, the dataset contract | Code, secrets, absolute machine-local paths (that's what env vars `CREDIT_FM_GCS_KEY` / `CREDIT_FM_BUCKET` are for) |
| `reference_implementations/` | Everything the raw source forces you to know (Fannie's MMYYYY dates, zero-balance codes, hive layout) | Generic logic other assets would need (promote it to `src/`) |
| `tests/` | Fast synthetic-data tests; script tests via subprocess; negative controls | Tests needing network/GCS/GPU without a skip guard |
| `notebooks/` | Generated teaching artifacts | Hand edits (they're overwritten by `build_*.py`); heavy computation |
| `docs/` | Explanations that outlive a PR | Run-by-run results (those go to `reports/`) |
| repo root | — | **Data** (all `data/*` gitignored — note the anchored pattern: a bare `data/` once silently ignored `src/credit_fm/data/`!), secrets, internal trackers |

## 3.3 How a new developer should think about it

**The dependency arrow points one way:**

```
scripts/  ──imports──▶  src/credit_fm/  ◀──imports── reference_implementations/
   │                          ▲
   └── reads ──▶ configs/ ────┘  (as data, at runtime)
```

`src/` never imports from `scripts/` or `reference_implementations/` (the adapter registry
resolves reference implementations *lazily by name*, so the arrow holds even for adapters).

**Three questions locate any change:**

1. *Is it true for every dataset?* → `src/credit_fm/`
2. *Is it true only for this dataset?* → `reference_implementations/<asset>/` or its `configs/`
3. *Is it a fact rather than logic* (a path, a threshold, a column list)? → `configs/`

**Before any commit** (the repo's non-negotiables): `ruff check .` clean, `pytest` green, and if
you produced a data artifact, its validator passes. Work happens on branches; `main` moves only
by PR.

### Things to remember

1. The dependency arrow: scripts → src ← reference_implementations; `src/` never imports asset code (test-enforced).
2. Three questions locate any change: generic → src/ · asset-specific → reference_implementations|configs · a fact → configs/.
3. Data, secrets, and internal trackers never enter the repo (note the anchored `data/*` gitignore lesson).
4. Before any commit: ruff clean, pytest green, and the relevant validator passes.

---
*Next: [Part 4 — End-to-End Data Flow](04_end_to_end_data_flow.md): the Ohio loan rides the whole pipeline.*
