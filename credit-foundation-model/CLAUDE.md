# CLAUDE.md

Guidance for Claude Code (and humans) working in this repo. Read this first when reopening.

## What this is

An **open-source (Apache 2.0) framework for training credit foundation models** (`credit_fm`
package) plus reference implementations, by **finevals.ai**. Compute: 8× H100 box (8-GPU DDP
since v1.1 G4b: `PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 …`,
never bare `torchrun`).

**Reference corpus: single-family mortgage performance** (real-world US fixed-rate
mortgages, 2000–2024, ~3.3B loan-month rows; pretraining uses validated 4% / 10% loan-hash
samples) — see `docs/data/mortgage_performance.md` + `notebooks/00_data_bible.ipynb`. The **Dutch
mortgages** synthetic panel is the controlled **validation/ablation** set (it carries the
hidden `_segment` ceiling proof).

Approach: **encoder-only, masked-language-modelling** over tabular credit-event sequences
(PRAGMA-style), three-branch architecture, key-value-time tokenization. The thesis: a sequence
foundation model beats point-in-time tabular baselines (XGBoost) on credit tasks — **validated
out-of-time** (see Status).

## Architecture (locked — see `docs/decision_log.md`)

- **Encoder-only + MLM** (not decoder/causal) — DL-001.
- **Three-branch encoders**: Profile (static fields) + Event (per-month dynamics) + History
  (contextualizes the sequence) → `[USR]` per-loan embedding — DL-002. Reference sizes:
  26M @ dim 384 and 100M @ dim 768 (the headline); RoPE/RMSNorm/SwiGLU; architecture
  FROZEN since M2.
- **Key-value-time tokenization**: fused `field=value` tokens + `t=`/`cal=<YYYYQ#>` time
  coordinates; anchored quantile bins — DL-003/011/012. Frozen vocab `tokenizer.json`
  (552 tokens, full-corpus fit).
- **Encode-once shards + flat `(B,L)` batches** — DL-014.
- **DL-009 resolved (v1.1 G4c)**: pluggable metrics logger (`logging:` block — null default /
  jsonl / tensorboard / wandb-offline); nothing phones home unless explicitly configured.

## Repo layout
src/credit_fm/ tokenizer/ (KVT) · models/ (3-branch) · data/ · training/ · utils/
scripts/ one config-driven script per stage: ingest (asset-blind, sharded+resumable) ·
prepare_data · classify_schema · train_tokenizer · encode_dataset · pretrain ·
extract_embeddings · evaluate_downstream · finetune · score_portfolio · calibrate ·
train_baseline · build_oot_baseline · publish_model · profile/compare ·
validate_{ingest,splits,dataset,scores} (artifact auditors) · run_*.sh · setup_container.sh
configs/ mortgage_performance/ (reference) · dutch_mortgages/ (validation) — common.yaml + stage recipes
notebooks/ 00_data_bible · 01_data_splits · 02_schema_classification · 03_tokenizer_training · 04_encode · 05_new_dataset (+ build_*.py generators —
edit the builder, never the .ipynb)
reference_implementations/ mortgage_performance/ (adapter · glossary · serve.py FastAPI example ·
runbook README) · dutch_mortgages/
models/ packaged checkpoints · reports/ canonical run reports
docs/ handbook/ (26-part teach-from-zero reference) · architecture · configuration ·
extending · tokenization · training · evaluation · decision_log · technical_report ·
deployment · model_cards/ · data_cards/
tests/ unit + artifact-validator tests

## Status (Jul 2026)

**The science is done and v1.1 (G1–G6) is fully merged; remaining work is release polish.**
- **OOT headline (E11, 100M/10%):** head trained on Dec-2016…2021 observations, tested on
  Dec-2022/2023 (defaults 2023–24, never seen): **FM 100M full 0.8468 ROC / 0.0175 AP beats
  XGB 0.7913 / 0.0057** (ROC +0.056, AP 3.1×). Scaling story: 26M full 0.8257 · 65M params-only
  FLAT 0.8223 · 26M-on-10% (data-only) 0.8406 → data is the dominant lever (DL-015). Mode
  ladder at 26M: frozen 0.7309 < LoRA 0.8068 < full 0.8257. Crisis stress (2000-06→2008-10):
  FM 0.7819/0.0248 vs XGB 0.757/0.024. Prepay (honest negative): 0.6259 — macro-rate-driven.
  Benign window (no regime shift): features win narrowly, as expected.
- **Pipeline validated end-to-end**: unit tests + artifact validators at every stage
  (`validate_{ingest,splits,dataset,scores}`), incl. negative controls. 4% sample proven
  REPRESENTATIVE vs the 100% book (pooled default 0.671% vs 0.648%).
- **v1.1 complete:** G1 dataset contract+adapters · G2 declarative labels · G3 streaming data
  path · G4 resume/DDP/logger · G5 packaging (1.1.0.dev0, lean deps, CI wheel job) + docs ·
  G6 calibration (score→PD, embargo-guarded) + FastAPI serving example.
- **Open:** research paper (#14), tech-report human read-through (#17), HF weights publish
  (deferred), model/data-card pass, optional 100%-corpus run (G3 enables it).
  **v1.1 science queued:** multi-objective pretraining (next-period heads), numeric
  value-embeddings, macro context — see internal PLAN.md.
- **Scale-out data path (v1.1 G3):** ingest is sharded + resumable (one `part-<quarter>.parquet`
  per source; rerun skips completed quarters); `prepare_data --stream true` splits any-size
  panels in two streamed passes into `<split>/bucket-<k>/` loan-hash dirs; `encode_dataset`
  auto-detects buckets and encodes one at a time — the 100% corpus (~3.3B rows) fits the box.

## Data (none committed — all gitignored)
- **Mortgage reference:** GCS `gs://sriram-credit-fm-data` — raw Hive-partitioned source →
  `scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml` writes the sharded panel
  with derived `origination_date`, `reporting_date`, `default_event` (D180 or Zero-Balance
  credit event), `prepay_event`, `is_performing`. Auth via `GOOGLE_APPLICATION_CREDENTIALS`
  (`/workspace/.gcloud/credit-fm-sa.json`) + `gcsfs` — note: the container's Arrow build has
  **no native GCS**; always read `gs://` through gcsfs/storage helpers.
- **Dutch mortgages (validation):** `data/raw/all_cutoffs.parquet` (500k loans × 24 monthly
  cutoffs, 71 ESMA Annex 2 cols; HF `Algoritmica/green-lion-2024-2025`). No origination column —
  the split derives origination = `reporting_date − seasoning_months` (DL-007).
- **Latents (eval-only):** `data/raw/loan_book.parquet` has `_segment` etc. — **NEVER model
  features**; ceiling validation only.
- **Splits:** `prepare_data.py -c configs/mortgage_performance/prepare.yaml` →
  `{train,val,test}.parquet + splits.csv + splits.meta.json`; reference splits: `run_2000_2024`
  (4%) and `run_2000_2022_10pct` (10% — the headline lineage), both reporting_max 2022-12-31.
  Always validate with `validate_splits.py`.

## Conventions
- Python 3.10+. **`ruff`** clean + **`pytest`** green before every commit (ruff lints notebooks
  too — one statement per line in generated cells). Type hints on public APIs; Google-style
  docstrings. Every file: SPDX header + `Copyright (c) 2026 finevals.ai`.
- **Leakage rules** (critical for credit): split by `loan_id` (never row); temporal by
  origination; vocab/bins fit on `train` only (DL-008); evaluation is calendar-OOT with
  loan-disjoint + embargo guards. source leakage = current delinquency / zero-balance /
  foreclosure-disposition / loss columns (see `configs/mortgage_performance/baseline.yaml`); Dutch
  leakage = the 8 contemporaneous-state columns. The honest baseline drops them and gates to
  performing-at-observation.
- **Two-layer validation per stage:** unit tests (logic, synthetic) + an artifact validator
  that re-derives the produced output (`scripts/validate_*.py`); validators must FAIL on
  corrupted input (negative control).
- **Schema configs:** `classify_schema.py` enforces the dataset contract's leakage/exclude
  lists (`configs/<asset>/dataset.yaml`) BEFORE classification (v1.1 G1.3, verified on the real
  254M-row split). The Mortgage `tokenizer.yaml` keeps a **documented** review layer on top:
  slice-superset fields, semantic role overrides (`original_ltv`/`dti` are structurally dynamic
  in the raw data), and the human-set bins/anchors. Tasks are declarative too (v1.1 G2.1):
  `dataset.yaml labels:` + `task.label` in finetune recipes.
- **Notebooks are generated** — edit `notebooks/build_*.py`, rerun it; never hand-edit `.ipynb`.

## Dev workflow (IMPORTANT)
- **All git happens on the H100 container**, not locally. The user drives edits/commits there.
- **GitHub is the hub**: branch → PR → merge. Don't commit to `main` directly.
- Container bring-up: `bash scripts/setup_container.sh` (restart-proof venv under `/workspace`;
  see `docs/container_setup.md`). Secrets in `/workspace/secrets.env` (never committed).
- `git` has no pager on the box if `core.pager=cat` is set; otherwise press `q` to exit pagers.

## Internal trackers (NOT in this repo)
Project plan, backlog, dated progress, deliverables manifest, and weekly status live **outside
the repo** in `_credit-fm-internal/` (sibling folder, never pushed; tracker of record is the
Project Manager xlsx there). This `CLAUDE.md` is the public-repo handoff; the internal folder
is the planning source of truth.

## Gotchas learned
- `.gitignore` must anchor data rules (`data/*`, not bare `data/`) — a bare `data/` once
  silently dropped the entire `src/credit_fm/data/` module from commits.
- The container's Arrow build lacks GCS: `pd.read_parquet("gs://…")` raises
  `ArrowNotImplementedError` — read via gcsfs (see `validate_splits.py`).
- mortgage loan_ids are numeric-looking strings: a CSV round-trip coerces them to int — always
  compare ids as `str`.
- The synthetic Dutch panel is rule-based, so baselines run high; report the honest (gated,
  no-leakage) number and the segment-ceiling context, not the inflated one.
- NGC PyTorch image ships without `ensurepip`; the venv needs `--system-site-packages` and
  `pythonX.Y-venv` — both handled by `setup_container.sh`. Never `pip install` RAPIDS/cuDF over
  the image's pinned numpy/pandas (it broke the venv once; GPU tokenizer engine parked).
- nullable-boolean labels: `default_event`/`is_performing` are pandas `boolean` (NA from
  unknown delinquency); every consumer must `.fillna(False)`.
- Per-quarter ingest shards disagree on ALL-NULL columns (arrow `null` type in year-2000
  quarters vs `string` later) — a naive directory scan dies with "Unsupported cast from string
  to null". `storage.read_parquet` / `streaming` unify fragment schemas (fix #111); any new
  parquet-dir reader must go through them.
