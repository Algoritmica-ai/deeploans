# Part 2 — Big Picture Architecture

## 2.1 The pipeline at one glance

Every box below is **one script** in `scripts/`, driven by **one YAML recipe** in `configs/`,
followed (where it matters) by a **validator** that audits what the box produced.

```
                       ┌──────────────────────────────────────────────────────┐
                       │              RAW DATA  (Fannie Mae, GCS)              │
                       │  hive dirs: reporting_year=2016/reporting_quarter=Q1  │
                       └────────────────────────┬─────────────────────────────┘
                                                ▼
      ┌──────────────┐   ingest.py + FannieMaeAdapter: parse dates, derive labels,
      │ 1. INGEST    │   hash-sample loans, write one shard per quarter (resumable)
      └──────┬───────┘        artifact: panel/  (part-2016Q1.parquet …)
             ▼
      ┌──────────────┐   validate_ingest.py / validate_dataset.py:
      │ 2. VALIDATE  │   re-derive every derived column, check the contract
      └──────┬───────┘        artifact: PASS/FAIL report (exit code gates the run)
             ▼
      ┌──────────────┐   prepare_data.py: loan-disjoint, temporal split by origination;
      │ 3. PREPARE   │   optional --stream true for corpora bigger than RAM
      └──────┬───────┘        artifact: {train,val,test}.parquet + splits.csv + meta
             ▼                (then validate_splits.py audits it)
      ┌──────────────┐   classify_schema.py (propose fields, leakage dropped FIRST)
      │ 4. TOKENIZE  │   train_tokenizer.py (fit vocab + bins on TRAIN ONLY)
      └──────┬───────┘        artifact: tokenizer.json  (552 tokens, frozen)
             ▼
      ┌──────────────┐   encode_dataset.py: every loan → token ids, ONCE
      │ 5. ENCODE    │   (worker pool; bucket-aware for streamed splits)
      └──────┬───────┘        artifact: shard-*.parquet + manifest.json
             ▼
      ┌──────────────┐   pretrain.py: masked-language-model training;
      │ 6. TRAIN     │   1 GPU or 8-GPU DDP; step checkpoints + resume
      └──────┬───────┘        artifact: m_100m.pt  (weights + config + history)
             ▼
      ┌──────────────┐   the .pt file IS the product of pretraining:
      │ 7. CHECKPOINT│   weights + model config + the full resolved run config
      └──────┬───────┘   (lineage: every artifact records what produced it)
             ▼
      ┌──────────────┐   extract_embeddings.py: loans → [USR] vectors (cached);
      │ 8. INFERENCE │   evaluate_downstream.py: probes on frozen embeddings
      └──────┬───────┘
             ▼
      ┌──────────────┐   finetune.py --mode frozen|lora|full
      │ 9. FINE-TUNE │   task from dataset.yaml labels: (default_12m, prepay_12m…)
      └──────┬───────┘        artifact: m_100m_ft.pt + a markdown report
             ▼
      ┌──────────────┐   calendar-OOT protocol; train_baseline / build_oot_baseline
      │10. EVALUATE  │   provide the XGBoost bar; validate_scores audits score files
      └──────┬───────┘
             ▼
      ┌──────────────┐   score_portfolio.py (batch, + calibrator → PDs)
      │11. DEPLOY    │   calibrate.py (isotonic/Platt) · serve.py (FastAPI example)
      └──────────────┘
```

## 2.2 Why the stages are separated

**Plain English:** the same reason a restaurant doesn't have one person shop, cook, serve, and do
dishes simultaneously: when something goes wrong, you need to know *which step* went wrong, and
you'd like to redo only that step.

**The concrete engineering reasons:**

1. **Cost asymmetry.** Ingest touches ~4B raw rows (hours). Encoding is CPU-days at full scale.
   Pretraining is GPU-days. Fine-tuning is GPU-hours. If these lived in one program, any crash —
   or any change to the fine-tune — would repay the whole bill. Separated, each stage writes a
   durable artifact and the next stage starts from it. Changing the fine-tune re-runs *only* the
   fine-tune.

2. **Auditability.** Credit modelling has a failure mode unique among ML fields: **leakage**
   silently produces spectacular fake results (Part 8). The defense is inspectable boundaries: a
   split you can audit *as a file* (`validate_splits.py` proves train/test loans are disjoint),
   an encoded corpus you can decode back, a tokenizer whose bins you can read in JSON.

3. **Encode once (DL-014's sibling decision).** The model revisits every loan hundreds of times
   during pretraining. Tokenizing on the fly would starve the GPUs; tokenizing once into shards
   makes epoch N+1 free. That's only possible if tokenize/encode are their own stage.

4. **Independent scaling.** Ingest is network-bound (thread pool), encoding is CPU-bound (process
   pool, 64 workers), pretraining is GPU-bound (DDP over 8×H100). One process can't be shaped for
   all three.

## 2.3 Why one script per stage (and not a monolith or a DAG engine)

Each stage is a plain Python script with a uniform grammar:

```bash
python scripts/<stage>.py -c configs/<asset>/<stage>.yaml [--key.path value ...]
```

- **Plain scripts** beat a workflow engine (Airflow etc.) at this scale of team: nothing to
  operate, `nohup` + logs for long runs, and each script is independently testable — the test
  suite runs the *real scripts* on synthetic data via `subprocess` and asserts on their artifacts.
- **Config-driven** beats argparse walls: the *recipe file* is the experiment record. Every
  artifact embeds `cfg.to_dict()` — you can reconstruct any run from its outputs (Part 18).
- **Uniformity is a feature:** once you've run one stage, you can run all eleven.

## 2.4 The two-layer safety net

Every consequential stage ships with **two kinds of tests** (a repo convention worth internalizing
before you write code):

```
   unit tests  (tests/test_*.py)           artifact validators (scripts/validate_*.py)
   ─ prove the LOGIC is right              ─ prove the PRODUCED FILES are right
   ─ synthetic data, fast, run in CI       ─ re-derive outputs from inputs, PASS/FAIL
   ─ e.g. "does the bucketer put           ─ e.g. "are train and test loan sets
      LTV 80.1 in the right bin?"             disjoint IN THESE ACTUAL PARQUETS?"
                     └──────── both must include NEGATIVE CONTROLS ────────┘
                       (corrupt the artifact on purpose → the validator MUST fail;
                        a validator that can't fail is decoration, not defense)
```

Why both? Unit tests can't see operational mistakes (you pointed the config at last month's
split; a partial file survived a crash). Validators can't localize logic bugs. Together they
cover each other's blind spots.

## 2.5 Where the framework abstractions sit

Three seams make the pipeline dataset-agnostic (deep dive in Part 17):

- **The dataset contract** (`configs/<asset>/dataset.yaml`) — columns, labels, and the
  machine-enforced leakage list. Everything downstream reads it; nothing re-declares it.
- **The adapter** (`reference_implementations/<asset>/adapter.py`) — the only place raw-source
  parsing lives. The core package `src/credit_fm/` imports **no asset-specific code, ever**
  (a test fails the build if it does).
- **The label layer** (`labels:` in the contract + `task.label` in recipes) — prediction tasks
  are configuration, not code.

### Things to remember

1. Eleven stages; each is one script + one YAML recipe, writing a durable artifact the next stage starts from.
2. Separation exists for cost asymmetry (redo only the broken stage), auditability, and independent scaling.
3. Two-layer safety net: unit tests prove the logic; artifact validators prove the produced files — both with negative controls.
4. Three seams make it dataset-agnostic: the contract (dataset.yaml), the adapter, the label layer.

---
*Next: [Part 3 — Project Structure](03_project_structure.md): what lives in which folder, and why.*
