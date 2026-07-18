# Part 17 — Framework Design: From One Model to a Framework

> The v1.1 generalization program (G1–G6, all merged). Design record:
> `docs/decision_log.md` + the per-brick PRs (#90–#109).

## 17.1 Why "becoming a framework" was a deliberate project

v1.0 proved the science on Fannie Mae — but the code *knew* it was Fannie: column names in
scripts, one hardcoded label, RAM-bound data handling, single-GPU training, results only
reproducible on one box. The v1.1 program extracted six seams, each shipped as an independently
tested brick. The test of success: **onboarding a new dataset or task touches YAML, not
`src/`** — and a build-breaking test (`test_asset_blind.py`) enforces it forever.

## 17.2 G1 — the dataset contract + adapters

**The contract** (`configs/<asset>/dataset.yaml`) is the single onboarding artifact — identity
columns, declarative `labels:`, and the machine-enforced `exclude:`/`leakage:` lists. Loaded by
`dataset_config.py` into typed objects (`DatasetConfig`, `LabelSpec`) with rules like "every
label's event/gate column MUST be in the leakage list" checked at load time.

**Adapters** (`data/adapter.py`) are the code half — the *only* place raw-source knowledge lives:

```
DatasetAdapter protocol:  sources() · load_panel()          (+ optional, for resumable ingest:
                                                              load_source(src) · source_tag(src))
   ├─ GenericParquetAdapter    your panel already conforms → ZERO code onboarding
   └─ @register_adapter("fannie_mae") FannieMaeAdapter      lives in reference_implementations/,
                                                            resolved LAZILY BY NAME — src/ never
                                                            imports asset code
```

*Why a registry with lazy import?* The core package stays asset-blind even while stock scripts
"just work" from the repo root; a new adapter is one decorated class, discoverable by config
string. The Dutch-mortgages configs are the standing proof: a 71-column ESMA schema with no
origination column runs the identical scripts, YAML-only.

## 17.3 G2 — the label abstraction (task adapters, in effect)

Tasks used to be code (`default_event` hardwired in `finetune.py`). Now a task is a **LabelSpec**
resolved from the contract (`task.label: default_12m`), evaluated by one generic function
(`forward_event_entities`: did `event_col` fire within `horizon_months` after the cutoff, for
loans passing `gate_col ∈ gate_values`?). The proof-by-construction: the prepayment model was
**one YAML line**, and the gate generalization (`gate_values` beyond booleans) means a
cure-prediction task — gate on *delinquent* loans — is config too. Equivalence gate at merge:
the declarative path reproduced the legacy headline to 3 decimals (0.8260 ≈ 0.8257).

## 17.4 G3 — streaming (the RAM ceiling removed)

Two bricks so the 3.3B-row corpus fits a single box:

- **Sharded resumable ingest** — one `part-<quarter>.parquet` per source, sidecar-after-shard as
  the completion marker; a hard kill costs one quarter (Part 6).
- **Streaming split + bucketing** — the subtle one. Streaming rows into per-split files isn't
  enough, because *the encoder needs whole loans* and raw shards are time-partitioned (one loan
  scattered over ~40 quarters). The fix is a loan-hash **shuffle**:

```
pass 1  stream ONLY (id, origination) columns → per-loan min, hierarchical reduce
        → temporal_loan_split assignment (IDENTICAL to in-RAM: proven by test)
pass 2  stream every row → route to  <split>/bucket-<hash(loan)%K>/part-<batch>.parquet
        → every bucket holds WHOLE loans; RAM bounded by batch size, never panel size
encode  auto-detects bucket-*/ dirs, encodes one bucket at a time (worker pool unchanged)
```

`validate_splits` audits both layouts unchanged. Memory now scales with `rows/K`, tunable.

## 17.5 G4 — trainer maturity (resume · DDP · logging)

- **Resume** — step checkpoints carry *complete* state (model+optimizer+scheduler+history+RNG);
  `--resume auto`; a multi-day run survives anything (Part 12.8).
- **DDP** — 8×H100 via `torch.distributed.run`; DistributedSampler sharding, `no_sync` under
  accumulation, rank-0-only I/O behind barriers, `find_unused_parameters` for the idle head.
  Parity-tested against single-GPU.
- **Pluggable metrics logger** — `logging.backend: null | jsonl | tensorboard | wandb`. The
  default is byte-identical stdout; jsonl is the zero-dependency, crash-safe workhorse; wandb is
  opt-in **and offline by default**. This closed DL-009, the sovereign-cloud requirement:
  *nothing phones home unless explicitly configured* — for bank deployments that's a feature
  with a compliance department attached.

## 17.6 G5 + G6 — packaging, docs, and the last mile

- **Packaging**: honest dependencies (core = only what's imported: torch/numpy/pandas/pyarrow/
  pyyaml/fsspec/scikit-learn; extras `[gcs] [baselines] [logging] [serving] [dev]`), version
  single-sourced, a 4-symbol top-level API, CI job that builds the wheel, installs it clean, and
  runs a toy pretrain against it. De-hardcoded key/bucket via `CREDIT_FM_GCS_KEY` /
  `CREDIT_FM_BUCKET`. `test_packaging.py` fails if a heavy dep creeps back into core.
- **Docs**: `docs/configuration.md` + `docs/extending.md` — an outsider onboards from docs alone.
- **Calibration + serving** (Part 15): the framework ends at a usable PD and a reference HTTP
  service, not at a metric.

## 17.7 Why these abstractions matter — the composition test

Each seam multiplies the others. "Score prepayment risk on a new bank's book, at full scale, on
their air-gapped hardware" decomposes into: adapter or generic contract (G1) + one label line
(G2) + streaming (G3) + resumable DDP training with local jsonl logs (G4) + `pip install
credit_fm` offline (G5) + calibrated serving (G6). None of those sentences mentions editing the
framework.

**Future extensibility — where the seams already point:**
- new *label semantics* (e.g. time-to-event) → a new `LabelSpec.type` beside `forward_event`
- multi-task fine-tuning (default+cure heads) → the fine-tune loop, backbone untouched
- macro covariates (HPI, rates) → just more `event:` fields in the contract; tokenizer handles it
- new architecture experiments → behind `CreditFoundationModel`'s constructor; the data layer's
  four-array contract is the stable interface
- deliberately rejected (and why): a *pluggable encoder zoo* — reviewed and declined to keep one
  validated architecture instead of four shallow ones; revisit only with evidence.

### Things to remember

1. Six seams (G1–G6): contract+adapters, declarative labels, streaming, trainer maturity, packaging, calibration+serving.
2. New dataset = one YAML contract (+ optionally one adapter class); new task = one label line.
3. Streaming's trick is the loan-hash bucket shuffle: whole loans per bucket, RAM bounded by batch size.
4. DL-009: nothing phones home unless explicitly configured — a compliance feature, not a preference.
5. The seams compose: a full new-bank deployment touches zero framework code.

---
*Next: [Part 18 — Experiments](18_experiments.md): how results stay reproducible and honest.*
