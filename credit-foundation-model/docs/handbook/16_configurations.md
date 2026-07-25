# Part 16 — Configurations: The YAML Engine

> File: `src/credit_fm/utils/config.py` (~180 lines, PyYAML only — no hydra/omegaconf).
> Public reference: `docs/configuration.md`. Every stage speaks one grammar:
> `python scripts/<stage>.py -c configs/<asset>/<stage>.yaml [--key.path value …]`

## 16.1 Why config files run the show

Hard-coded parameters make experiments unrepeatable ("which LR was that run?"). Argparse walls
make them illegible. A **recipe file** makes the experiment itself an artifact: diffable,
reviewable, and — because every output embeds `cfg.to_dict()` — recoverable from any checkpoint
or manifest. The config *is* the experiment record (Part 18 builds on exactly this).

## 16.2 The three features

**1. Inheritance — `include:`** (defaults live in one place)

```yaml
# configs/mortgage_performance/encode.yaml
include: common.yaml          # deep-merged; THIS file's keys win; a list of includes is allowed
split: train
input: ${paths.processed}/${split}.parquet
```

Deep-merge means nested dicts merge key-by-key — override `model.dim` without restating
`model.n_heads`. Every mortgage recipe includes `common.yaml`, which defines paths **once**:

```yaml
# configs/mortgage_performance/common.yaml (the hub)
run_name: run_2016_2017
gcs_root: gs://sriram-credit-fm-data
dataset: configs/mortgage_performance/dataset.yaml      tokenizer: configs/mortgage_performance/tokenizer.json
paths:
  processed: ${gcs_root}/output/processed/mortgage_performance/${run_name}
  encoded:   ${gcs_root}/output/encoded/mortgage_performance/${run_name}
  runs:      ${gcs_root}/runs
seed: 42
```

**2. Interpolation — `${a.b}`**: any string can reference another key by dotted path. Two forms:
a **whole-string** reference (`batch: ${data.batch_size}`) substitutes the *value with its type*
(int stays int, null stays null); an **embedded** reference does string substitution. Chains
resolve iteratively (up to 10 passes); a missing key raises `KeyError: interpolation '${…}' not
found` — fail-fast, never a silent empty string.

**3. CLI overrides — dotted paths**: everything after `-c` is parsed as overrides:

```bash
python scripts/pretrain.py -c configs/mortgage_performance/pretrain_100m.yaml \
    --run_name run_2000_2022_10pct --schedule.steps 2000 --data.limit null --runtime.bf16
```

Values are **YAML-parsed** — `null`, `true`, `0.5`, `[1,2]` arrive typed (`--origination_col
null` is Python `None`, not the string "null"); `--key=value` and bare flags (`--runtime.bf16` →
true) work; intermediate dicts are created as needed.

## 16.3 Precedence and the worked example (memorize this resolution)

```
include files  <  the recipe's own keys  <  CLI overrides   …then interpolation runs LAST
```

Interpolation-after-overrides is the killer feature: `--run_name run_2000_2024` re-points
*every* derived path in one flag —

```
1. include:  pretrain.yaml pulls common.yaml (run_name: run_2016_2017, paths…)
2. override: run_name ← run_2000_2024                        (CLI beats include)
3. interp:   paths.encoded → gs://…/encoded/mortgage_performance/run_2000_2024
4. interp:   data.train_dir = ${paths.encoded}/train → gs://…/run_2000_2024/train
```

One flag switched the entire pipeline to a different data run. This is *the* intended workflow.

## 16.4 In code

`parse_cli()` returns a `Config` — a dict subclass with attribute access:

```python
cfg.model.dim                          # missing key → AttributeError listing available keys
cfg.get_path("schedule.grad_accum", 1) # dotted lookup with default — for OPTIONAL keys
cfg.to_dict()                          # plain nested dict → embedded in every artifact
```

House style: required keys via attributes (loud failure), optional keys via `get_path`
(explicit default at the use site). One normalization quirk handled for you: YAML parses bare
ISO dates into `datetime.date`; the engine coerces them back to strings so configs stay
JSON-serializable in manifests.

## 16.5 Tour of the recipe families (what to look for in each)

| Recipe | The keys that matter |
|---|---|
| `dataset.yaml` | **the contract** (Part 17): columns, `labels:`, `exclude:`, `leakage:` |
| `ingest*.yaml` | `sources.root/reporting`, `sample_pct`, `sharded/combine`, `workers` |
| `prepare.yaml` | `input`, `origination_col` (null = derive), `fractions`, `reporting_max`, `stream/buckets` |
| `classify.yaml` / `tokenizer_fit.yaml` | schema proposal in/out; fit uses the TRAIN split path |
| `tokenizer.yaml` | the curated field schema: profile/event lists, `bins:`, `anchors:` |
| `encode.yaml` | `split` (note `input: ${paths.processed}/${split}.parquet` — one recipe, three splits via `--split`), `shard_size`, `workers`, `engine` |
| `pretrain*.yaml` | `model:`, `optimizer:`, `schedule:` (incl. `grad_accum`), `checkpoint:` (out/every/keep), `logging:` (backend null/jsonl/tensorboard/wandb-offline), `resume` |
| `finetune*.yaml` | `checkpoint`, `task:` (`label`, cutoffs), `mode`, `lora:`, `train:` (lr/epochs/neg_per_pos/pos_weight_cap), `save`, `report` |
| `scoring.yaml` / `calibrate.yaml` | `cutoff`, `gate`, `calibrator` / `method`, `test_cutoffs` (the refusal list), `out` |

Variant recipes are thin: `finetune_prepay_oot.yaml` is `include: finetune_oot.yaml` plus
`task.label: prepay_12m` and a couple of training tweaks — inheritance doing its job.

## 16.6 Common mistakes

- Editing `common.yaml` to switch runs (it changes *everything*, including colleagues' recipes)
  — override `--run_name` instead.
- `cfg.some_optional_key` crashing on older recipes — optional means `get_path` with a default.
- Forgetting quotes in shell lists: `--task.test_cutoffs '[2022-12-31, 2023-12-31]'`.
- Assuming env vars interpolate — they don't; the two env knobs are `CREDIT_FM_GCS_KEY` (storage
  auth) and `CREDIT_FM_BUCKET` (run_*.sh), read by code, not by the YAML engine.

### Things to remember

1. Precedence: include < recipe < CLI overrides — and interpolation runs LAST.
2. `--run_name X` re-points every derived path in the pipeline: the intended workflow.
3. Override values are YAML-parsed (typed); required keys via attributes, optional via `get_path(default)`.
4. `cfg.to_dict()` embedded in every artifact makes the config the experiment record.

---
*Next: [Part 17 — Framework Design](17_framework_design.md): the abstractions that made this dataset-agnostic.*
