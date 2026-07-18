# Configuration

Every pipeline script runs from a **YAML recipe** plus optional command-line overrides — one
grammar everywhere:

```bash
python scripts/<stage>.py -c configs/<asset>/<stage>.yaml [--key.path value ...]
```

The engine lives in [`src/credit_fm/utils/config.py`](../src/credit_fm/utils/config.py)
(~180 lines, PyYAML only — no hydra/omegaconf dependency). It has exactly three features:
**includes**, **interpolation**, and **dotted CLI overrides**.

## 1. `include:` — share a base file

A recipe can start from one or more base files:

```yaml
# configs/fannie_mae/encode.yaml
include: common.yaml        # path relative to THIS file; a list is allowed
split: train
input: ${paths.processed}/${split}.parquet
```

Includes are **deep-merged**: nested dicts merge key-by-key, and the including file's own keys
win over the included ones (with a list of includes, later files win over earlier ones). This is
how every stage shares one `common.yaml` — paths, seed, tokenizer location — while overriding
only what the stage needs.

## 2. `${a.b}` — interpolation

Any string may reference another key by dotted path. `configs/fannie_mae/common.yaml` defines
paths **once**:

```yaml
run_name: run_2016_2017
gcs_root: gs://sriram-credit-fm-data
paths:
  processed: ${gcs_root}/output/processed/fannie_mae/${run_name}
  encoded:   ${gcs_root}/output/encoded/fannie_mae/${run_name}
```

Two forms:

- **Whole-string reference** (`batch_size: ${data.batch_size}`) — the referenced *value* is
  substituted, keeping its type (int stays int, null stays null).
- **Embedded reference** (`input: ${paths.encoded}/train`) — string substitution.

References may chain (`paths.encoded` references `gcs_root` and `run_name`); resolution
iterates until nothing changes (up to 10 passes). A reference to a missing key raises
`KeyError: interpolation '${...}' not found` — fail-fast, no silent empty strings.

## 3. Dotted CLI overrides

Everything after `-c recipe.yaml` is parsed as `--key.path value` overrides:

```bash
python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml \
    --model.dim 512 --schedule.steps 2000 --data.limit null --runtime.bf16
```

- `--a.b.c value` and `--a.b.c=value` are equivalent; a bare flag (`--runtime.bf16`) sets `true`.
- Values are **YAML-parsed**, so `null`, `true`, `0.5`, `2020-12-31`, and `[1, 2]` arrive typed —
  `--origination_col null` really is `None`, not the string `"null"`.
- Intermediate dicts are created as needed, so you can set keys the recipe doesn't mention.

## Precedence and resolution order

```
include files  <  the recipe's own keys  <  CLI overrides      … then interpolation runs last
```

Interpolation running **after** overrides is the important part: overriding one upstream key
re-points everything derived from it.

### Worked example — what `${paths.encoded}/train` means

`configs/fannie_mae/pretrain.yaml` says `train_dir: ${paths.encoded}/train`. Running

```bash
python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml --run_name run_2000_2024
```

resolves in four steps:

1. **Include** — `pretrain.yaml` pulls `common.yaml`, so the config now holds `run_name`,
   `gcs_root`, and the `paths:` block, with `run_name: run_2016_2017` from the base file.
2. **Override** — the CLI sets `run_name: run_2000_2024` (beats the include).
3. **Interpolate, pass 1** — `paths.encoded` = `${gcs_root}/output/encoded/fannie_mae/${run_name}`
   → `gs://sriram-credit-fm-data/output/encoded/fannie_mae/run_2000_2024`.
4. **Interpolate, pass 2** — `train_dir` = `${paths.encoded}/train`
   → `gs://sriram-credit-fm-data/output/encoded/fannie_mae/run_2000_2024/train`.

One flag re-pointed the entire pipeline at a different data run. That is the intended workflow:
**paths are defined once in `common.yaml`; stages reference them; `--run_name` switches runs.**

## In code

Scripts call `parse_cli()` and get a `Config` — a dict with attribute access:

```python
cfg = parse_cli(__doc__, default_config="configs/fannie_mae/pretrain.yaml")
cfg.model.dim                 # attribute access; missing keys raise with the available keys
cfg.get_path("schedule.grad_accum", 1)   # dotted lookup with a default (for optional keys)
cfg.to_dict()                 # plain nested dict — stored in every checkpoint/manifest (lineage)
```

`cfg.to_dict()` being written into checkpoints and manifests means **every artifact records the
exact resolved config that produced it** — the run is reproducible from the artifact alone.

## Storage locations

Any `input`/`output`/`out_dir` value is a *location*, not a path: a local path, `gs://…`, or
`s3://…` — only the URL scheme changes (see `credit_fm.utils.storage`). GCS credentials come
from the `CREDIT_FM_GCS_KEY` env var (a service-account JSON; `GOOGLE_APPLICATION_CREDENTIALS`
also works), and the experiment shell scripts honor `CREDIT_FM_BUCKET` to re-point the bucket.
