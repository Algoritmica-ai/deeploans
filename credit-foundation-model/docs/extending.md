# Extending to a New Asset Class

Onboarding a dataset is **configuration, not code**: one contract file
(`configs/<asset>/dataset.yaml`) declares what the framework needs to know, and the same stock
scripts run the whole pipeline. Code is only needed if your raw source requires bespoke parsing
— and then it is one adapter class, outside the core package.

The guided, runnable version of this page is
[`notebooks/05_new_dataset.ipynb`](../notebooks/05_new_dataset.ipynb).

## 1. The dataset contract — `configs/<asset>/dataset.yaml`

The single onboarding artifact. Everything downstream reads it
(`credit_fm.data.dataset_config.load_dataset_config`); nothing re-declares it.

```yaml
dataset:
  name: my_asset
  adapter: generic                 # 'generic' = your panel already conforms (zero code)
  id_col: loan_id                  # entity id — ALWAYS handled as string
  time_col: reporting_date         # ISO 'YYYY-MM-DD' month-end string
  origination_col: origination_date  # temporal-split key (or origination_derived: true
                                     # to derive it as reporting − seasoning months)

labels:                            # declarative task targets — tasks are CONFIG, not code
  default_12m:
    type: forward_event            # did event_col fire within the horizon after the cutoff?
    event_col: default_event       # boolean event column in the panel
    horizon_months: 12
    gate_col: is_performing        # observe only entities in this state at the cutoff

exclude:                           # structural non-features: ids, raw dates, free text, geo
  - servicer_name
leakage:                           # outcome / contemporaneous-state columns — the no-peek list;
  - default_event                  # every label event/gate column MUST be listed here
  - is_performing
```

The panel it describes is **one row per entity-month**. The `leakage:` list is machine-enforced:
`classify_schema.py` drops those columns *before* proposing the tokenizer schema, and the
`validate_dataset.py` auditor fails if one leaks through.

## 2. Two onboarding paths

**Path A — `adapter: generic` (no code).** Your parquet already has the contract columns.
Point the pipeline straight at it; ingest is skipped entirely.

**Path B — a custom adapter (one class).** Raw source needs parsing/derivations? Put them in
`reference_implementations/<asset>/adapter.py` — *outside* `src/credit_fm`, which imports no
asset-specific code ever (a test enforces this):

```python
from credit_fm.data.adapter import register_adapter

@register_adapter("my_asset")
class MyAssetAdapter:
    def __init__(self, config, *, stage): ...
    def sources(self) -> list[str]: ...            # raw inputs (recorded for lineage)
    def load_panel(self) -> pd.DataFrame: ...      # contract-conforming panel

    # optional — enables sharded, RESUMABLE ingest (one shard per source, rerun skips
    # completed ones; how the 100% Fannie corpus is ingested):
    def load_source(self, source) -> pd.DataFrame: ...
    def source_tag(self, source) -> str: ...       # unique shard tag, e.g. '2016Q1'
```

`reference_implementations/fannie_mae/adapter.py` is the worked example (~140 lines: column
derivations, date parsing, hash sampling).

## 3. The pipeline — same scripts, your recipes

Each stage is one script + one YAML recipe (see [`configuration.md`](configuration.md) for the
`include:`/`${...}`/`--key.path` grammar). With `configs/my_asset/` in place:

```bash
python scripts/ingest.py          -c configs/my_asset/ingest.yaml      # path B only; resumable
python scripts/validate_dataset.py --dataset configs/my_asset/dataset.yaml --panel <panel>
python scripts/prepare_data.py    -c configs/my_asset/prepare.yaml     # loan-disjoint temporal split
python scripts/validate_splits.py --dir <out_dir>                      # artifact audit
python scripts/classify_schema.py -c configs/my_asset/classify.yaml    # leakage dropped FIRST,
                                                                       # then field routing proposed
python scripts/train_tokenizer.py -c configs/my_asset/tokenizer_fit.yaml   # TRAIN split only
python scripts/encode_dataset.py  -c configs/my_asset/encode.yaml      # encode-once shards
python scripts/pretrain.py        -c configs/my_asset/pretrain.yaml    # MLM pretraining
python scripts/finetune.py        -c configs/my_asset/finetune.yaml    # task.label: default_12m
```

Guardrails you get for free: splits are by entity (never row) and temporal by origination;
vocabulary/bins fit on train only; each stage pairs unit tests with an **artifact validator**
that re-derives the produced output and fails on corrupted input.

`validate_dataset.py` audits the contract against the real panel (checks A–G): contract columns
present, string ids, ISO month-end dates, one row per (id, time), label event/gate domains,
tokenizer schema free of leakage/exclude columns, gate/terminal-state consistency.

## 4. Adding a task = one YAML block

Because labels are declarative, a new task touches zero code. The prepayment task on Fannie is,
in full:

```yaml
# dataset.yaml — the label definition
  prepay_12m:
    type: forward_event
    event_col: prepay_event
    horizon_months: 12
    gate_col: is_performing

# finetune_prepay.yaml — the recipe that uses it
task:
  label: prepay_12m
```

Same split, same OOT protocol, same reports — different target.

## 5. Scale knobs (when the panel outgrows RAM)

- **Ingest** writes one shard per source as each completes; a killed run **resumes** by rerunning
  the same command (completed sources are skipped).
- **Split**: `prepare_data --stream true` never loads the panel — two streamed passes write
  `<split>/bucket-<k>/` loan-hash directories (whole entities per bucket).
- **Encode** auto-detects bucketed inputs and encodes one bucket at a time.
- **Pretrain** on all GPUs:
  `PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 scripts/pretrain.py -c <recipe>`.

## 6. Worked examples

- **Fannie Mae** (`configs/fannie_mae/` + `reference_implementations/fannie_mae/`) — full
  path-B reference: custom adapter, 113-column raw layout, 44 leakage + 15 exclude columns.
- **Dutch mortgages** (`configs/dutch_mortgages/`) — a completely different schema (ESMA
  Annex 2, 71 columns, no origination column → derived) running through identical scripts;
  the delta is YAML only.
