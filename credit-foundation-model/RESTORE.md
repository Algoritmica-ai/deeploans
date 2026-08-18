# Restore guide

How to pick this project up from nothing and get back to a working state. Every claim here was verified
by running it on 18 Aug 2026: clean clone, full test suite, weights pulled from object storage and run.

## Where everything lives

| What | Where |
|---|---|
| Code — framework, scripts, configs, tests, docs, paper | this repository |
| Artifacts — weights, and the data at every pipeline stage | the project's object-storage bucket |
| Curated archive — the same artifacts, organised and documented | archive bucket, `README.md` at its root |
| Published models | Hugging Face (private repository) |

The archive bucket is the canonical backup: numbered folders in pipeline order, `01_models/` through
`08_rescued_local_files/`, with a README explaining each. Its own README is the best starting point if you
are restoring artifacts rather than code.

## Get running

```bash
git clone <this repo> && cd credit-foundation-model
pip install -e ".[gcs,baselines,dev]"

export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
PYTHONPATH=src pytest -q     # expect: 221 passed, 2 skipped
ruff check .                 # expect: All checks passed!
```

Both gates were confirmed green on a fresh clone. If either fails, stop and fix that before anything else —
the pipeline assumes a working package.

## Loading a model

Checkpoints are self-describing: each carries its architecture config, and fine-tuned ones also carry the
task definition and the metrics they achieved. You never have to guess what a file is.

```python
import torch, fsspec
from credit_fm.utils import storage
from credit_fm.models import CreditFoundationModel

url = 'gs://<bucket>/runs/m_100m_rr_ft.pt'
storage.ensure_auth(url)
with fsspec.open(url, 'rb') as f:
    ck = torch.load(f, map_location='cpu')

cfg = ck['config']
model = CreditFoundationModel(
    cfg['vocab_size'], cfg['n_field_types'], dim=cfg['dim'], n_heads=cfg['n_heads'],
    profile_layers=cfg['profile_layers'], event_layers=cfg['event_layers'],
    history_layers=cfg['history_layers'])
model.load_state_dict(ck['model'], strict=True)
model.eval()

ck.get('finetune', {}).get('metrics')   # what this checkpoint scored, and on what task
```

The tokenizer must be the one in this repo — `configs/mortgage_performance/tokenizer.json`, 552 tokens and
45 field types. It is frozen and paired with the weights; a different vocabulary produces silent nonsense
rather than an error.

### Which checkpoint to use

| Checkpoint | What it is | Out-of-time result |
|---|---|---|
| `m_100m_rr.pt` / `m_100m_rr_ft.pt` | 100M backbone (20,000 steps) and its default model | ROC 0.8447 / AP 0.0160 |
| `m_26m_10pct.pt` / `m_26m_10pct_ft.pt` | 26M backbone on the same corpus, and its default model | **ROC 0.8488** / AP 0.0156 |
| `m5_full.pt` / `m5_full_ft.pt` | 26M on the smaller corpus | ROC 0.8406 / AP 0.0145 |
| `m5_65m.pt` | 65M capacity probe — the null result | ROC 0.8223 |

Prefer the reproduction pair (`*_rr*`): they are the checkpoints whose whole lineage can be re-derived.
**`m_100m.pt` is not the published backbone** — it was overwritten by a later short run; see the technical
report §7.5 and §10.

The 26M model matching the 100M one is not a typo. It is the paper's central finding: at this data scale
capacity bought nothing, so the smaller model is the better default unless you specifically need the
100M model's slightly better tail precision.

## Rerunning the pipeline

Stage order, each stage config-driven and each with an auditor that re-derives its own output:

```
ingest → prepare_data → classify_schema → train_tokenizer
       → encode_dataset → pretrain → finetune
validate_{ingest,splits,dataset,scores} audit the stages above
```

Full commands with the exact flags used for the published results are in `docs/technical_report.md` §10.
Recipes live in `configs/<asset>/`; every path is defined once in `common.yaml` and interpolated, so
switching data runs is a single `--run_name` override.

Three things that are easy to get wrong:

- **`classify_schema.py` emits a suggestion, not the production schema.** The tokenizer needs the curated
  `tokenizer.yaml`, which carries `time_field`, bins, regulatory-cliff anchors and calendar settings that
  classification does not produce.
- **`sample_pct` is cast to `int` and applied as `hash % 100`.** Any value below 1 truncates to zero and
  selects *nothing*, with no error — the failure only surfaces stages later as empty splits. Minimum
  useful value is 1.
- **Multi-GPU changes the effective batch.** It is `batch_size × grad_accum × world_size`; to reproduce a
  published run on a different GPU count, hold that product constant rather than copying `batch_size`.

## Reproducing the headline

The full pipeline was re-run end to end from raw data after nine refactoring pull requests. Deterministic
stages matched exactly — identical split loan counts, 2,997,726,396 tokens, an identical
1,782,453-observation evaluation population — and the stochastic endpoint landed inside the pre-registered
run-to-run band. Details in `docs/technical_report.md` §7.5. That section is the best evidence that a
restore is possible at all: it is a restore, performed and documented.

## Credentials

| For | What |
|---|---|
| Object storage | service-account key, exported as `GOOGLE_APPLICATION_CREDENTIALS` |
| Hugging Face | `hf auth login` as a member of the publishing organisation |

The framework never phones home: metrics logging defaults to off, and nothing leaves the machine unless a
logging backend is explicitly configured.

## Housekeeping worth knowing

- Step-rotation checkpoints (`*.step0190*.pt`, `*.step0200*.pt`) are mid-training state for resuming a run.
  They are several GB and can be deleted without losing anything reproducible.
- The archive bucket has versioning enabled, so deletions there are recoverable.
