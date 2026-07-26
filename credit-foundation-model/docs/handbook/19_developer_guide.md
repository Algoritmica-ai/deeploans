# Part 19 — Developer Guide: Tutorials

> Prereqs for all tutorials: repo root as cwd; `pip install -e ".[dev,gcs,baselines]"` (container:
> `bash scripts/setup_container.sh`); `CREDIT_FM_GCS_KEY` pointing at a service-account JSON for
> `gs://` work. Long runs: `nohup bash … > logs/run.log 2>&1 &` then `tail -f` (the run_*.sh
> scripts write their own timestamped logs). House rules: `ruff check .` + `pytest` green before
> any commit; branch → PR → merge, never commit `main`.

## T1 — Run ingestion (resumable by construction)

```bash
python scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml \
    --sample_pct 10 --combined_name panel_2000_2024_10pct.parquet
# killed? rerun the SAME command — completed quarters print "skip part-…"
```

Output: `<paths.raw>/panel_2000_2024_10pct/part-<YYYYQ#>.parquet` + sidecars + `_ingest.meta.json`.
Then audit a shard or two:

```bash
python scripts/validate_ingest.py --panel <dir>/part-2016Q1.parquet --sample-pct 10
```

## T2 — Prepare (split) and validate

```bash
python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
    --input <shard-dir-or-parquet> --run_name run_2000_2022_10pct --reporting_max 2022-12-31
python scripts/validate_splits.py --dir gs://…/processed/mortgage_performance/run_2000_2022_10pct
# bigger than RAM?  add:  --stream true --buckets 256      (same assignment, bucketed layout)
```

## T3 — Tokenizer + encode (only when starting a NEW lineage — the reference vocab is frozen)

```bash
python scripts/classify_schema.py -c configs/mortgage_performance/classify.yaml      # propose; human-review into tokenizer.yaml
python scripts/train_tokenizer.py -c configs/mortgage_performance/tokenizer_fit.yaml # fit on TRAIN only
python scripts/encode_dataset.py  -c configs/mortgage_performance/encode.yaml --run_name run_2000_2022_10pct --split train --workers 64
python scripts/encode_dataset.py  -c configs/mortgage_performance/encode.yaml --run_name run_2000_2022_10pct --split val   --workers 64
```

## T4 — Pretrain (single GPU), T5 — Resume, T6 — 8-GPU DDP

```bash
# T4: toy first — ALWAYS a scratch checkpoint path for anything experimental
python scripts/pretrain.py -c configs/mortgage_performance/pretrain.yaml \
    --data.limit 2000 --schedule.steps 200 --checkpoint.out runs/toy.pt
# real run (writes step checkpoints every 1000, keeps 2; jsonl metrics locally):
python scripts/pretrain.py -c configs/mortgage_performance/pretrain_100m.yaml \
    --run_name run_2000_2022_10pct --logging.backend jsonl

# T5: box died at step 13,400? same command + resume:
python scripts/pretrain.py -c configs/mortgage_performance/pretrain_100m.yaml … --resume auto
#   → "resumed from …step013000.pt (continuing at 13001/20000)"

# T6: all 8 GPUs — NEVER bare `torchrun` (system python loses the venv):
PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 \
    scripts/pretrain.py -c configs/mortgage_performance/pretrain_100m.yaml --run_name …
```

## T7 — Fine-tune (the OOT protocol), T8 — Score / calibrate / serve

```bash
python scripts/finetune.py -c configs/mortgage_performance/finetune_oot.yaml --mode full \
    --checkpoint gs://…/runs/m_100m.pt --panel <full-panel> \
    --save gs://…/runs/m_100m_ft.pt --report reports/m_100m_oot_ft_full.md
# expect: per-cutoff obs counts → epochs with val ROC → "=== Fine-tune (full) ===  ROC-AUC …"

# T8: batch score → calibrate (past, NON-test cutoff) → calibrated score → audit → serve
python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml --cutoff 2021-12-31 --out gs://…/cal_scores.parquet
python scripts/calibrate.py       -c configs/mortgage_performance/calibrate.yaml
python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml --calibrator gs://…/calibrator.json
python scripts/validate_scores.py --scores <scores> --labeled-panel <panel> --min-roc 0.80
python reference_implementations/mortgage_performance/serve.py --checkpoint … --calibrator … --port 8000
```

## T9 — Evaluate against the bar

```bash
python scripts/build_oot_baseline.py …        # the XGBoost bar on IDENTICAL windows
python scripts/extract_embeddings.py -c configs/mortgage_performance/extract.yaml
python scripts/evaluate_downstream.py -c configs/mortgage_performance/evaluate.yaml   # probes vs features
```

Never quote an FM number without the matching bar from the same windows.

## T10 — Add a dataset (the G1 path; full walkthrough: notebook 05 + docs/extending.md)

1. Write `configs/<asset>/dataset.yaml` — columns, `labels:`, `exclude:`, `leakage:` (every
   label event/gate column must be in `leakage:`).
2. Conforming panel already? `adapter: generic` — **no code**, skip to 4.
3. Else `reference_implementations/<asset>/adapter.py`: one `@register_adapter("<asset>")` class
   with `sources()/load_panel()` (+ `load_source()/source_tag()` for resumable ingest). Unit-test
   the derivations on hand-crafted rows (copy `test_ingest_mortgage_performance.py`'s pattern).
4. `python scripts/validate_dataset.py --dataset configs/<asset>/dataset.yaml --panel <panel>`
5. Copy the recipe family (`common.yaml` first — set paths/`run_name`), then T2→T7 in order.

## T11 — Add a prediction task (the G2 path — usually zero code)

```yaml
# 1. dataset.yaml                                  # 2. finetune_cure.yaml
labels:                                            include: finetune_oot.yaml
  cure_6m:                                         task:
    type: forward_event                              label: cure_6m
    event_col: is_performing
    horizon_months: 6
    gate_col: dlq_num          # gate on DELINQUENT loans at the cutoff
    gate_values: [1, 2, 3]
```

Add both columns to `leakage:` if not already there; run finetune. New label *semantics*
(time-to-event etc.) = a new `type:` in `data/labels.py` — that one is code.

## T12 — Add/modify a model

New shape = same class, new config = new pretrain (T4) under a new checkpoint name. New
*blocks*: keep the data-layer contract (`input_ids/event_index/field_type/branch` + `n_events`)
sacred, extend `models/`, and mirror `test_models.py`/`test_e2e.py` with a tiny-config
end-to-end test. Anything that changes the vocabulary is a new lineage — see Part 18.3.

## T13 — Debugging playbook

| Symptom | First moves |
|---|---|
| Any validator FAILs | Believe it. Read the failing check's detail; fix upstream; re-run stage + validator |
| CUDA OOM | Halve `data.batch_size`, double `schedule.grad_accum` (same effective batch); `expandable_segments` is already set by pretrain.py |
| Loss NaN/explodes | LR too high for the mode? grad_clip on? inspect the last logged lr |
| `gcsfs` import error under DDP | You used bare `torchrun` — relaunch via `python -m torch.distributed.run` |
| `Descriptors cannot be created` (protobuf) | NGC image quirk — pretrain.py sets `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`; export it for ad-hoc scripts |
| `ArrowNotImplementedError` on gs:// | Go through `storage.read_parquet`, never `pd.read_parquet("gs://…")` |
| Scores look "too good" | Leakage until proven otherwise: validate_splits; check F of validate_dataset; confirm the cutoff truncation test still passes |
| val ROC ~0.5 in fine-tune | Labels/gate miswired (check the task block), or LR nuked the backbone |
| Weird single-loan result | Decode it: `KVTTokenizer.load(...)` → `tok.decode(ids)` — read what the model read |
| Reproducing an old run | Don't guess: `torch.load(ckpt)["run_config"]` is the exact recipe |

### Things to remember

1. Toy run first, scratch `--checkpoint.out` always, real run second.
2. The resume story everywhere is 'rerun the same command' (ingest skips, pretrain --resume auto).
3. `PYTHONPATH=src python -m torch.distributed.run …` — bare `torchrun` loses the venv.
4. New dataset → T10 (YAML-first); new task → T11 (usually zero code).
5. Debugging starts at the validators — the full decision trees are Part 22.

---
*Next: [Part 20 — Glossary](20_appendix_glossary.md).*
