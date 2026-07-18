# Part 22 — Debugging the Pipeline

> The chapter you'll open months from now at 11pm. Organized by **symptom**, each with causes
> in order of likelihood, the diagnosis that discriminates between them, and the fix. The quick
> table lives in Part 19 (T13); this is the full decision-tree version. General rule first:
>
> **Believe the validators. Run the relevant `validate_*` before debugging anything else —
> half of all "model bugs" are data bugs wearing a costume.**

## 22.1 Training loss explodes or goes NaN

```
loss: 5.9 → 3.2 → 47.1 → NaN
        │
        ├─ Did it explode in the first ~100 steps?
        │     └─ YES → LR too high for this mode, or warmup skipped.
        │              Diagnose: the log prints lr each log_every — was it already ≥1e-4
        │              while the head was still random? Fine-tune full at 3e-4 is the classic.
        │              Fix: mode-default LRs (full 2e-5); confirm warmup: 1000 in the recipe.
        │
        ├─ Sudden single-step spike late in training?
        │     └─ One pathological batch. grad_clip: 1.0 should have caught it —
        │        is it set? If clipping is on and spikes persist → suspect the data:
        │        decode the offending batch (tok.decode) and look for garbage rows.
        │
        ├─ NaN from step 1?
        │     └─ Architecture/precision bug: fully-masked attention row (softmax of all -inf),
        │        or fp16 somewhere (we use bf16 for exactly this). Diagnose on CPU with a tiny
        │        config: torch.autograd.set_detect_anomaly(True) names the op that made the NaN.
        │
        └─ After you changed masking/model code?
              └─ A token attends to nothing. event_block_additive_mask always allows the
                 diagonal for this reason — did your change break that invariant?
```

## 22.2 Loss flat at ~6.3 and never moves

ln(552) ≈ 6.3 = random guessing → **the model sees no signal**, almost always a wiring bug, in
order: labels all −100 (masking rates zeroed in config? `mask=False` collator used for
training?); labels misaligned with ids (custom collator change); LR effectively 0 (scheduler
misconfigured — print `sched.get_last_lr()`); frozen parameters you didn't mean to freeze
(`sum(p.requires_grad for p in model.parameters())`). Discriminator: overfit **one batch** for
200 steps (`--data.limit 64`). A healthy setup drives loss near 0 on one batch; if it can't,
the bug is in the loop, not the data.

## 22.3 Train loss falls, val loss rises (or val is absurdly noisy)

Rising = overfitting: raise `dropout`, more data, fewer steps — and remember the artifact
restores *best*-val weights, so the damage may already be contained. Noisy-val-across-epochs =
you lost deterministic val masking (the val collator takes a `seed`; an unshuffled loader
matters too). Val *lower* than train = not a bug: dropout is off in eval and val masking may be
easier — compare trends, not levels.

## 22.4 CUDA out of memory

```
OOM at step 0        → batch simply too big: halve data.batch_size, double schedule.grad_accum
                       (same effective batch, half the activation memory). This is the exact
                       trade that trained the 100M model.
OOM mid-run          → fragmentation: PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
                       (pretrain.py sets it; export it for ad-hoc scripts).
OOM in eval/scoring  → someone dropped torch.no_grad(), or scoring bsz too large.
OOM only under DDP   → per-rank batch didn't shrink: effective = micro × accum × world_size.
Where did it die?    → the traceback names the module; ours OOM'd in SwiGLU's w_up —
                       activations, not attention (SDPA already avoids the L² matrix).
```

## 22.5 DDP: hangs, crashes, or wrong numbers

| Symptom | Cause → fix |
|---|---|
| `ModuleNotFoundError: gcsfs` ×8 | bare `torchrun` = system python → `PYTHONPATH=src python -m torch.distributed.run …` (pretrain.py fast-fails with this exact message) |
| `Descriptors cannot be created directly` | NGC protobuf clash → `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` (set by pretrain.py; export elsewhere) |
| "Expected to have finished reduction … parameters not used" | a head got no gradients (classification head during MLM) → `find_unused_parameters: true` (our default) |
| Hangs forever at a "random" step | rank divergence: someone does I/O or an early `continue` on rank 0 only without a `barrier()` — every rank must reach every collective |
| Loss ≠ single-GPU loss | it won't be bit-equal (reduction order); compare *curves*. Parity test: 2-proc gloo run in `test_distributed.py` |
| Duplicate log lines ×8 | prints outside the `rprint`/rank-0 guards |

## 22.6 Data loading: GPU idle, or loader crashes

GPU util sawtoothing to 0% → loader-bound: raise `num_workers`, check you're reading GCS shards
(network) vs local cache. Worker crashes with pickling/deadlock errors → you forked after gRPC
init; the encode pool uses **spawn** for exactly this — keep it. `pd.read_parquet("gs://…")`
raising `ArrowNotImplementedError` → this pyarrow build lacks native GCS; always go through
`storage.read_parquet`. `Unsupported cast from string to null` on a shard **directory** →
per-quarter shards disagree on all-null column types (a field empty in 2000, populated in
2016); the storage/streaming readers unify fragment schemas automatically (fix #111) — any
raw pyarrow scan of a shard dir must do the same. Transient SSL/OAuth failures hours in →
`storage.retry` handles known markers; a *new* transient marker belongs in `_TRANSIENT_MARKERS`.

## 22.7 Corrupt tokenizer / checkpoint-shape mismatches

`load_state_dict` errors are self-describing if you read them: `size mismatch for
embeddings.token.weight: ckpt (552, 768) vs model (552, 384)` = you rebuilt with the wrong
config — **always rebuild from `ckpt["config"]`**, never from a recipe. Missing
`lora_A/lora_B` keys = LoRA checkpoint without re-inserting adapters first (`load_finetuned`
does the order right). Nonsense predictions with no error = tokenizer/checkpoint from different
lineages — same vocab size by luck, different token↔id map. Diagnose: `tok.decode(input_ids)`
of one sample and *read* it; garbage reads as garbage. Lineage answers live inside artifacts:
`torch.load(ckpt)["run_config"]`.

## 22.8 Scores look wrong

```
All scores ≈ identical     → model in train mode (dropout wobble averaged out?) — model.eval();
                             or the head never trained (frozen run with lr=0 — check epoch logs)
ROC ≈ 0.5 on a labeled set → labels miswired: wrong task block, gate filtering everyone out
                             (print the positive count per cutoff — finetune.py already does),
                             or scoring a cutoff with no outcomes yet
ROC suspiciously HIGH      → leakage until proven otherwise: validate_splits (disjoint?),
                             validate_dataset check F (a leakage column in the schema?),
                             and confirm the cutoff-truncation test still passes
Mean score ≫ base rate     → not a bug: rebalanced training (Part 8.4). Calibrate (Part 15).
Scores differ batch vs API → they can't, structurally — if they do, the server is running a
                             different checkpoint/calibrator: GET /health and compare paths
```

## 22.9 Resume didn't resume

"nothing to resume — cold start" = no step files match `<checkpoint.out>.step*.pt`: you changed
`checkpoint.out` between runs (resume looks *next to the output path*), or `checkpoint.every`
was 0. Resumed loss jumps slightly = expected (dataloader stream restarts — documented
approximation); resumed loss jumps *a lot* = you resumed against different data/config —
compare `run_config` in the step file vs your recipe.

## 22.10 The universal five-step loop

1. **Validators first** (data bugs wear model costumes).
2. **Shrink it**: `--data.limit 1000 --schedule.steps 50 --checkpoint.out runs/toy.pt` — a
   5-minute repro beats a 5-hour one; overfit-one-batch discriminates loop-vs-data.
3. **Look at the data the model saw**: decode a sample; print shapes; say the shape sentence
   (Part 10½).
4. **Read the artifact's lineage**, not your memory: `run_config` inside the checkpoint.
5. **When fixed, encode the lesson**: a regression test (the DDP unused-head bug has one) or a
   row in this chapter.

### Things to remember

1. Validators before debuggers — always.
2. Loss ≈ 6.3 flat means "no signal reaching the model," not "model too small."
3. OOM has a standard first move: halve batch, double accumulation.
4. Every DDP mystery is either the wrong python, a rank-divergent branch, or an unused parameter.
5. Suspiciously good numbers are leakage until three checks say otherwise.

---
*Next: [Part 23 — Credit FM vs LLM](23_credit_fm_vs_llm.md).*
