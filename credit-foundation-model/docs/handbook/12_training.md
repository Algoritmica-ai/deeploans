# Part 12 — Training: One Step, Tensor by Tensor

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ split ─▶ tokenize ─▶ encode ─▶ [PRETRAIN] ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ serve


> Files: `src/credit_fm/training/` — `trainer.py` (`train_mlm`), `masking.py`, `optimizers.py`,
> `distributed.py`, `loggers.py` · driver `scripts/pretrain.py`.

## 12.1 The loop from orbit

```
for step in 1..20000:
    for j in 1..grad_accum:                       # 4 micro-batches
        batch  = next(shard stream)               # 64 loans, padded+masked by the collator
        loss   = model(batch)["loss"] / accum     # FORWARD
        loss.backward()                           # BACKWARD (gradients accumulate)
    clip_grad_norm_(params, 1.0)
    optimizer.step(); scheduler.step()            # UPDATE + LR schedule
    every 50 steps: log train loss/lr             # (+ jsonl/tensorboard/wandb if configured)
    every 1000:     eval on val (deterministic masking) → track BEST state
                    write resumable step-checkpoint, rotate old ones
restore best-val weights; save final checkpoint
```

Vocabulary: a **step** = one optimizer update (the unit pretraining thinks in). An **epoch** =
one full pass over the data (the unit fine-tuning thinks in; pretraining just cycles the shard
stream — `_cycle` — and counts steps). **Early stopping** here is the gentle form: we don't halt,
but the *best*-val weights are what we keep, so late overfitting can't hurt the artifact.

## 12.2 What a batch looks like (real shapes, 2-loan toy)

The collator (`MLMCollator`) pads every loan to the batch max and applies masking:

```
input_ids       (B=2, L=11)   [[ 1, 5,217, 7,63,412, 88, 8, 7,63, 3],     ← 3 = [MASK]!
                               [ 1, 5,198, 7,64,412, 90, 8, 0, 0, 0]]     ← 0 = [PAD]
attention_mask  (2, 11)       [[ 1,1,1,1,1,1,1,1,1,1,1],
                               [ 1,1,1,1,1,1,1,1,0,0,0]]                  ← 1 real, 0 pad
labels          (2, 11)       [[-100,…,-100, 71],                          ← original id under the mask;
                               [-100,…      ]]                               -100 = "don't grade here"
event_index / field_type / branch   (2, 11)  each, padded with -1
n_events        (2,)          [2, 1]
```

## 12.3 Masking — the exam generator (`masking.py`)

Three selection strategies, **unioned**, each aimed at a different branch:

```
token_rate 0.15   coin-flip per token            → local field/value structure (Event/Profile)
event_rate 0.10   hide ENTIRE months             → forces temporal inference (History branch:
                                                    "reconstruct May from April and June")
type_rate  0.10   hide one FIELD across ALL months → forces cross-field inference
                                                    ("infer the whole balance path from rate+age")
```

Selected positions get BERT's 80/10/10 corruption: 80% → `[MASK]`, 10% → a *random field token*
(never a special), 10% left unchanged — so the model can't learn the shortcut "masked slot ⇒
literally [MASK]" and must stay suspicious of every token. Specials (ids 0–8) are never masked.
Masking is **dynamic** (fresh randomness each time a loan is seen — RoBERTa-style, so 20k steps
see 20k different exams), except validation, which uses a **seeded** generator so val loss is
comparable across epochs.

## 12.4 Forward pass — following the tensors

```
(B,L) ids ─Embeddings─▶ (B,L,768) ─Profile──▶ profile_vec (B,768)  + profile token states
                                  ─Event────▶ event_vecs (B,E,768) + event token states
                                  ─History──▶ loan_emb (B,768), hist_event_ctx (B,E,768)
MLM head: concat[ local(B,L,768) ‖ segment(B,L,768) ‖ loan(B,L,768) ] ─▶ logits (B,L,552)
```

(The `torch.where`/`gather` gymnastics in `credit_fm.py::forward` are just routing: each token
picks its own branch's output as "local," its month's history-context as "segment.")

## 12.5 Loss — cross-entropy with an ignore list

**Plain:** at each masked slot the model outputs 552 scores; softmax turns them into
probabilities; the loss is `−log P(correct token)` — confident-and-right ≈ 0,
confident-and-wrong ≫ 1.

`F.cross_entropy(logits.view(-1,552), labels.view(-1), ignore_index=-100)`: every unmasked and
padded position carries label −100 and contributes *nothing*. The model is graded only on the
blanks. Calibration of expectations: random guessing = ln(552) ≈ 6.3 — which is why train loss
starting at ~6.5 and ending at **0.14** (val 0.33) tells you the model went from "no idea" to
"substantially knows the grammar."

## 12.6 Backward pass and the update

- **`loss.backward()`** — backpropagation: the chain rule, automated. Autograd recorded every
  operation in the forward pass; backward replays the tape in reverse computing
  ∂loss/∂parameter for all ~100M parameters. Cost ≈ 2× forward; memory: all forward activations
  are kept (this — specifically SwiGLU's `w_up` activations — is what OOM'd at batch 256, and
  why gradient accumulation exists: 4×64 keeps activations at 64-loan scale while gradients
  *sum* to the 256-loan gradient; the `/accum` in the loss makes it a proper mean).
- **`optimizer.step()`** — AdamW: per-parameter adaptive steps using running averages of
  gradient (momentum, β₁=0.9) and squared gradient (scale, β₂=0.95), plus decoupled weight
  decay. Plain SGD with one global step size simply doesn't train transformers well.
- **`scheduler.step()`** — the warmup-cosine curve from Part 11 moves the LR.
- Under **bf16 autocast**, forward/backward run in bfloat16 while master weights and the
  optimizer stay fp32 — free speed on H100s with no loss-scaling machinery.

## 12.7 Validation and the best-checkpoint rule

Every `val_every` steps: run the val loader (no grad, eval mode, deterministic masking), record
val loss; if it's the best so far, deep-copy the weights to CPU. At the end, **the best-val
weights are restored** into the final checkpoint. The printed `*best` markers in the log are
this mechanism talking.

## 12.8 Checkpoints & resume (v1.1 G4a — read before any long run)

Every 1000 steps, `<out>.step<N>.pt` captures the **complete** training state: model, optimizer
(Adam's momentum!), scheduler, loss history, best-val tracking, and all RNG states
(python/numpy/torch/cuda). `keep: 2` rotates older ones. `--resume auto` finds the newest and
continues at step N+1 — a test proves resumed-vs-uninterrupted runs match to the RNG state.
One documented approximation: the shuffled dataloader position restarts (statistically neutral
for MLM over a cycled corpus). Practical consequence: **a crash at step 19,000 costs ≤ 1000
steps.** Also practical: *always* pass a scratch `--checkpoint.out` for test runs — a parity
test once overwrote a real backbone.

## 12.9 Multi-GPU (v1.1 G4b) — the same loop, eight times wider

`PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 scripts/pretrain.py -c …`
(never bare `torchrun`: it resolves to system python and the workers lose the venv). Each of 8
processes owns one GPU and 1/8 of each epoch (`DistributedSampler`); after each backward, DDP
**all-reduces** (averages) gradients so every rank steps identically — mathematically one big
batch of `64×4×8 = 2048`. Details that bite: gradient sync is deferred to the last micro-batch
(`no_sync`); only rank 0 validates/checkpoints/logs (others wait at a `barrier()`); and
`find_unused_parameters=True` because the idle classification head has no grads during MLM —
without it DDP errors with "parameters not used in producing loss."

## 12.10 Watching a run (what the numbers should do)

```
step 50/20000    loss 5.9412  lr 1.50e-05     ← warmup climbing; loss off the ~6.3 ceiling
step 1000/20000  loss 1.8...  [val] 1.7 *best ← fast phase: easy structure (field grammar)
step 8000/20000  loss 0.4...                  ← slow phase: the long tail of hard patterns
step 20000/20000 loss 0.14    | best val 0.328 @ step 19000
```

Pathologies: loss **NaN/explodes** → LR too high or a poisoned batch (clip should catch; check
data); loss **flat at ~6.3** → labels or masking broken (model sees no signal); **train ≪ val,
val rising** → memorization (raise dropout, more data); val loss noisy across epochs → you
forgot the seeded val masking (the collator's `seed`).

### Things to remember

1. One step = accumulate micro-batch gradients → clip → AdamW step → LR schedule tick.
2. Masking is the exam: 15% tokens ∪ 10% whole months ∪ 10% whole fields, BERT-80/10/10 corrupted, fresh every batch.
3. Calibrate expectations: random = ln(552) ≈ 6.3; this run ended train 0.14 / val 0.33.
4. Step checkpoints carry model+optimizer+scheduler+RNG: `--resume auto` makes a crash cost ≤ 1000 steps.
5. DDP: rank-0-only I/O behind barriers, find_unused_parameters for the idle head, never bare `torchrun`.

---
*Next: [Part 13 — Fine-Tuning](13_fine_tuning.md): cashing in the pretrained knowledge.*
