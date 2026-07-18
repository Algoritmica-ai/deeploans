# Part 11 — Model Architecture & Every Knob

> Files: `src/credit_fm/models/` — `credit_fm.py` (assembly), `profile_encoder.py`,
> `event_encoder.py`, `history_encoder.py`, `mlm_head.py`, `classification_head.py`.
> Decision: DL-002 (three-branch), frozen since milestone M2.

## 11.1 Why three branches instead of one big transformer

One flat transformer over ~950 tokens *works*, but wastes capacity discovering structure we
already know: loans have **static facts**, **monthly facts**, and **a timeline**. The
architecture bakes that in (PRAGMA-style):

```
        (B, L) token ids + field_type + branch + event_index
                          │
                    Embeddings (sum of 3)
                          │  (B, L, dim)
        ┌─────────────────┼──────────────────────┐
        ▼                                        ▼
  ProfileStateEncoder (3 layers)          EventEncoder (5 layers)
  attends ONLY among profile tokens       attends ONLY within each month
  (branch==0), masked-mean pool           (event_index blocks), masked-mean
        │                                 pool per month
        ▼                                        ▼
  profile_vec (B, dim)                    event_vecs (B, E, dim) + mask
        └────────────────┬───────────────────────┘
                         ▼
              HistoryEncoder (6 layers)
              sequence: [LOAN] profile_vec event₀ … event₅₉   (RoPE = month order)
                         │
        ┌────────────────┴─────────────────┐
        ▼                                  ▼
  loan_embedding (B, dim)           hist_event_ctx (B, E, dim)
  = the [USR] vector                = each month, timeline-aware
        │                                  │
  ClassificationHead              MLMHead( local ‖ segment ‖ loan ) → vocab logits
  (downstream: 2 classes)         (pretraining)
```

**The information bottleneck is the point.** Each month gets squeezed into ONE vector before the
History encoder sees it — forcing the Event encoder to summarize months well and the History
encoder to reason about the *sequence of summaries* (like reading chapter summaries in order,
rather than every word of a novel at once). It also collapses attention cost: History attends
over ~62 positions, not ~950.

**Why intra-month attention masks?** Within a month, `dlq=1` should interact with `upb=…` and
`rate=…` of the *same* month (a coherent snapshot); cross-month reasoning is the History
encoder's job, on pooled vectors. The mask (`event_block_additive_mask`) enforces this division
of labor. Profile tokens get the same treatment as "one segment."

**The `[LOAN]` token** (`history_encoder.py`): a learnable vector prepended to the timeline —
an empty notebook that attention fills with whatever summarizes the loan. Its output *is* the
loan embedding. (Same idiom as BERT's `[CLS]`.) `[LOAN]` and the profile slot are always valid;
absent months are hidden by a padding mask.

**The MLM head's 3-way concat** (`mlm_head.py`): to predict a hidden token, concatenate three
contexts — **local** (the token's own branch-encoder output: within-month structure), **segment**
(its month's *history-contextualized* vector: cross-month/regime info), **loan** (the global
embedding) — then one Linear to 552 logits. This is what makes all three encoders learn during
pretraining: each contributes a distinct, non-redundant slice of evidence.

## 11.2 Every parameter, what it does, and its tradeoff

Model shape (constructor args of `CreditFoundationModel`; values: 26M config / 100M config):

| Knob | 26M / 100M | Plain meaning | Raise it → | Lower it → |
|---|---|---|---|---|
| `dim` | 384 / 768 | width of every vector — the "vocabulary of thought" | capacity ↑, params ~quadratic ↑, memory ↑ | fast but crude representations |
| `n_heads` | 8 / 8 | parallel attention specialists | more relation types per layer; head_dim = dim/heads shrinks (must stay even for RoPE; 96 at dim 768) | fewer, blunter heads |
| `profile_layers` | 3 / 3 | depth on static facts | static fields interact more subtly | profile is nearly linear anyway — 3 is plenty |
| `event_layers` | 5 / 5 | depth within each month | richer month snapshots | months under-summarized — starves History |
| `history_layers` | 6 / 6 | depth along the timeline | deeper temporal composition (the branch that "reads the story") | myopic sequence reasoning |
| `mlp_mult` | 4 | SwiGLU hidden ≈ 8/3·dim·(mult/4) | more per-token processing (most params live here) | — |
| `dropout` | 0.1 (pretrain) | randomly zero activations in training — forces redundancy, fights memorization | more regularization; too high = underfit | 0 for small/toy runs; auto-off in eval |
| `n_classes` | 2 | downstream head width | multi-class tasks | — |

Optimization (`optimizer:`/`schedule:` in `pretrain*.yaml`; machinery in `training/optimizers.py`):

| Knob | Value | What it really does |
|---|---|---|
| `lr` (learning rate) | 3e-4 pretrain; 2e-5 full-FT | step size of every weight update. THE hyperparameter: 10× too high → loss explodes/NaN; 10× too low → nothing happens for days. Full fine-tuning uses ~15× smaller because pretrained weights are already good — big steps would bulldoze them |
| `warmup` | 1000 steps | LR ramps 0 → 3e-4 linearly first. Early gradients (random Adam state, random head) are garbage; warmup keeps them from wrecking the init |
| schedule | cosine → 10% of peak | after warmup, LR glides down a cosine curve (`min_lr_ratio 0.1`): big steps early (explore), small late (settle) |
| `weight_decay` | 0.01 | gently shrinks weights toward 0 each step (AdamW = decay *decoupled* from the gradient). Applied **only to 2-D matrices** — biases, norm gains, embeddings, `[LOAN]` are exempt (`_NO_DECAY_HINTS`), standard LLM practice |
| `grad_clip` | 1.0 | if the whole gradient's norm exceeds 1, rescale it. Insurance against the occasional pathological batch — one bad step can undo hours |
| betas | (0.9, 0.95) | Adam's momentum/variance memory; 0.95 (not the default 0.999) reacts faster — LLM-standard |

Batch & throughput (`data:`/`schedule:`/`runtime:`):

| Knob | Value | Meaning + tradeoff |
|---|---|---|
| `batch_size` | 64 (micro) | loans per forward pass. Bounded by GPU memory |
| `grad_accum` | 4 | sum gradients over 4 micro-batches, then one optimizer step → **effective batch 256** at 64's memory cost. This is how the 100M model trained on one H100 after the OOM. Effective batch = micro × accum × world_size |
| `steps` | 20,000 | optimizer steps. 256 loans × ~500 tokens × 20k ≈ 2.5B tokens ≈ the Chinchilla-ish budget for 100M params (~20 tokens/param) |
| `epochs` | (fine-tune only: ~4–8) | full passes over the labeled set. Pretraining thinks in *steps* over a cycled corpus instead |
| `num_workers` | 4 | background processes feeding batches; too few starves the GPU, too many thrashes RAM |
| `bf16` | true | bfloat16 mixed precision: half-size activations, H100 tensor-core speed, fp32's exponent range (no loss-scaling circus that fp16 needs) |
| `device` | null → auto | cuda if available; DDP overrides to `cuda:<local_rank>` |
| `log_every`/`val_every` | 50 / 1000 | print/metric cadence; val picks the **best checkpoint** (lowest val loss is restored at the end) |
| `checkpoint.every/keep` | 1000 / 2 | mid-run resumable snapshots, rotated (Part 12) |

Fine-tune-specific (`train:` in `finetune*.yaml`): `neg_per_pos` (downsample fit negatives),
`pos_weight_cap` (bound the positive class weight), `lora.rank/alpha` (Part 13),
`split.test_frac` (loan-holdout mode only).

## 11.3 Where the parameters live (100M config)

```
Embeddings: 552×768 tokens + 15×768 field-types + 3×768 branches            ≈ 0.4M
Per block @ dim 768: qkv 3·768² + proj 768² + SwiGLU ≈ 3·768·2048           ≈ 7.2M
× 14 blocks (3+5+6)                                                          ≈ 100M
MLM head: 3·768 × 552                                                        ≈ 1.3M
Classification head: 768 × 2                                                 ≈ 0.001M   ← tiny!
```

The downstream head being ~1.5k parameters is the whole foundation-model economics: the
expensive 100M-parameter reader is trained once; the task-specific part is a rounding error.

## 11.4 Changing the architecture safely

The shape lives in the checkpoint (`ckpt["config"]`), so loaders rebuild exactly what was
trained — but that also means **dim/layers/vocab are frozen per lineage**: you can't "just try
dim 512" against an existing checkpoint. New shape = new pretraining run = new experiment name
(Part 18). This is why the repo froze the architecture at M2 and spent effort on data/scale
instead — and why E10's lesson (65M on unchanged data = flat) was cheap to learn.

### Things to remember

1. Three branches encode structure we already know; the per-month bottleneck (~950→60→1) is deliberate.
2. The `[LOAN]` slot's output IS the loan embedding; the MLM head concatenates local‖segment‖loan.
3. Learning rate is THE hyperparameter; effective batch = micro × accum × world_size.
4. The architecture config lives inside the checkpoint — shape is frozen per lineage; new shape = new pretrain.

---
*Next: [Part 12 — Training](12_training.md): what one step actually does, tensor by tensor.*
