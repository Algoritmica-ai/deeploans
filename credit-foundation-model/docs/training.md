# Training

MLM pretraining over encode-once token shards. The loop is a purpose-built single-file trainer
(`src/credit_fm/training/trainer.py` — `train_mlm`), not a framework wrapper: AdamW with
warmup-cosine decay, bf16 autocast, gradient clipping, dropout, periodic deterministic
validation, and **best-val checkpointing** (the saved weights are the best validation step, not
the last).

## Objective — three masking sources

Hide part of each loan's sequence, predict it (`src/credit_fm/training/masking.py`):

| Strategy | Rate | Hides | Forces the model to learn |
|---|---|---|---|
| token | 15% | individual `field=value` tokens | local field↔value structure |
| event | 10% | a whole month's block | temporal dynamics (History encoder) |
| type | 10% | one field across all months | cross-field structure (Event encoder) |

Corruption is BERT-style (80% `[MASK]` / 10% random / 10% unchanged); the 9 structural specials
are never masked. Train masking is dynamic (fresh each batch); val/test masking is seeded and
deterministic so losses are comparable across epochs.

## Running it

```bash
python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml
# dotted overrides, e.g.:
python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml --steps 12000 --batch_size 128
```

Reads shard dirs (local or `gs://`) via `CreditDataModule`; vocab size comes from the shard
manifest. Read the loss the honest way: train and val falling *together* = generalising; a
widening gap = memorising (at small data this is expected — see DL-015: the ~26M model needs
roughly Chinchilla-scale tokens, ~500M+, to generalise).

**MLM loss is a proxy, not the verdict.** The model is gated on the downstream out-of-time
evaluation (see `evaluation.md`), never on pretrain loss alone.

## Fine-tuning (adaptation ladder)

`scripts/finetune.py` attaches a classification head to the pretrained backbone and trains at
three freeze levels — **frozen** (head only), **LoRA** (r=8, α=16 adapters), **full** — under
rare-event stabilizers: train-side negative downsampling (`neg_per_pos`), capped class weight
(`pos_weight_cap`), per-epoch true-balance validation ROC, and best-epoch restore. Test sets
are never downsampled.

## Current scale (v1.1)

Both single-GPU and **8-GPU DDP** (v1.1 G4b) are working configurations — launch with
`PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8
scripts/pretrain.py -c <recipe>` (never bare `torchrun`; it resolves to system python).
Effective batch = micro × `schedule.grad_accum` × world size; gradient sync defers to the last
micro-batch (`no_sync`), and validation/checkpointing/logging are rank-0-only. FlashAttention
(SDPA) is on by default in `models/base.py`.

Long runs are **resumable** (v1.1 G4a): `checkpoint.every` writes full-state step files
(model+optimizer+scheduler+RNG, rotated by `keep`); `--resume auto` continues from the newest —
a crash costs at most one checkpoint interval.

Metrics logging is pluggable (v1.1 G4c, **resolves DL-009**): `logging.backend:
null | jsonl | tensorboard | wandb`. The default is byte-identical stdout; jsonl is the
zero-dependency crash-safe workhorse; wandb is strictly opt-in and offline by default —
nothing phones home unless explicitly configured (the sovereign-cloud constraint, honored).
