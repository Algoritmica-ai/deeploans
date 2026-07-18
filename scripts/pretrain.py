# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Pretrain the credit foundation model (MLM) over encode-once shards.

Loads a frozen ``tokenizer.json`` (for ``vocab_size`` + ``n_field_types``), builds a
``CreditDataModule`` over the encoded shard directories, constructs ``CreditFoundationModel``, and
runs ``train_mlm``. The checkpoint stores the model config (for reload) and the full resolved
run config (lineage).

Config-driven (recipe: ``configs/fannie_mae/pretrain.yaml``)::

    python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml                # single GPU
    PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 \
        scripts/pretrain.py -c configs/fannie_mae/pretrain_100m.yaml             # 8-GPU DDP (G4b)
    # NB: use `python -m torch.distributed.run`, NOT bare `torchrun` — under a venv the bare command
    # resolves to the SYSTEM install and spawns system-python workers that lack venv deps (gcsfs).
    python scripts/pretrain.py -c configs/fannie_mae/pretrain.yaml \
        --data.limit 1000 --schedule.steps 100 --model.dim 256 \
        --checkpoint.out runs/toy.pt                                              # toy run
"""

from __future__ import annotations

import os

# reduce CUDA fragmentation for long-sequence O(L²) attention — must be set BEFORE torch inits CUDA.
# (FlashAttention/SDPA already avoids the big score matrix; this is belt-and-suspenders for headroom.)
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

# DDP construction lazily imports torch._dynamo -> torch.onnx -> onnx, and the NGC image's onnx is
# built against protobuf < 3.20 (the '_pb2' descriptors fail under protobuf 4.x). Pure-Python
# protobuf parsing sidesteps it (documented workaround). Must be set BEFORE any protobuf import.
os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")

import fsspec  # noqa: E402
import torch  # noqa: E402

from credit_fm.data import CreditDataModule  # noqa: E402
from credit_fm.models import CreditFoundationModel  # noqa: E402
from credit_fm.tokenizer import KVTTokenizer  # noqa: E402
from credit_fm.training import train_mlm  # noqa: E402
from credit_fm.training.loggers import build_logger  # noqa: E402
from credit_fm.training.distributed import cleanup_distributed, init_distributed  # noqa: E402
from credit_fm.utils import storage  # noqa: E402
from credit_fm.utils.config import parse_cli, summarize  # noqa: E402
from credit_fm.utils.reproducibility import set_seed  # noqa: E402


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/pretrain.yaml")
    info = init_distributed(cfg.get_path("runtime.device"))     # no-op single-GPU (G4b)
    device = cfg.get_path("runtime.device")
    if info.is_distributed and torch.cuda.is_available():
        device = f"cuda:{info.local_rank}"

    def rprint(*a, **k):                                        # only rank 0 prints
        if info.is_main:
            print(*a, **k)

    if info.is_distributed:
        rprint(f"DDP: {info.world_size} ranks ({info.backend}); this=rank {info.rank} "
               f"on {device}", flush=True)
        # Fast-fail the classic launcher trap: bare `torchrun` spawns SYSTEM-python workers that
        # lack the venv's gcsfs → gs:// reads explode 8× deep. One clear line instead.
        import importlib.util
        if (str(cfg.get_path("data.train_dir", "")).startswith("gs://")
                and importlib.util.find_spec("gcsfs") is None):
            raise SystemExit(
                "gcsfs is not importable in this worker interpreter — the DDP workers are not using "
                "the venv. Relaunch with the venv's python:\n"
                "  PYTHONPATH=src python -m torch.distributed.run --standalone --nproc_per_node 8 "
                "scripts/pretrain.py -c <recipe>\n"
                "(bare `torchrun` resolves to the system install and spawns system-python workers).")
    rprint(f"config: {cfg.config_path}\n"
           f"{summarize(cfg, 'data', 'model', 'optimizer', 'schedule', 'runtime', 'checkpoint')}",
           flush=True)
    set_seed(cfg.seed + info.rank)                             # per-rank dropout/masking streams

    tok = KVTTokenizer.load(cfg.tokenizer)
    vocab_size, n_field_types = tok.vocab_size, len(tok.field_types)
    rprint(f"tokenizer: {vocab_size} tokens, {n_field_types} field types", flush=True)

    dm = CreditDataModule(cfg.data.train_dir, val_dir=cfg.data.val_dir, vocab_size=vocab_size,
                          batch_size=cfg.data.batch_size, num_workers=cfg.data.num_workers,
                          limit=cfg.data.limit, key=cfg.key)
    rprint(f"data: {len(dm.train)} train loans"
           + (f", {len(dm.val)} val loans" if dm.val is not None else ""), flush=True)

    m = cfg.model
    model = CreditFoundationModel(
        vocab_size, n_field_types, dim=m.dim, n_heads=m.n_heads,
        profile_layers=m.profile_layers, event_layers=m.event_layers,
        history_layers=m.history_layers, dropout=m.dropout)
    rprint(f"model: {model.num_parameters()/1e6:.1f}M params (dim={m.dim}, dropout={m.dropout})",
           flush=True)

    grad_accum = int(cfg.get_path("schedule.grad_accum") or 1)
    eff = cfg.data.batch_size * grad_accum * info.world_size
    if grad_accum > 1 or info.is_distributed:
        rprint(f"effective batch {eff} (micro {cfg.data.batch_size} × accum {grad_accum} "
               f"× {info.world_size} ranks)", flush=True)
    ckpt_every = int(cfg.get_path("checkpoint.every") or 0)
    if ckpt_every:
        rprint(f"step checkpoints: every {ckpt_every} steps, keep {cfg.get_path('checkpoint.keep', 2)}"
               f" (resume with --resume auto)", flush=True)
    mlog = build_logger(cfg.get_path("logging")) if info.is_main else None    # rank-0 only (G4c)
    if mlog is not None and cfg.get_path("logging.backend"):
        rprint(f"metrics logger: {cfg.get_path('logging.backend')}", flush=True)
        mlog.log_config(cfg.to_dict())
    history = train_mlm(
        model, dm, steps=cfg.schedule.steps, lr=cfg.optimizer.lr,
        weight_decay=cfg.optimizer.weight_decay, warmup=cfg.optimizer.warmup,
        grad_clip=cfg.optimizer.grad_clip, grad_accum=grad_accum,
        device=device, bf16=cfg.runtime.bf16,
        log_every=cfg.schedule.log_every, val_every=cfg.schedule.val_every,
        checkpoint_every=ckpt_every, checkpoint_keep=int(cfg.get_path("checkpoint.keep", 2) or 2),
        checkpoint_out=cfg.get_path("checkpoint.out"), resume=cfg.get_path("resume"),
        dist_info=info, ddp_find_unused=bool(cfg.get_path("ddp.find_unused_parameters", True)),
        metrics_logger=mlog)

    first, last = history["train"][0], history["train"][-1]
    msg = f"done: train loss {first:.4f} -> {last:.4f} over {cfg.schedule.steps} steps"
    if history["best_val"] is not None:
        msg += f"  | best val {history['best_val']:.4f} @ step {history['best_step']}"
    rprint(msg, flush=True)

    out = cfg.get_path("checkpoint.out")
    if out and info.is_main:                                       # only rank 0 writes the final model
        ckpt = {
            "model": model.state_dict(),
            "config": {"vocab_size": vocab_size, "n_field_types": n_field_types, "dim": m.dim,
                       "n_heads": m.n_heads, "profile_layers": m.profile_layers,
                       "event_layers": m.event_layers, "history_layers": m.history_layers},
            "run_config": cfg.to_dict(),                           # lineage
            "tokenizer": cfg.tokenizer, "steps": cfg.schedule.steps, "history": history,
        }
        storage.ensure_auth(out, cfg.key)
        with fsspec.open(out, "wb") as f:
            torch.save(ckpt, f)
        rprint(f"saved checkpoint -> {out}", flush=True)

    cleanup_distributed()


if __name__ == "__main__":
    main()
