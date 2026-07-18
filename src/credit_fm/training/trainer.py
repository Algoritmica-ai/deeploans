# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""MLM training loop for the credit foundation model.

``train_mlm`` is the reusable core (single-GPU/CPU; bf16 autocast on CUDA) used by the M2 toy run
and the M3 pretraining script. It pulls batches from a :class:`CreditDataModule`, optimises the MLM
loss with AdamW + warmup-cosine, clips gradients, and periodically reports validation loss. It
returns a history dict for logging/plotting.

``CreditTrainer`` is a thin object wrapper kept for the scaffold interface; the HF-Trainer / NeMo
backends are deferred to M3.
"""

from __future__ import annotations

import contextlib
import random
import re
from collections.abc import Callable, Iterator

import fsspec
import numpy as np
import torch

from .distributed import DistInfo, barrier
from .optimizers import build_optimizer, build_scheduler


def _cycle(loader) -> Iterator:
    """Infinite pass over ``loader``; reshuffles a ``DistributedSampler`` each epoch (no-op else)."""
    epoch = 0
    sampler = getattr(loader, "sampler", None)
    while True:
        if sampler is not None and hasattr(sampler, "set_epoch"):
            sampler.set_epoch(epoch)
        yield from loader
        epoch += 1


# --------------------------------------------------------------- step checkpoints (v1.1 G4a)
def _rng_states(device: str) -> dict:
    state = {"python": random.getstate(), "numpy": np.random.get_state(),
             "torch": torch.get_rng_state()}
    if device.startswith("cuda") and torch.cuda.is_available():
        state["cuda"] = torch.cuda.get_rng_state_all()
    return state


def _restore_rng(state: dict, device: str) -> None:
    random.setstate(state["python"])
    np.random.set_state(state["numpy"])
    torch.set_rng_state(state["torch"])
    if "cuda" in state and device.startswith("cuda") and torch.cuda.is_available():
        torch.cuda.set_rng_state_all(state["cuda"])


def _step_path(out: str, step: int) -> str:
    return f"{out}.step{step:06d}.pt"


def _save_step(out: str, step: int, model, opt, sched, history, best, device, keep, log) -> None:
    """Write the full resumable state to ``<out>.step<N>.pt`` and rotate old step files."""
    from credit_fm.utils import storage
    best_val, best_step, best_state = best
    payload = {
        "step": step, "model": model.state_dict(), "optimizer": opt.state_dict(),
        "scheduler": sched.state_dict(), "history": history,
        "best_val": best_val, "best_step": best_step, "best_state": best_state,
        "rng": _rng_states(device),
    }
    path = _step_path(out, step)
    storage.ensure_auth(path)
    with fsspec.open(path, "wb") as f:
        torch.save(payload, f)
    log(f"  [ckpt] step {step} -> {path}")
    fs, base = fsspec.core.url_to_fs(out)
    olds = sorted(fs.glob(f"{base}.step*.pt"))
    for old in olds[:-max(keep, 1)]:
        fs.rm(old)


def _find_resume(out: str | None, resume) -> str | None:
    """Resolve the resume source: an explicit path, or ``"auto"`` = newest step file for ``out``."""
    if not resume:
        return None
    if resume != "auto":
        return str(resume)
    if not out:
        return None
    from credit_fm.utils import storage
    storage.ensure_auth(out)
    fs, base = fsspec.core.url_to_fs(out)
    candidates = [p for p in fs.glob(f"{base}.step*.pt")
                  if re.search(r"\.step\d+\.pt$", str(p))]
    if not candidates:
        return None
    newest = max(candidates, key=lambda p: int(re.search(r"\.step(\d+)\.pt$", str(p)).group(1)))
    proto = out.split("://")[0] + "://" if "://" in out else ""
    return proto + str(newest) if proto and "://" not in str(newest) else str(newest)


def _to_device(batch: dict, device: str) -> dict:
    return {k: (v.to(device) if torch.is_tensor(v) else v) for k, v in batch.items()}


@torch.no_grad()
def _evaluate(model, datamodule, device: str, use_amp: bool) -> float:
    was_training = model.training
    model.eval()
    total, n = 0.0, 0
    for batch in datamodule.val_dataloader():
        batch = _to_device(batch, device)
        with torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
            total += model(batch)["loss"].item()
        n += 1
    if was_training:
        model.train()
    return total / max(n, 1)


def train_mlm(model, datamodule, *, steps: int = 100, lr: float = 3e-4, weight_decay: float = 0.01,
              warmup: int = 10, grad_clip: float = 1.0, min_lr_ratio: float = 0.1,
              grad_accum: int = 1, device: str | None = None, bf16: bool = False, log_every: int = 10,
              val_every: int = 0, restore_best: bool = True,
              checkpoint_every: int = 0, checkpoint_keep: int = 2,
              checkpoint_out: str | None = None, resume=None,
              dist_info: DistInfo | None = None, ddp_find_unused: bool = True,
              metrics_logger=None,
              log: Callable[[str], None] = print) -> dict:
    """Train ``model`` for ``steps`` optimiser steps on ``datamodule``; return a loss history.

    ``grad_accum`` micro-batches are summed per optimiser step, so the *effective* batch is
    ``datamodule.batch_size * grad_accum`` while peak memory stays at one micro-batch — the lever
    for training a large model on a single GPU without shrinking the token budget. ``grad_accum=1``
    is the plain loop. When validating, tracks the best (lowest) val loss; if ``restore_best`` the
    model is rolled back to that checkpoint at the end. History carries ``best_val``/``best_step``.

    **Mid-run checkpoint / resume (v1.1 G4a).** With ``checkpoint_every > 0`` (and
    ``checkpoint_out`` set), every N steps the FULL resumable state — model, optimizer, scheduler,
    loss history, best-val tracking, and RNG states — is written to ``<out>.step<N>.pt`` (local or
    ``gs://``), keeping the newest ``checkpoint_keep`` files. ``resume="auto"`` restores the newest
    step file and continues at step N+1; an explicit path resumes from that file; nothing found =
    clean cold start. A crash costs at most ``checkpoint_every`` steps instead of the whole run.
    One documented approximation: the shuffled dataloader stream restarts on resume (its position
    is not checkpointable) — which batches follow differs from an uninterrupted run, statistically
    neutral for MLM pretraining over a cycled corpus.

    **Multi-GPU (v1.1 G4b).** Pass ``dist_info`` from :func:`~credit_fm.training.distributed.
    init_distributed`; when it reports ``world_size > 1`` the model is wrapped in
    ``DistributedDataParallel``, the train loader shards across ranks (``DistributedSampler``), and
    validation / checkpointing / logging happen on **rank 0 only** (guarded by barriers). Gradient
    accumulation composes: syncing is deferred to the last micro-batch (``no_sync``). The effective
    batch is ``batch_size × grad_accum × world_size``. ``dist_info=None`` (or ``world_size == 1``)
    is the single-GPU path, unchanged.
    """
    info = dist_info or DistInfo()
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    use_amp = bf16 and device.startswith("cuda")
    accum = max(int(grad_accum), 1)
    log_main = log if info.is_main else (lambda *a, **k: None)   # only rank 0 speaks
    from .loggers import NullLogger
    mlog = metrics_logger if (metrics_logger is not None and info.is_main) else NullLogger()

    raw_model = model.to(device)
    raw_model.train()
    if info.is_distributed:
        from torch.nn.parallel import DistributedDataParallel as DDP
        ddp_kwargs = {"device_ids": [info.local_rank]} if device.startswith("cuda") else {}
        # find_unused_parameters: the MLM task drives only mlm_head — classification_head's params
        # never get a grad during pretraining, and DDP's reducer errors on unmarked params unless
        # told to detect them. Small per-step overhead, and it auto-adapts if heads change.
        model = DDP(raw_model, find_unused_parameters=ddp_find_unused, **ddp_kwargs)
    else:
        model = raw_model
    opt = build_optimizer(raw_model, lr=lr, weight_decay=weight_decay)
    sched = build_scheduler(opt, warmup_steps=warmup, total_steps=steps, min_lr_ratio=min_lr_ratio)

    history: dict = {"train": [], "val": [], "best_val": None, "best_step": None}
    best_val, best_step, best_state = float("inf"), None, None
    start_step = 1

    src = _find_resume(checkpoint_out, resume)
    if resume and src is None:
        log_main(f"resume: nothing to resume for {checkpoint_out} — cold start")
    elif src:
        from credit_fm.utils import storage
        storage.ensure_auth(src)
        with fsspec.open(src, "rb") as f:
            ckpt = torch.load(f, map_location="cpu", weights_only=False)
        raw_model.load_state_dict(ckpt["model"])
        opt.load_state_dict(ckpt["optimizer"])
        sched.load_state_dict(ckpt["scheduler"])
        history = ckpt["history"]
        best_val, best_step = ckpt["best_val"], ckpt["best_step"]
        best_state = ckpt["best_state"]
        _restore_rng(ckpt["rng"], device)
        start_step = ckpt["step"] + 1
        history["resumed_from"] = ckpt["step"]
        log_main(f"resumed from {src} (step {ckpt['step']}; continuing at {start_step}/{steps})")

    batches = _cycle(datamodule.train_dataloader())
    for step in range(start_step, steps + 1):
        opt.zero_grad(set_to_none=True)
        step_loss = 0.0
        for j in range(accum):                        # accumulate `accum` micro-batches → one step
            batch = _to_device(next(batches), device)
            # DDP all-reduces grads on every backward; defer to the LAST micro-batch of the step
            sync_ctx = (model.no_sync() if info.is_distributed and j < accum - 1
                        else contextlib.nullcontext())
            with sync_ctx, torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
                loss = model(batch)["loss"] / accum   # scale so grads are the mean over micro-batches
            loss.backward()
            step_loss += loss.item()
        if grad_clip:
            torch.nn.utils.clip_grad_norm_(raw_model.parameters(), grad_clip)
        opt.step()
        sched.step()

        history["train"].append(step_loss)
        if log_every and (step % log_every == 0 or step == 1):
            log_main(f"step {step}/{steps}  loss {step_loss:.4f}  lr {sched.get_last_lr()[0]:.2e}")
            mlog.log_metrics(step, {"train/loss": step_loss, "train/lr": sched.get_last_lr()[0]})
        if val_every and datamodule.val is not None and step % val_every == 0:
            if info.is_main:                          # rank 0 evaluates the raw (unwrapped) model
                v = _evaluate(raw_model, datamodule, device, use_amp)
                history["val"].append((step, v))
                star = ""
                if v < best_val:
                    best_val, best_step = v, step
                    best_state = {k: t.detach().to("cpu").clone()
                                  for k, t in raw_model.state_dict().items()}
                    star = "  *best"
                log_main(f"  [val] step {step}  loss {v:.4f}{star}")
                mlog.log_metrics(step, {"val/loss": v})
            barrier()                                 # other ranks wait out rank 0's eval
        if checkpoint_every and checkpoint_out and step % checkpoint_every == 0:
            if info.is_main:
                _save_step(checkpoint_out, step, raw_model, opt, sched, history,
                           (best_val, best_step, best_state), device, checkpoint_keep, log_main)
            barrier()

    if best_state is not None:
        history["best_val"], history["best_step"] = best_val, best_step
        if restore_best:
            raw_model.load_state_dict(best_state)
            log_main(f"restored best checkpoint: val {best_val:.4f} @ step {best_step}")
    mlog.finish({"best_val": history["best_val"], "best_step": history["best_step"],
                 "final_train_loss": history["train"][-1] if history["train"] else None})
    return history


class CreditTrainer:
    """Scaffold wrapper around :func:`train_mlm` (HF/NeMo backends deferred to M3)."""

    def __init__(self, model, config: dict, backend: str = "hf"):
        assert backend in {"hf", "nemo"}
        self.model, self.config, self.backend = model, config, backend

    def train(self, datamodule) -> dict:
        return train_mlm(self.model, datamodule, **self.config)
