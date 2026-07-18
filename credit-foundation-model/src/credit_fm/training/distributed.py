# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Distributed-data-parallel (DDP) helpers — multi-GPU training (v1.1 G4b).

Thin wrappers around ``torch.distributed`` so the training loop scales from one GPU to all eight
with **no change to the single-GPU path**. Everything keys off the ``torchrun`` environment
(``WORLD_SIZE`` / ``RANK`` / ``LOCAL_RANK``); when ``WORLD_SIZE <= 1`` (or the env is absent) every
function is a no-op returning single-process values, so a plain ``python scripts/pretrain.py`` run
behaves exactly as before.

Launch a multi-GPU run with::

    torchrun --standalone --nproc_per_node 8 scripts/pretrain.py -c configs/fannie_mae/pretrain_100m.yaml

Backend is NCCL on CUDA (the fast GPU collective) and gloo on CPU (used by the smoke test).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import torch
import torch.distributed as dist


@dataclass(frozen=True)
class DistInfo:
    """Snapshot of the distributed context (single-process defaults)."""

    rank: int = 0
    world_size: int = 1
    local_rank: int = 0
    backend: str = ""

    @property
    def is_distributed(self) -> bool:
        return self.world_size > 1

    @property
    def is_main(self) -> bool:
        return self.rank == 0


def init_distributed(device: str | None = None) -> DistInfo:
    """Initialise the process group from the torchrun env; no-op single-process when ``WORLD_SIZE<=1``.

    On CUDA this also pins the process to its ``LOCAL_RANK`` GPU. Returns a :class:`DistInfo`
    describing the context (``world_size == 1`` means "not distributed").
    """
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if world_size <= 1:
        return DistInfo()
    rank = int(os.environ.get("RANK", "0"))
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    use_cuda = torch.cuda.is_available() and (device is None or str(device).startswith("cuda"))
    backend = "nccl" if use_cuda else "gloo"
    if not dist.is_initialized():
        dist.init_process_group(backend=backend, rank=rank, world_size=world_size)
    if use_cuda:
        torch.cuda.set_device(local_rank)
    return DistInfo(rank=rank, world_size=world_size, local_rank=local_rank, backend=backend)


def cleanup_distributed() -> None:
    """Tear down the process group (safe to call unconditionally)."""
    if dist.is_available() and dist.is_initialized():
        dist.destroy_process_group()


def unwrap(model):
    """Return the underlying module (strips a ``DistributedDataParallel`` wrapper if present)."""
    return model.module if hasattr(model, "module") else model


def barrier() -> None:
    """Synchronise all ranks (no-op single-process)."""
    if dist.is_available() and dist.is_initialized():
        dist.barrier()


def all_reduce_mean(value: float, device: str) -> float:
    """Average a scalar across ranks; returns ``value`` unchanged when not distributed."""
    if not (dist.is_available() and dist.is_initialized()):
        return value
    t = torch.tensor([value], device=device, dtype=torch.float64)
    dist.all_reduce(t, op=dist.ReduceOp.SUM)
    return (t / dist.get_world_size()).item()
