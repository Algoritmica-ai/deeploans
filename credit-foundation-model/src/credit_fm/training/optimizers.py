# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""AdamW with decoupled weight decay + linear-warmup / cosine-decay schedule.

Weight decay is applied to 2-D weight matrices only; biases, norm gains, embeddings, and the
learnable ``[LOAN]`` token are excluded (standard LLM practice).
"""

from __future__ import annotations

import math

import torch

_NO_DECAY_HINTS = ("norm", "embed", "loan_token", "bias")


def build_optimizer(model, lr: float = 3e-4, weight_decay: float = 0.01,
                    betas: tuple[float, float] = (0.9, 0.95)) -> torch.optim.Optimizer:
    decay, no_decay = [], []
    for name, p in model.named_parameters():
        if not p.requires_grad:
            continue
        if p.ndim < 2 or any(h in name.lower() for h in _NO_DECAY_HINTS):
            no_decay.append(p)
        else:
            decay.append(p)
    groups = [{"params": decay, "weight_decay": weight_decay},
              {"params": no_decay, "weight_decay": 0.0}]
    return torch.optim.AdamW(groups, lr=lr, betas=betas)


def build_scheduler(optimizer, warmup_steps: int = 500, total_steps: int = 10_000,
                    min_lr_ratio: float = 0.1):
    """Linear warmup to the base LR, then cosine decay to ``min_lr_ratio`` of it."""
    def lr_lambda(step: int) -> float:
        if step < warmup_steps:
            return step / max(1, warmup_steps)
        progress = (step - warmup_steps) / max(1, total_steps - warmup_steps)
        progress = min(progress, 1.0)
        return min_lr_ratio + (1 - min_lr_ratio) * 0.5 * (1 + math.cos(math.pi * progress))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)
