# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Event Encoder — contextualises each month's field tokens, then pools to one vector per event.

Operates on the flat ``(B, L, dim)`` embedded sequence plus ``event_index`` (the data-layer
contract). Attention is **intra-event**: a token attends only to other tokens of the same month
(:func:`event_block_additive_mask`), so months are encoded independently. Each event is then pooled
by a masked mean over its tokens → ``(B, E, dim)`` event vectors fed to the History encoder, where
``E`` is the batch's max event count. Profile/structural tokens (``event_index == -1``) take no part.
"""

from __future__ import annotations

import torch
import torch.nn as nn

from .base import TransformerEncoder, event_block_additive_mask


class EventEncoder(nn.Module):
    def __init__(self, dim: int = 256, n_layers: int = 5, n_heads: int = 8, mlp_mult: int = 4,
                 dropout: float = 0.0):
        super().__init__()
        self.encoder = TransformerEncoder(n_layers, dim, n_heads, mlp_mult, dropout=dropout)

    def forward(self, hidden: torch.Tensor, event_index: torch.Tensor,
                n_events: int | None = None, return_tokens: bool = False):
        """Encode events and pool.

        Args:
            hidden: ``(B, L, dim)`` embedded token sequence.
            event_index: ``(B, L)`` month index per token; ``-1`` for profile/structural.
            n_events: ``E`` to pool to; defaults to ``event_index.max() + 1`` over the batch.
            return_tokens: also return the token-level encoded states (for the MLM head's local ctx).

        Returns:
            ``(event_vecs, event_mask)`` — ``(B, E, dim)`` per-event vectors and ``(B, E)`` bool mask
            of which events are real; plus ``(B, L, dim)`` token states when ``return_tokens``.
        """
        bsz, length, dim = hidden.shape
        encoded = self.encoder(hidden, event_block_additive_mask(event_index))   # (B, L, dim)

        valid = event_index >= 0                                                  # (B, L)
        e_idx = event_index.clamp(min=0)                                          # -1 -> bin 0 (masked)
        size = n_events if n_events is not None else int(event_index.max().item()) + 1
        size = max(size, 1)

        masked = encoded * valid.unsqueeze(-1)                                    # zero non-event tokens
        sums = hidden.new_zeros(bsz, size, dim).scatter_add_(
            1, e_idx.unsqueeze(-1).expand(-1, -1, dim), masked)
        counts = hidden.new_zeros(bsz, size).scatter_add_(1, e_idx, valid.to(hidden.dtype))  # (B, E)
        event_vecs = sums / counts.clamp(min=1).unsqueeze(-1)                     # masked mean
        if return_tokens:
            return event_vecs, counts > 0, encoded
        return event_vecs, counts > 0
