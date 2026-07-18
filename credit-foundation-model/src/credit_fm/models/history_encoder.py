# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""History Encoder — contextualises the loan's timeline into one per-loan embedding.

Takes the profile vector and the per-event vectors (from the Profile and Event encoders) and runs
a transformer over the short sequence::

    [LOAN]  profile_vec  event_0  event_1  ...  event_{E-1}

A learnable ``[LOAN]`` token (the ``[USR]`` slot) aggregates the whole timeline; its output is the
**loan embedding** used downstream. RoPE encodes event order (oldest → newest), and a padding mask
hides absent events. The contextualised event vectors are also returned (broadcast back to tokens
by the MLM head as cross-event context).
"""

from __future__ import annotations

import torch
import torch.nn as nn

from .base import TransformerEncoder, padding_additive_mask


class HistoryEncoder(nn.Module):
    def __init__(self, dim: int = 256, n_layers: int = 6, n_heads: int = 8, mlp_mult: int = 4,
                 dropout: float = 0.0):
        super().__init__()
        self.loan_token = nn.Parameter(torch.zeros(1, 1, dim))
        nn.init.normal_(self.loan_token, std=0.02)
        self.encoder = TransformerEncoder(n_layers, dim, n_heads, mlp_mult, dropout=dropout)

    def forward(self, profile_vec: torch.Tensor, event_vecs: torch.Tensor,
                event_mask: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Contextualise the timeline.

        Args:
            profile_vec: ``(B, dim)`` from the Profile encoder.
            event_vecs: ``(B, E, dim)`` from the Event encoder.
            event_mask: ``(B, E)`` bool — which events are real.

        Returns:
            ``(loan_embedding, history_event_ctx)`` — ``(B, dim)`` per-loan embedding (the ``[LOAN]``
            output) and ``(B, E, dim)`` history-contextualised event vectors.
        """
        bsz, n_events, dim = event_vecs.shape
        loan = self.loan_token.expand(bsz, 1, dim)
        seq = torch.cat([loan, profile_vec.unsqueeze(1), event_vecs], dim=1)   # (B, 2+E, dim)
        valid = torch.cat([event_mask.new_ones(bsz, 2), event_mask], dim=1)    # [LOAN]+profile always
        out = self.encoder(seq, padding_additive_mask(valid.long()))
        return out[:, 0], out[:, 2:]                                           # loan emb, event ctx
