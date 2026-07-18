# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Profile State Encoder — contextualises the static origination tokens into one profile vector.

Operates on the flat ``(B, L, dim)`` embedded sequence plus ``branch`` (the data-layer contract).
Attention is restricted to the **profile region** (``branch == 0``) — the static origination
tokens attend among themselves, ignoring event/structural tokens — then a masked mean over those
tokens yields a single ``(B, dim)`` profile vector fed to the History encoder. Structurally this is
the Event encoder's pooling applied to one "segment" (all profile tokens share index 0).
"""

from __future__ import annotations

import torch
import torch.nn as nn

from .base import TransformerEncoder, event_block_additive_mask


class ProfileStateEncoder(nn.Module):
    def __init__(self, dim: int = 256, n_layers: int = 3, n_heads: int = 8, mlp_mult: int = 4,
                 dropout: float = 0.0):
        super().__init__()
        self.encoder = TransformerEncoder(n_layers, dim, n_heads, mlp_mult, dropout=dropout)

    def forward(self, hidden: torch.Tensor, branch: torch.Tensor, return_tokens: bool = False):
        """Return the ``(B, dim)`` profile vector.

        Args:
            hidden: ``(B, L, dim)`` embedded token sequence.
            branch: ``(B, L)`` branch ids; profile tokens are ``branch == 0``.
            return_tokens: also return the token-level encoded states (for the MLM head's local ctx).
        """
        profile = branch == 0                                          # (B, L)
        index = torch.where(profile, torch.zeros_like(branch), torch.full_like(branch, -1))
        encoded = self.encoder(hidden, event_block_additive_mask(index))   # intra-profile attention
        mask = profile.unsqueeze(-1).to(hidden.dtype)
        profile_vec = (encoded * mask).sum(1) / mask.sum(1).clamp(min=1)   # masked mean → (B, dim)
        return (profile_vec, encoded) if return_tokens else profile_vec
