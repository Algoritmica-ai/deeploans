# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""CreditFoundationModel — the hierarchical three-branch encoder, end to end.

Consumes a batch from :class:`~credit_fm.data.collators.MLMCollator` (the frozen data-layer
contract) and produces the MLM loss for pretraining and the per-loan ``[USR]`` embedding for
downstream use::

    embed → Profile encoder ─┐
            Event encoder  ──┼─→ History encoder → loan embedding
                             │                     │
            (token states) ──┴────────── MLM head (local + segment + loan) → vocab logits

Flat ``(B, L)`` throughout (DL-014); the Event/Profile encoders pool via ``event_index``/``branch``.
Default sizes target ~30M params; ``test_e2e`` runs a tiny config end to end.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F

from .base import Embeddings
from .classification_head import ClassificationHead
from .event_encoder import EventEncoder
from .history_encoder import HistoryEncoder
from .mlm_head import MLMHead
from .profile_encoder import ProfileStateEncoder


class CreditFoundationModel(nn.Module):
    """Three-branch encoder-only credit foundation model.

    Args:
        vocab_size: tokenizer vocabulary size.
        n_field_types: number of field-type ids (``len(tokenizer.field_types)``).
        dim, n_heads: model width and attention heads.
        profile_layers, event_layers, history_layers: depth per branch.
        n_classes: downstream classification head output size.
        pad_id: padding token id (default ``[PAD]`` = 0).
    """

    def __init__(self, vocab_size: int, n_field_types: int, dim: int = 384, n_heads: int = 8,
                 profile_layers: int = 3, event_layers: int = 5, history_layers: int = 6,
                 mlp_mult: int = 4, n_classes: int = 2, pad_id: int = 0, dropout: float = 0.0):
        super().__init__()
        self.dim = dim
        self.embeddings = Embeddings(vocab_size, dim, n_field_types, pad_id, dropout=dropout)
        self.profile_encoder = ProfileStateEncoder(dim, profile_layers, n_heads, mlp_mult, dropout)
        self.event_encoder = EventEncoder(dim, event_layers, n_heads, mlp_mult, dropout)
        self.history_encoder = HistoryEncoder(dim, history_layers, n_heads, mlp_mult, dropout)
        self.mlm_head = MLMHead(dim, vocab_size)
        self.classification_head = ClassificationHead(dim, n_classes)

    def _encode(self, batch: dict):
        """Run the three branches; return token states + the loan embedding for the heads."""
        ids, ftype, branch, ev = (batch["input_ids"], batch["field_type"],
                                  batch["branch"], batch["event_index"])
        hidden = self.embeddings(ids, ftype, branch)                       # (B, L, dim)
        profile_vec, profile_tok = self.profile_encoder(hidden, branch, return_tokens=True)
        n_events = max(int(batch["n_events"].max().item()), 1)
        event_vecs, event_mask, event_tok = self.event_encoder(
            hidden, ev, n_events=n_events, return_tokens=True)
        loan_emb, hist_event_ctx = self.history_encoder(profile_vec, event_vecs, event_mask)
        return hidden, profile_vec, profile_tok, event_tok, hist_event_ctx, loan_emb

    def forward(self, batch: dict) -> dict:
        """Return ``{logits, loan_embedding[, loss]}`` for the batch."""
        hidden, profile_vec, profile_tok, event_tok, hist_event_ctx, loan_emb = self._encode(batch)
        branch, ev = batch["branch"], batch["event_index"]
        _, length, dim = hidden.shape

        is_event = (branch == 1).unsqueeze(-1)
        is_profile = (branch == 0).unsqueeze(-1)
        # local: each token's own encoder output (event / profile token states; else raw embed)
        local = torch.where(is_event, event_tok, torch.where(is_profile, profile_tok, hidden))
        # segment: history-context of the token's event; profile tokens use the profile vector
        gather_idx = ev.clamp(min=0).unsqueeze(-1).expand(-1, -1, dim)
        gathered = torch.gather(hist_event_ctx, 1, gather_idx)             # (B, L, dim)
        segment = torch.where(is_event, gathered, profile_vec.unsqueeze(1).expand(-1, length, -1))
        # loan: the global [USR]/[LOAN] embedding broadcast to every token
        loan_ctx = loan_emb.unsqueeze(1).expand(-1, length, -1)

        logits = self.mlm_head(local, segment, loan_ctx)                  # (B, L, vocab)
        out = {"logits": logits, "loan_embedding": loan_emb}
        if "labels" in batch:
            out["loss"] = F.cross_entropy(
                logits.reshape(-1, logits.size(-1)), batch["labels"].reshape(-1), ignore_index=-100)
        return out

    @torch.no_grad()
    def extract_embeddings(self, batch: dict) -> torch.Tensor:
        """Return the ``(B, dim)`` per-loan ``[USR]`` embedding (inference; no masking needed)."""
        return self._encode(batch)[-1]

    def classify(self, batch: dict) -> torch.Tensor:
        """Return downstream class logits ``(B, n_classes)`` from the loan embedding."""
        return self.classification_head(self._encode(batch)[-1])

    def num_parameters(self) -> int:
        return sum(p.numel() for p in self.parameters())
