# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Shared transformer building blocks for the three-branch encoder (modern LLM stack).

A small, self-contained block library used by every branch (Profile / Event / History):

* :class:`RMSNorm` — pre-norm normalisation (no mean-subtraction, no bias).
* **RoPE** (:func:`apply_rope`) — rotary position embeddings, applied to q/k inside attention.
* :class:`MultiHeadSelfAttention` — RoPE attention with an optional **additive** mask
  ``(B, 1, L, L)`` so callers can express padding *or* block-structured (intra-event) attention.
* :class:`SwiGLU` — gated MLP.
* :class:`TransformerBlock` / :class:`TransformerEncoder` — pre-norm residual stack.
* :class:`Embeddings` — token + ``field_type`` + ``branch`` embeddings (the data-layer contract;
  ``-1`` metadata is mapped to a dedicated row, never an invalid index).
* mask helpers: :func:`padding_additive_mask`, :func:`event_block_additive_mask`.

All blocks take a ``dropout`` (default ``0.0``); set it > 0 for regularisation during pretraining
(it's a no-op in ``eval()``, so validation loss is clean).

``BaseEncoder`` is the legacy scaffold interface, kept only so the not-yet-rebuilt
profile/history scaffolds still import; new code uses :class:`TransformerEncoder`.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F

NEG_INF = float("-inf")


class RMSNorm(nn.Module):
    def __init__(self, dim: int, eps: float = 1e-6):
        super().__init__()
        self.eps = eps
        self.weight = nn.Parameter(torch.ones(dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        norm = x * torch.rsqrt(x.pow(2).mean(-1, keepdim=True) + self.eps)
        return norm * self.weight


def _rope_tables(seq_len: int, head_dim: int, base: float, device, dtype):
    """Return ``(cos, sin)`` of shape ``(L, head_dim)`` for rotary embeddings."""
    half = head_dim // 2
    inv_freq = 1.0 / (base ** (torch.arange(0, half, device=device).float() / half))
    pos = torch.arange(seq_len, device=device).float()
    ang = torch.outer(pos, inv_freq)                      # (L, half)
    cos = torch.cat([ang.cos(), ang.cos()], dim=-1)
    sin = torch.cat([ang.sin(), ang.sin()], dim=-1)
    return cos.to(dtype), sin.to(dtype)


def _rotate_half(x: torch.Tensor) -> torch.Tensor:
    half = x.shape[-1] // 2
    return torch.cat([-x[..., half:], x[..., :half]], dim=-1)


def apply_rope(q: torch.Tensor, k: torch.Tensor, cos: torch.Tensor, sin: torch.Tensor):
    """Apply rotary position embeddings to ``q``/``k`` ``(B, n_heads, L, head_dim)``."""
    cos, sin = cos[None, None], sin[None, None]
    return q * cos + _rotate_half(q) * sin, k * cos + _rotate_half(k) * sin


class MultiHeadSelfAttention(nn.Module):
    """RoPE self-attention via ``scaled_dot_product_attention`` (FlashAttention-2 on H100).

    SDPA computes ``softmax(QKᵀ/√d + attn_mask) · V`` **without materialising the ``(B,H,L,L)``
    score matrix** — the O(L²) memory that OOMs long sequences under the naive path. The additive
    float ``attn_mask`` (``(B,1,L,L)`` or ``(B,1,1,L)``, ``0`` keep / ``-inf`` mask) broadcasts over
    heads exactly as before, so the math is identical (proven in tests) — it just fits far bigger
    batches and runs faster. On CPU / older GPUs SDPA falls back to the math kernel automatically.
    """

    def __init__(self, dim: int, n_heads: int, rope_base: float = 10_000.0, dropout: float = 0.0):
        super().__init__()
        if dim % n_heads or (dim // n_heads) % 2:
            raise ValueError(f"dim {dim} must divide by n_heads {n_heads} into an even head_dim")
        self.n_heads = n_heads
        self.head_dim = dim // n_heads
        self.rope_base = rope_base
        self.dropout_p = dropout                          # applied to attn weights inside SDPA
        self.qkv = nn.Linear(dim, 3 * dim, bias=False)
        self.proj = nn.Linear(dim, dim, bias=False)
        self.resid_drop = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor, attn_mask: torch.Tensor | None = None) -> torch.Tensor:
        bsz, length, dim = x.shape
        qkv = self.qkv(x).view(bsz, length, 3, self.n_heads, self.head_dim).permute(2, 0, 3, 1, 4)
        q, k, v = qkv[0], qkv[1], qkv[2]                  # (B, n_heads, L, head_dim)
        cos, sin = _rope_tables(length, self.head_dim, self.rope_base, x.device, x.dtype)
        q, k = apply_rope(q, k, cos, sin)
        out = F.scaled_dot_product_attention(            # FlashAttention-2 backend on H100
            q, k, v, attn_mask=attn_mask,
            dropout_p=self.dropout_p if self.training else 0.0)
        out = out.transpose(1, 2).contiguous().view(bsz, length, dim)
        return self.resid_drop(self.proj(out))


class SwiGLU(nn.Module):
    def __init__(self, dim: int, mult: int = 4, dropout: float = 0.0):
        super().__init__()
        hidden = int(mult * dim * 2 / 3)
        hidden = (hidden + 7) // 8 * 8                    # round to a multiple of 8
        self.w_gate = nn.Linear(dim, hidden, bias=False)
        self.w_up = nn.Linear(dim, hidden, bias=False)
        self.w_down = nn.Linear(hidden, dim, bias=False)
        self.drop = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.drop(self.w_down(F.silu(self.w_gate(x)) * self.w_up(x)))


class TransformerBlock(nn.Module):
    def __init__(self, dim: int, n_heads: int, mlp_mult: int = 4, rope_base: float = 10_000.0,
                 dropout: float = 0.0):
        super().__init__()
        self.norm1 = RMSNorm(dim)
        self.attn = MultiHeadSelfAttention(dim, n_heads, rope_base, dropout)
        self.norm2 = RMSNorm(dim)
        self.mlp = SwiGLU(dim, mlp_mult, dropout)

    def forward(self, x: torch.Tensor, attn_mask: torch.Tensor | None = None) -> torch.Tensor:
        x = x + self.attn(self.norm1(x), attn_mask)
        x = x + self.mlp(self.norm2(x))
        return x


class TransformerEncoder(nn.Module):
    """Pre-norm residual stack of ``n_layers`` blocks; final RMSNorm."""

    def __init__(self, n_layers: int, dim: int, n_heads: int, mlp_mult: int = 4,
                 rope_base: float = 10_000.0, dropout: float = 0.0):
        super().__init__()
        self.layers = nn.ModuleList(
            [TransformerBlock(dim, n_heads, mlp_mult, rope_base, dropout) for _ in range(n_layers)])
        self.norm = RMSNorm(dim)

    def forward(self, x: torch.Tensor, attn_mask: torch.Tensor | None = None) -> torch.Tensor:
        for layer in self.layers:
            x = layer(x, attn_mask)
        return self.norm(x)


class Embeddings(nn.Module):
    """Sum of token + field-type + branch embeddings (the frozen data-layer contract).

    ``field_type`` ``-1`` (structural specials) maps to a dedicated final row; ``branch``
    ``{-1, 0, 1}`` maps to ``{0, 1, 2}``.
    """

    def __init__(self, vocab_size: int, dim: int, n_field_types: int, pad_id: int = 0,
                 dropout: float = 0.0):
        super().__init__()
        self.n_field_types = n_field_types
        self.token = nn.Embedding(vocab_size, dim, padding_idx=pad_id)
        self.field = nn.Embedding(n_field_types + 1, dim)    # last row = "none" (-1)
        self.branch = nn.Embedding(3, dim)                   # -1/0/1 -> 0/1/2
        self.drop = nn.Dropout(dropout)

    def forward(self, input_ids, field_type, branch) -> torch.Tensor:
        field = torch.where(field_type >= 0, field_type,
                            torch.full_like(field_type, self.n_field_types))
        return self.drop(self.token(input_ids) + self.field(field) + self.branch(branch + 1))


def padding_additive_mask(attention_mask: torch.Tensor) -> torch.Tensor:
    """``(B, L)`` 1=real/0=pad → additive ``(B, 1, 1, L)`` mask (0 keep, -inf on pad keys)."""
    pad = attention_mask == 0
    return torch.zeros_like(pad, dtype=torch.float).masked_fill(pad, NEG_INF)[:, None, None, :]


def event_block_additive_mask(event_index: torch.Tensor) -> torch.Tensor:
    """Intra-event additive mask ``(B, 1, L, L)``: a token attends only within its own event.

    Tokens with ``event_index == -1`` (profile/structural) attend to nothing but themselves
    (diagonal always allowed, so no fully-masked row → no NaNs); their outputs are discarded.
    """
    same = (event_index[:, :, None] == event_index[:, None, :]) & (event_index[:, :, None] >= 0)
    length = event_index.shape[1]
    eye = torch.eye(length, dtype=torch.bool, device=event_index.device)[None]
    allow = same | eye
    return torch.zeros_like(allow, dtype=torch.float).masked_fill(~allow, NEG_INF)[:, None]


class BaseEncoder(nn.Module):
    """Legacy scaffold interface (kept for import-compat); new code uses TransformerEncoder."""

    def __init__(self, num_layers: int, hidden_size: int, num_heads: int):
        super().__init__()
        self.num_layers, self.hidden_size, self.num_heads = num_layers, hidden_size, num_heads

    def forward(self, x, attn_mask=None):
        raise NotImplementedError
