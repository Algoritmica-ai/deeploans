# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Model building-block tests — RMSNorm/RoPE attention/encoder/embeddings + Event encoder.

Shape + finiteness checks, plus the two behavioural guarantees that matter for the hierarchy:
the Event encoder's attention is **intra-event** (one month can't leak into another) and pooling
is a masked mean (absent events → zero vector, masked out).
"""

from __future__ import annotations

import math

import torch

from credit_fm.models.base import (
    Embeddings,
    MultiHeadSelfAttention,
    RMSNorm,
    TransformerEncoder,
    apply_rope,
    event_block_additive_mask,
    padding_additive_mask,
    _rope_tables,
)
from credit_fm.models.event_encoder import EventEncoder
from credit_fm.models.history_encoder import HistoryEncoder
from credit_fm.models.profile_encoder import ProfileStateEncoder


def test_rmsnorm_normalises():
    x = torch.randn(2, 5, 16) * 7 + 3
    out = RMSNorm(16)(x)
    assert out.shape == x.shape
    assert torch.allclose(out.pow(2).mean(-1).sqrt(), torch.ones(2, 5), atol=1e-4)


def test_attention_shape_and_padding_mask_no_nan():
    x = torch.randn(2, 6, 32)
    attn = MultiHeadSelfAttention(32, n_heads=4)
    mask = padding_additive_mask(torch.tensor([[1, 1, 1, 1, 0, 0], [1, 1, 1, 0, 0, 0]]))
    out = attn(x, mask)
    assert out.shape == x.shape and torch.isfinite(out).all()


def _manual_attention(mod: MultiHeadSelfAttention, x, attn_mask):
    """The pre-FlashAttention reference: explicit softmax(QKᵀ/√d + mask)·V using the module's own
    weights + RoPE. SDPA (mod.forward) must match this to prove the swap changed nothing but speed."""
    b, length, dim = x.shape
    qkv = mod.qkv(x).view(b, length, 3, mod.n_heads, mod.head_dim).permute(2, 0, 3, 1, 4)
    q, k, v = qkv[0], qkv[1], qkv[2]
    cos, sin = _rope_tables(length, mod.head_dim, mod.rope_base, x.device, x.dtype)
    q, k = apply_rope(q, k, cos, sin)
    scores = torch.matmul(q, k.transpose(-2, -1)) / math.sqrt(mod.head_dim)
    if attn_mask is not None:
        scores = scores + attn_mask
    out = torch.matmul(scores.softmax(-1), v).transpose(1, 2).contiguous().view(b, length, dim)
    return mod.proj(out)


def test_sdpa_attention_is_numerically_identical_to_manual():
    """FlashAttention (SDPA) must equal the old manual attention to fp tolerance, for no mask, a
    padding mask, and an intra-event block mask — the safety proof for the swap."""
    torch.manual_seed(0)
    mod = MultiHeadSelfAttention(32, n_heads=4, dropout=0.0).eval()   # eval => no dropout, comparable
    x = torch.randn(3, 8, 32)
    event_index = torch.tensor([[-1, 0, 0, 1, 1, 1, 2, 2],
                                [-1, -1, 0, 0, 0, 1, 1, 1],
                                [0, 0, 1, 1, 2, 2, 3, 3]])
    pad = padding_additive_mask(torch.tensor([[1, 1, 1, 1, 1, 1, 0, 0],
                                              [1, 1, 1, 1, 1, 0, 0, 0],
                                              [1, 1, 1, 1, 1, 1, 1, 1]]))
    for mask in (None, pad, event_block_additive_mask(event_index)):
        got = mod(x, mask)
        ref = _manual_attention(mod, x, mask)
        assert torch.allclose(got, ref, atol=1e-5, rtol=1e-4), "SDPA diverged from manual attention"


def test_encoder_forward_and_backward():
    enc = TransformerEncoder(n_layers=2, dim=32, n_heads=4)
    x = torch.randn(3, 7, 32, requires_grad=True)
    out = enc(x)
    assert out.shape == x.shape and torch.isfinite(out).all()
    out.sum().backward()
    assert torch.isfinite(x.grad).all()


def test_dropout_active_in_train_inert_in_eval():
    torch.manual_seed(0)
    enc = TransformerEncoder(n_layers=2, dim=16, n_heads=2, dropout=0.5)
    x = torch.randn(2, 6, 16)
    enc.train()
    assert not torch.allclose(enc(x), enc(x))      # dropout randomises across train forwards
    enc.eval()
    assert torch.allclose(enc(x), enc(x))          # deterministic in eval (clean val loss)


def test_embeddings_handle_minus_one_metadata():
    emb = Embeddings(vocab_size=20, dim=16, n_field_types=5)
    input_ids = torch.randint(0, 20, (2, 6))
    field_type = torch.tensor([[-1, 0, 4, 2, -1, 3], [1, 1, -1, 0, 4, -1]])
    branch = torch.tensor([[-1, 0, 1, 1, -1, 0], [0, 0, -1, 1, 1, -1]])
    out = emb(input_ids, field_type, branch)               # must not index-error on -1
    assert out.shape == (2, 6, 16) and torch.isfinite(out).all()


def test_event_mask_helpers_allow_diagonal():
    ev = torch.tensor([[-1, 0, 0, 1, -1]])
    add = event_block_additive_mask(ev)                    # (1,1,5,5)
    assert add.shape == (1, 1, 5, 5)
    assert (add.diagonal(dim1=-2, dim2=-1) == 0).all()     # every token attends to itself
    assert add[0, 0, 1, 2] == 0 and add[0, 0, 1, 3] == float("-inf")   # same event vs cross event


def _ev_index():
    # profile/specials = -1; event 0 = 3 tokens; event 1 = 2 tokens
    return torch.tensor([[-1, -1, 0, 0, 0, 1, 1, -1],
                         [-1, -1, 0, 0, 0, 1, 1, -1]])


def test_event_encoder_shapes_and_mask():
    enc = EventEncoder(dim=16, n_layers=2, n_heads=4).eval()
    hidden = torch.randn(2, 8, 16)
    vecs, mask = enc(hidden, _ev_index())
    assert vecs.shape == (2, 2, 16) and mask.shape == (2, 2)
    assert mask.all() and torch.isfinite(vecs).all()       # both events present


def test_event_encoder_is_intra_event_isolated():
    enc = EventEncoder(dim=16, n_layers=2, n_heads=4).eval()
    ev = _ev_index()
    hidden = torch.randn(2, 8, 16)
    with torch.no_grad():
        v1, _ = enc(hidden, ev)
        bumped = hidden.clone()
        bumped[:, 5:7] += 5.0                              # perturb only event-1 tokens
        v2, _ = enc(bumped, ev)
    assert torch.allclose(v1[:, 0], v2[:, 0], atol=1e-5)   # event 0 unaffected by event 1
    assert not torch.allclose(v1[:, 1], v2[:, 1])          # event 1 did change


def test_event_encoder_absent_event_is_zero_and_masked():
    enc = EventEncoder(dim=16, n_layers=2, n_heads=4).eval()
    ev = torch.tensor([[-1, 0, 0, -1]])                    # only event 0 exists
    vecs, mask = enc(torch.randn(1, 4, 16), ev, n_events=2)   # but pool to 2 events
    assert mask.tolist() == [[True, False]]
    assert torch.equal(vecs[0, 1], torch.zeros(16))        # absent event → zero vector


def test_profile_encoder_shape_and_event_isolation():
    enc = ProfileStateEncoder(dim=16, n_layers=2, n_heads=4).eval()
    branch = torch.tensor([[-1, -1, 0, 0, 0, 1, 1, -1],    # profile at 2..4, events at 5,6
                           [-1, -1, 0, 0, 0, 1, 1, -1]])
    hidden = torch.randn(2, 8, 16)
    with torch.no_grad():
        v1 = enc(hidden, branch)
        bumped = hidden.clone()
        bumped[:, 5:7] += 5.0                              # perturb event tokens only
        v2 = enc(bumped, branch)
    assert v1.shape == (2, 16) and torch.isfinite(v1).all()
    assert torch.allclose(v1, v2, atol=1e-5)              # profile vector ignores event tokens


def test_history_encoder_shapes_and_masks_absent_events():
    enc = HistoryEncoder(dim=16, n_layers=2, n_heads=4).eval()
    profile = torch.randn(2, 16)
    event_vecs = torch.randn(2, 3, 16)
    event_mask = torch.tensor([[True, True, True], [True, True, False]])   # loan 1 missing event 2
    with torch.no_grad():
        loan1, ctx = enc(profile, event_vecs, event_mask)
        bumped = event_vecs.clone()
        bumped[1, 2] += 5.0                               # perturb loan 1's ABSENT event
        loan2, _ = enc(profile, bumped, event_mask)
    assert loan1.shape == (2, 16) and ctx.shape == (2, 3, 16)
    assert torch.isfinite(loan1).all()
    assert torch.allclose(loan1[1], loan2[1], atol=1e-5)  # masked event can't move the loan embedding


def test_history_encoder_gradient_flows():
    enc = HistoryEncoder(dim=16, n_layers=2, n_heads=4)
    profile = torch.randn(2, 16, requires_grad=True)
    event_vecs = torch.randn(2, 3, 16, requires_grad=True)
    mask = torch.ones(2, 3, dtype=torch.bool)
    loan, _ = enc(profile, event_vecs, mask)
    loan.sum().backward()
    assert torch.isfinite(profile.grad).all() and torch.isfinite(event_vecs.grad).all()
