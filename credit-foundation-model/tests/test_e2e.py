# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""End-to-end M2 gate — tokenizer → collator → CreditFoundationModel.

Builds a real batch (encode_with_meta + MLMCollator) and runs it through the full hierarchical
model: forward shapes + finite loss, gradient flow, and — the actual gate — that the model
**overfits a single fixed batch** (loss falls sharply), i.e. the architecture learns. Tiny config
keeps it well under 60s.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch

from credit_fm.data.collators import MLMCollator
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer

CONFIG = {
    "id_col": "loan_id",
    "time_col": "reporting_date",
    "time_field": "loan_age",
    "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
    "event": {"numeric": ["current_interest_rate", "current_upb"], "categorical": []},
    "n_bins": 8,
    "max_categories": 64,
    "max_events": 60,
    "calendar": "yearquarter",
}


def _panel(n_loans=6, n_months=5) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for lid in range(n_loans):
        ltv = int(rng.integers(40, 97))
        chan = rng.choice(["R", "C", "B"])
        rate = float(rng.uniform(3, 8))
        for m in range(n_months):
            rows.append({
                "loan_id": f"L{lid}", "reporting_date": f"2020-{m+1:02d}-28",
                "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                "current_interest_rate": rate, "current_upb": 200_000 - m * 1_000,
            })
    return pd.DataFrame(rows)


def _batch(tok, panel, token_rate=0.3, seed=0):
    samples = []
    for lid in panel.loan_id.unique():
        meta = tok.encode_with_meta(panel[panel.loan_id == lid])
        s = {k: torch.tensor(v, dtype=torch.long) for k, v in meta.items()}
        s["n_events"] = int(max(meta["event_index"]) + 1)
        samples.append(s)
    return MLMCollator(vocab_size=tok.vocab_size, seed=seed, token_rate=token_rate)(samples)


def _model(tok):
    torch.manual_seed(0)
    return CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=32, n_heads=4,
                                 profile_layers=1, event_layers=1, history_layers=1)


def test_forward_shapes_and_finite_loss_with_grads():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    batch = _batch(tok, panel)
    model = _model(tok)
    out = model(batch)
    bsz, length = batch["input_ids"].shape
    assert out["logits"].shape == (bsz, length, tok.vocab_size)
    assert out["loan_embedding"].shape == (bsz, 32)
    assert torch.isfinite(out["loss"])
    out["loss"].backward()
    grads = [p.grad for p in model.parameters() if p.grad is not None]
    assert grads and all(torch.isfinite(g).all() for g in grads)


def test_overfits_single_batch():
    """The M2 gate: on a fixed masked batch the loss must fall sharply (the model learns)."""
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    batch = _batch(tok, panel, token_rate=0.4)
    model = _model(tok)
    opt = torch.optim.Adam(model.parameters(), lr=2e-3)
    losses = []
    for _ in range(60):
        opt.zero_grad()
        out = model(batch)
        out["loss"].backward()
        opt.step()
        losses.append(out["loss"].item())
    assert losses[-1] < losses[0] * 0.5                  # loss at least halves


def test_extract_embeddings_and_classify_shapes():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    batch = _batch(tok, panel)
    model = _model(tok).eval()
    bsz = batch["input_ids"].shape[0]
    assert model.extract_embeddings(batch).shape == (bsz, 32)
    assert model.classify(batch).shape == (bsz, 2)
