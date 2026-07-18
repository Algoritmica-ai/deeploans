# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""MLMCollator tests — flat (B, L) padding, lockstep metadata, masking only on real field tokens.

Builds real per-loan samples (via the tokenizer's encode_with_meta, as the dataset would yield) of
*different* lengths, then collates and checks padding, the attention mask, label/ignore placement,
and that masking never touches specials or padding.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch

from credit_fm.data.collators import MLMCollator
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.training.masking import IGNORE_INDEX, N_SPECIAL_TOKENS

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


def _panel() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    months = {"L0": 3, "L1": 6, "L2": 4}                 # deliberately different lengths
    for lid, n in months.items():
        ltv = int(rng.integers(40, 97))
        chan = rng.choice(["R", "C", "B"])
        rate = float(rng.uniform(3, 8))
        for m in range(n):
            rows.append({
                "loan_id": lid, "reporting_date": f"2020-{m+1:02d}-28",
                "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                "current_interest_rate": rate, "current_upb": 200_000 - m * 1_000,
            })
    return pd.DataFrame(rows)


def _samples(tok, panel):
    """Mimic CreditSequenceDataset.__getitem__ output for each loan."""
    out = []
    for lid in ["L0", "L1", "L2"]:
        m = tok.encode_with_meta(panel[panel.loan_id == lid])
        s = {k: torch.tensor(v, dtype=torch.long) for k, v in m.items()}
        s["n_events"] = int(max(m["event_index"]) + 1)
        out.append(s)
    return out


def test_collate_pads_to_batch_max_with_lockstep_metadata():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    samples = _samples(tok, panel)
    lengths = [s["input_ids"].shape[0] for s in samples]
    coll = MLMCollator(vocab_size=tok.vocab_size, seed=0)
    batch = coll(samples)

    max_len = max(lengths)
    for key in ("input_ids", "attention_mask", "labels", "event_index", "field_type", "branch"):
        assert batch[key].shape == (3, max_len)
    for i, n in enumerate(lengths):
        assert batch["attention_mask"][i].sum().item() == n          # real-token count
        # pad region: ids=pad, labels ignored, metadata = -1
        assert (batch["input_ids"][i, n:] == coll.pad_id).all()
        assert (batch["labels"][i, n:] == IGNORE_INDEX).all()
        for c in ("event_index", "field_type", "branch"):
            assert (batch[c][i, n:] == -1).all()
        # real region metadata carried through untouched
        assert torch.equal(batch["event_index"][i, :n], samples[i]["event_index"])
        assert torch.equal(batch["branch"][i, :n], samples[i]["branch"])


def test_masking_only_hits_real_field_tokens_never_specials_or_pad():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    samples = _samples(tok, panel)
    coll = MLMCollator(vocab_size=tok.vocab_size, seed=1, token_rate=0.5)   # force some masking
    batch = coll(samples)

    masked = batch["labels"] != IGNORE_INDEX
    assert masked.any()                                              # something was masked
    # every predicted (label) id is a real field token, never a special
    assert (batch["labels"][masked] >= N_SPECIAL_TOKENS).all()
    for i, s in enumerate(samples):
        n = s["input_ids"].shape[0]
        orig = s["input_ids"]
        specials = orig < N_SPECIAL_TOKENS                           # [BOS]/[USR]/[EVT_*]/[EOS]
        # specials are never masked: unchanged input + ignored label
        assert torch.equal(batch["input_ids"][i, :n][specials], orig[specials])
        assert (batch["labels"][i, :n][specials] == IGNORE_INDEX).all()
        # padding is never masked
        assert (batch["labels"][i, n:] == IGNORE_INDEX).all()


def test_seed_is_deterministic_and_mask_false_disables():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    samples = _samples(tok, panel)

    a = MLMCollator(vocab_size=tok.vocab_size, seed=7)(samples)
    b = MLMCollator(vocab_size=tok.vocab_size, seed=7)(samples)
    assert torch.equal(a["input_ids"], b["input_ids"])              # same seed -> same masking
    assert torch.equal(a["labels"], b["labels"])

    nomask = MLMCollator(vocab_size=tok.vocab_size, mask=False)(samples)
    assert (nomask["labels"] == IGNORE_INDEX).all()                # nothing to predict
    for i, s in enumerate(samples):                                 # ids untouched (just padded)
        n = s["input_ids"].shape[0]
        assert torch.equal(nomask["input_ids"][i, :n], s["input_ids"])
