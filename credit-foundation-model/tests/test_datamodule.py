# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""CreditDataModule tests — loaders, manifest-derived vocab_size, shuffle/eval policy, determinism.

Writes toy train/val shard dirs (with a manifest carrying ``vocab_size``, as encode_dataset.py does)
and exercises the loaders end-to-end.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import torch
from torch.utils.data import RandomSampler, SequentialSampler

from credit_fm.data.datamodule import CreditDataModule
from credit_fm.data.encode import iter_shards
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage

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


def _panel(n_loans, n_months=5) -> pd.DataFrame:
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


def _write_split(tok, panel, out_dir, shard_size=4):
    """Write shards + a manifest carrying vocab_size (as scripts/encode_dataset.py does)."""
    names = []
    for i, shard in enumerate(iter_shards(tok, panel, shard_size)):
        name = f"shard-{i:05d}.parquet"
        storage.write_parquet(shard, storage.join(out_dir, name))
        names.append(name)
    storage.write_text(json.dumps({"vocab_size": tok.vocab_size, "shards": names}),
                       storage.join(out_dir, "manifest.json"))


def _build(tmp_path, n_train=10, n_val=6):
    tok = KVTTokenizer(CONFIG).fit(_panel(n_train))
    train_dir, val_dir = str(tmp_path / "train"), str(tmp_path / "val")
    _write_split(tok, _panel(n_train), train_dir)
    _write_split(tok, _panel(n_val), val_dir)
    return tok, train_dir, val_dir


def test_vocab_from_manifest_and_loader_shapes(tmp_path):
    tok, train_dir, val_dir = _build(tmp_path)
    dm = CreditDataModule(train_dir, val_dir=val_dir, batch_size=4)
    assert dm.vocab_size == tok.vocab_size                       # read from manifest

    seen = 0
    for b in dm.train_dataloader():
        for key in ("input_ids", "attention_mask", "labels", "event_index", "field_type", "branch"):
            assert b[key].shape[0] == b["input_ids"].shape[0] and b[key].dim() == 2
        seen += b["input_ids"].shape[0]
    assert seen == 10                                            # every train loan delivered once


def test_train_shuffles_eval_does_not(tmp_path):
    _, train_dir, val_dir = _build(tmp_path)
    dm = CreditDataModule(train_dir, val_dir=val_dir, batch_size=4)
    assert isinstance(dm.train_dataloader().sampler, RandomSampler)
    assert isinstance(dm.val_dataloader().sampler, SequentialSampler)


def test_val_masking_is_deterministic(tmp_path):
    _, train_dir, val_dir = _build(tmp_path)
    dm = CreditDataModule(train_dir, val_dir=val_dir, batch_size=4)
    first = torch.cat([b["labels"] for b in dm.val_dataloader()])
    second = torch.cat([b["labels"] for b in dm.val_dataloader()])
    assert torch.equal(first, second)                           # fixed val_seed -> stable masking


def test_missing_split_and_explicit_vocab(tmp_path):
    tok, train_dir, _ = _build(tmp_path)
    dm = CreditDataModule(train_dir, vocab_size=tok.vocab_size, batch_size=4)   # explicit override
    assert dm.vocab_size == tok.vocab_size
    try:
        dm.val_dataloader()
        raise AssertionError("expected ValueError for missing val split")
    except ValueError:
        pass
