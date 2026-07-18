# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""CreditSequenceDataset tests — reads encode-once shards, returns aligned unpadded tensors.

Writes toy shards (via ``iter_shards`` + ``storage``) to a tmp dir, then reads them back through the
dataset — so the encode->shard->read contract is exercised end-to-end with no GCS/real data.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import torch

from credit_fm.data.dataset import CreditSequenceDataset
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


def _panel(n_loans=8, n_months=5) -> pd.DataFrame:
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


def _write_shards(tok, panel, out_dir, shard_size):
    """Mimic scripts/encode_dataset.py: write shard parquet files + a manifest."""
    names = []
    for i, shard in enumerate(iter_shards(tok, panel, shard_size)):
        name = f"shard-{i:05d}.parquet"
        storage.write_parquet(shard, storage.join(out_dir, name))
        names.append(name)
    storage.write_text(json.dumps({"shards": names}), storage.join(out_dir, "manifest.json"))


def test_dataset_reads_shards_and_returns_aligned_tensors(tmp_path):
    panel = _panel(n_loans=6, n_months=5)
    tok = KVTTokenizer(CONFIG).fit(panel)
    out = str(tmp_path / "train")
    _write_shards(tok, panel, out, shard_size=4)            # spans 2 shards (4 + 2)

    ds = CreditSequenceDataset(out)
    assert len(ds) == 6
    item = ds[0]
    length = item["input_ids"].shape[0]
    for c in ("input_ids", "event_index", "field_type", "branch"):
        assert item[c].shape[0] == length                   # all metadata aligned with ids
        assert item[c].dtype == torch.long
    assert int(item["event_index"].max()) + 1 == item["n_events"]   # event count consistent


def test_dataset_item_matches_tokenizer_encode(tmp_path):
    panel = _panel(n_loans=4, n_months=5)
    tok = KVTTokenizer(CONFIG).fit(panel)
    out = str(tmp_path / "train")
    _write_shards(tok, panel, out, shard_size=10)           # single shard, first-seen order

    ds = CreditSequenceDataset(out)
    # encode_panel preserves first-seen loan order, so ds[0] is L0
    assert ds[0]["input_ids"].tolist() == tok.encode(panel[panel.loan_id == "L0"])
    assert tok.decode(ds[0]["input_ids"].tolist())[0] == "[BOS]"


def test_dataset_limit_truncates(tmp_path):
    panel = _panel(n_loans=8, n_months=4)
    tok = KVTTokenizer(CONFIG).fit(panel)
    out = str(tmp_path / "train")
    _write_shards(tok, panel, out, shard_size=3)
    assert len(CreditSequenceDataset(out, limit=5)) == 5
