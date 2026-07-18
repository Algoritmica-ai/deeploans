# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Encode-once shard tests — one row per loan, aligned columns, whole-loan shards, parquet survives.

Covers the testable core of ``scripts/encode_dataset.py`` (``credit_fm.data.encode``) on a synthetic
panel, so no GCS/real data is needed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from credit_fm.data.encode import encode_panel, encode_to_shards, iter_shards
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


def test_encode_panel_one_row_per_loan_with_aligned_columns():
    panel = _panel(n_loans=6, n_months=5)
    tok = KVTTokenizer(CONFIG).fit(panel)
    df = encode_panel(tok, panel)
    assert len(df) == panel.loan_id.nunique()                  # one row per loan
    row = df.iloc[0]
    cols = ("input_ids", "event_index", "field_type", "branch")
    assert all(len(row[c]) == row.n_tokens for c in cols)      # all four columns aligned
    lid = row[tok.id_col]
    assert list(row.input_ids) == tok.encode(panel[panel.loan_id == lid])   # ids reproduce encode()
    assert (df.n_events == 5).all()                            # 5 monthly blocks per loan


def test_iter_shards_keeps_loans_whole_and_covers_all():
    panel = _panel(n_loans=8, n_months=4)
    tok = KVTTokenizer(CONFIG).fit(panel)
    shards = list(iter_shards(tok, panel, shard_size=3))
    assert len(shards) == 3                                    # 3 + 3 + 2
    seen = [set(s[tok.id_col]) for s in shards]
    assert sum(len(s) for s in seen) == 8                      # no overlap
    assert set().union(*seen) == set(panel.loan_id)            # every loan present exactly once


def test_parallel_encode_matches_sequential(tmp_path):
    panel = _panel(n_loans=8, n_months=4)
    tok = KVTTokenizer(CONFIG).fit(panel)
    tok_path = str(tmp_path / "tok.json")
    tok.save(tok_path)

    seq_names, seq_loans, seq_tokens = encode_to_shards(
        tok, tok_path, panel, str(tmp_path / "seq"), shard_size=3, workers=0)
    par_names, par_loans, par_tokens = encode_to_shards(
        tok, tok_path, panel, str(tmp_path / "par"), shard_size=3, workers=2)

    assert seq_names == par_names                       # deterministic shard names
    assert (seq_loans, seq_tokens) == (par_loans, par_tokens)
    assert seq_loans == 8
    seq0 = storage.read_parquet(storage.join(str(tmp_path / "seq"), seq_names[0]))
    par0 = storage.read_parquet(storage.join(str(tmp_path / "par"), par_names[0]))
    assert list(seq0.iloc[0].input_ids) == list(par0.iloc[0].input_ids)


def test_shard_survives_parquet_roundtrip(tmp_path):
    panel = _panel(n_loans=4, n_months=5)
    tok = KVTTokenizer(CONFIG).fit(panel)
    df = encode_panel(tok, panel)
    path = str(tmp_path / "shard-00000.parquet")
    storage.write_parquet(df, path)
    back = storage.read_parquet(path)
    assert len(back) == len(df)
    assert list(back.iloc[0].input_ids) == list(df.iloc[0].input_ids)   # ragged lists survive
    assert list(back.iloc[0].event_index) == list(df.iloc[0].event_index)
