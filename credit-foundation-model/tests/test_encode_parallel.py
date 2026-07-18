# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""encode_panel_parallel — the spawn-pool path must be byte-identical to the serial encode."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from credit_fm.data.encode import encode_panel, encode_panel_parallel
from credit_fm.tokenizer import KVTTokenizer

CONFIG = {
    "id_col": "loan_id",
    "time_col": "reporting_date",
    "time_field": "loan_age",
    "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
    "event": {"numeric": ["current_interest_rate"], "categorical": []},
    "n_bins": 8,
    "max_categories": 64,
    "max_events": 60,
    "calendar": "yearquarter",
}


def _panel(n_loans=24, n_months=4) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for lid in range(n_loans):
        ltv = int(rng.integers(40, 97))
        chan = rng.choice(["R", "C", "B"])
        for m in range(n_months):
            rows.append({
                "loan_id": f"L{lid:03d}", "reporting_date": f"2020-{m+1:02d}-28",
                "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                "current_interest_rate": float(rng.uniform(3, 8)),
            })
    return pd.DataFrame(rows)


def test_small_panel_falls_back_to_serial(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel())
    path = tmp_path / "tok.json"
    tok.save(path)
    panel = _panel()
    out = encode_panel_parallel(tok, str(path), panel, workers=4)  # < shard_size -> serial
    assert len(out) == panel.loan_id.nunique()


@pytest.mark.slow
def test_parallel_matches_serial(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel())
    path = tmp_path / "tok.json"
    tok.save(path)
    panel = _panel()

    serial = encode_panel(tok, panel).sort_values("loan_id").reset_index(drop=True)
    par = encode_panel_parallel(tok, str(path), panel, workers=2, shard_size=8)
    par = par.sort_values("loan_id").reset_index(drop=True)

    assert len(par) == len(serial)
    assert (par["n_tokens"].to_numpy() == serial["n_tokens"].to_numpy()).all()
    for a, b in zip(par["input_ids"], serial["input_ids"]):
        assert list(a) == list(b)
