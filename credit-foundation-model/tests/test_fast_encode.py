# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""encode_panel_fast (vector engine) must match encode_panel token-for-token.

Covers the tricky semantics: NA values, exact zeros, unseen categories (UNK), loans longer than
``max_events`` (tail truncation + profile from the first *kept* row), missing configured columns,
and the calendar token.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from credit_fm.data.encode import encode_panel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.tokenizer.fast_encode import encode_panel_fast

CONFIG = {
    "id_col": "loan_id",
    "time_col": "reporting_date",
    "time_field": "loan_age",
    "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
    "event": {"numeric": ["current_interest_rate", "current_upb"], "categorical": ["state"]},
    "n_bins": 8,
    "max_categories": 64,
    "max_events": 5,          # small so truncation is exercised
    "calendar": "yearquarter",
}


def _panel(n_loans=40, n_months=9) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows = []
    for lid in range(n_loans):
        ltv = float(rng.integers(40, 97)) if lid % 7 else np.nan       # some NA profiles
        chan = ["R", "C", "B"][lid % 3]
        rate = float(rng.uniform(3, 8))
        st = ["CA", "TX", "NY", "FL"][lid % 4]
        months = n_months if lid % 5 else 3                            # some short loans
        for m in range(months):
            rows.append({
                "loan_id": f"L{lid:03d}",
                "reporting_date": f"20{15 + m // 12}-{m % 12 + 1:02d}-28",
                "loan_age": 12 + m,
                "original_ltv": ltv,
                "channel": chan,
                "current_interest_rate": rate if m % 4 else 0.0,       # exact zeros
                "current_upb": 200_000 - m * 1_000 if m % 6 else np.nan,  # NAs
                "state": st,
            })
    return pd.DataFrame(rows)


def _assert_identical(fast: pd.DataFrame, slow: pd.DataFrame):
    fast = fast.sort_values("loan_id").reset_index(drop=True)
    slow = slow.sort_values("loan_id").reset_index(drop=True)
    assert list(fast.loan_id) == list(slow.loan_id)
    assert list(fast.n_tokens) == list(slow.n_tokens)
    assert list(fast.n_events) == list(slow.n_events)
    for col in ("input_ids", "event_index", "field_type", "branch"):
        for a, b in zip(fast[col], slow[col]):
            assert list(a) == list(b), col


def test_fast_matches_slow_including_truncation_na_zero():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    _assert_identical(encode_panel_fast(tok, panel), encode_panel(tok, panel))


def test_unseen_categories_map_to_unk():
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel[panel.state != "FL"])          # FL unseen at fit time
    _assert_identical(encode_panel_fast(tok, panel), encode_panel(tok, panel))


def test_missing_configured_column_is_all_na():
    panel = _panel().drop(columns=["current_upb"])                      # configured but absent
    tok = KVTTokenizer(CONFIG).fit(_panel())
    _assert_identical(encode_panel_fast(tok, panel), encode_panel(tok, panel))


def test_year_calendar_and_no_calendar():
    for mode in ("year", "none"):
        cfg = dict(CONFIG, calendar=mode)
        panel = _panel()
        tok = KVTTokenizer(cfg).fit(panel)
        _assert_identical(encode_panel_fast(tok, panel), encode_panel(tok, panel))
