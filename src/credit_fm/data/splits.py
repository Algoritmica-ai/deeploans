# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Loan-stratified temporal split for credit panel data.

Splitting a credit panel by *row* leaks loans across train/test: cutoff 6 of a loan can
land in train while cutoff 7 lands in test, so the model effectively sees the same loan
twice and the test score is fake. This module instead splits by ``loan_id`` (every cutoff
of a loan stays in one split) and orders the split by **origination date** (train < val <
test in time), so evaluation mirrors production — a model trained on older loans is tested
on newer ones.

Label-horizon leakage (e.g. ``default_within_6m`` needing the cutoff to be >= 6 months
before the panel end) is handled at the label-generator layer, not here.
"""

from __future__ import annotations

import pandas as pd

SPLITS = ("train", "val", "test")


def temporal_loan_split(
    origination: pd.Series,
    fractions: tuple[float, float, float] = (0.8, 0.1, 0.1),
) -> dict:
    """Assign each loan to train/val/test by origination order.

    Args:
        origination: one row per loan, indexed by ``loan_id``, value = origination date.
        fractions: ``(train, val, test)`` fractions; must sum to 1.0.

    Returns:
        ``{loan_id: split}``. Deterministic: loans are sorted by
        ``(origination_date, loan_id)`` and partitioned positionally, so the earliest-
        originated ``train`` fraction is train, the next is val, and the latest is test.
    """
    if len(fractions) != 3 or abs(sum(fractions) - 1.0) > 1e-9:
        raise ValueError(f"fractions must be 3 values summing to 1.0, got {fractions}")
    if origination.index.duplicated().any():
        raise ValueError("origination must have one row per loan_id (no duplicates)")
    if origination.isna().any():
        raise ValueError("origination has missing values; clean or drop before splitting")

    # Deterministic order: origination first, loan_id as the tie-breaker.
    id_name = origination.index.name or "loan_id"
    ordered = (
        origination.rename("origination")
        .rename_axis(id_name)
        .reset_index()
        .sort_values(["origination", id_name], kind="mergesort")
    )
    loan_ids = ordered[id_name].tolist()

    n = len(loan_ids)
    n_train = int(fractions[0] * n)
    n_val = int(fractions[1] * n)
    bounds = {
        "train": loan_ids[:n_train],
        "val": loan_ids[n_train : n_train + n_val],
        "test": loan_ids[n_train + n_val :],
    }
    return {lid: split for split in SPLITS for lid in bounds[split]}
