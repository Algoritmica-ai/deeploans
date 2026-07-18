# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Categorical value -> value token, fit on TRAIN only.

Produces the *value* part of a KVT token (e.g. the ``R`` in ``channel=R``). The vocabulary is the
set of categories seen in training (optionally capped / min-count filtered); anything unseen at
val/test/inference time maps to ``"UNK"`` and missing maps to ``"NA"``. High-cardinality fields
(e.g. zip / MSA) should set ``max_categories`` to bound the vocabulary.
"""

from __future__ import annotations

import pandas as pd


class CategoricalTokenizer:
    UNK = "UNK"
    NA = "NA"

    def __init__(self, max_categories: int = 256, min_count: int = 1):
        self.max_categories = max_categories
        self.min_count = min_count
        self.categories_: list[str] = []

    def fit(self, values) -> 'CategoricalTokenizer':
        """Learn the kept category set from training values (most frequent first)."""
        counts = pd.Series(values).dropna().astype(str).value_counts()
        counts = counts[counts >= self.min_count]
        self.categories_ = list(counts.index[:self.max_categories])
        return self

    def transform(self, value) -> str:
        """One value → its category label (``UNK`` if unseen, ``NA`` if missing)."""
        if pd.isna(value):
            return self.NA
        s = str(value)
        return s if s in set(self.categories_) else self.UNK

    def transform_series(self, values) -> pd.Series:
        """Vectorised: a column of values → a column of category labels."""
        known = set(self.categories_)
        return pd.Series(values).map(
            lambda x: self.NA if pd.isna(x) else (str(x) if str(x) in known else self.UNK))

    def vocab(self) -> list[str]:
        """All category labels this field can emit."""
        return list(self.categories_) + [self.UNK, self.NA]
