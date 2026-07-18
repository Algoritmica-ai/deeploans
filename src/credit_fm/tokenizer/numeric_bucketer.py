# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Quantile bucketing for one continuous field.

Produces the *value* part of a KVT token (e.g. the ``4`` in ``original_ltv=4``). Bucket labels:
``"0"`` = exact zero, ``"1".."n_bins"`` = quantile bins of the non-zero training values,
``"NA"`` = missing. Edges are fit on **training data only** (never leak from val/test), and a
value beyond the training range is clamped into the edge bucket — it can never create a new one.

``anchors`` forces a bin boundary at known thresholds (e.g. LTV 80/90/95/97, DTI 43/45) so
razor-thin regulatory cliffs aren't blurred inside a quantile bin — the granularity an XGBoost
baseline gets for free. ``n_bins`` can be raised per high-signal field.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


class NumericBucketer:
    NA = "NA"

    def __init__(self, n_bins: int = 16, anchors=None):
        self.n_bins = n_bins
        self.anchors = list(anchors) if anchors else []   # forced cut-points (e.g. LTV 80)
        self.edges = None        # bin edges of non-zero training values (quantile + anchors)
        self.n_bins_ = 0         # actual #bins after de-duplicating edges

    def fit(self, values) -> 'NumericBucketer':
        """Compute quantile bin edges from training values (NaNs and zeros excluded)."""
        v = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype="float64")
        v = v[~np.isnan(v)]
        nz = v[v != 0.0]
        if nz.size >= 2 and np.unique(nz).size >= 2:
            edges = np.quantile(nz, np.linspace(0.0, 1.0, self.n_bins + 1))
            anc = [a for a in self.anchors if nz.min() < a < nz.max()]   # only in-range anchors
            self.edges = np.unique(np.concatenate([edges, anc]))         # force boundary at cliffs
            self.n_bins_ = max(self.edges.size - 1, 1)
        else:                                           # constant / all-zero / all-missing field
            self.edges = None
            self.n_bins_ = 1
        return self

    def _bucket(self, x: float) -> str:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return self.NA
        if x == 0.0:
            return "0"
        if self.edges is None:
            return "1"
        # interior edges → buckets 1..n_bins_ ; out-of-range values clamp to the edge bucket
        i = int(np.clip(np.searchsorted(self.edges[1:-1], x, side="right") + 1, 1, self.n_bins_))
        return str(i)

    def transform(self, value) -> str:
        """One value → its bucket label string."""
        return self._bucket(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])

    def transform_series(self, values) -> pd.Series:
        """Vectorised: a column of values → a column of bucket labels."""
        return pd.to_numeric(pd.Series(values), errors="coerce").map(self._bucket)

    def vocab(self) -> list[str]:
        """All bucket labels this field can emit."""
        return ["0"] + [str(i) for i in range(1, self.n_bins_ + 1)] + [self.NA]
