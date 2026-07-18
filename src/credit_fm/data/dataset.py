# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""PyTorch datasets over credit panels.

``CreditSequenceDataset`` is the M2 data layer: a random-access reader over the **encode-once**
token-id shards written by ``scripts/encode_dataset.py``. One item = one loan's *unpadded* tensors
(``input_ids`` + the ``event_index`` / ``field_type`` / ``branch`` metadata the hierarchical model
and MLM masking read). Padding and masking happen later, in the collator (batch level), so this
stays a thin, fast read layer — no tokenization on the hot path.

``CreditPanelDataset`` is the older tokenize-on-the-fly scaffold, kept for reference.
"""

from __future__ import annotations

import json

import fsspec
import pandas as pd
import torch
from torch.utils.data import Dataset

from credit_fm.utils import storage

_RAGGED = ("input_ids", "event_index", "field_type", "branch")


class CreditSequenceDataset(Dataset):
    """Random-access reader over encode-once token-id shards (one loan per item).

    Reads ``manifest.json`` + ``shard-*.parquet`` (written by ``scripts/encode_dataset.py``) from a
    local path or ``gs://`` URL into memory, and returns one loan's unpadded ``torch.long`` tensors.

    Args:
        shard_dir: directory holding ``manifest.json`` and the shard parquet files.
        limit: keep only the first N loans (for the M2 toy run); ``None`` = all.
        key: GCS service-account key path (auto-applied for ``gs://``).
    """

    def __init__(self, shard_dir: str, limit: int | None = None,
                 key: str | None = storage.GCS_DEFAULT_KEY):
        storage.ensure_auth(shard_dir, key)
        with fsspec.open(storage.join(shard_dir, "manifest.json"), "r") as f:
            manifest = json.load(f)
        frames = [storage.read_parquet(storage.join(shard_dir, s)) for s in manifest["shards"]]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if limit is not None:
            df = df.iloc[:limit]
        self.shard_dir = shard_dir
        self._cols = {c: df[c].tolist() for c in _RAGGED}
        self._n_events = df["n_events"].tolist() if len(df) else []
        self._len = len(df)

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, idx: int) -> dict:
        item = {c: torch.tensor(self._cols[c][idx], dtype=torch.long) for c in _RAGGED}
        item["n_events"] = int(self._n_events[idx])
        return item


class CreditPanelDataset(Dataset):
    """Deprecated tokenize-on-the-fly scaffold — superseded by :class:`CreditSequenceDataset`."""

    def __init__(self, parquet_path: str, tokenizer, labels=None):
        self.parquet_path, self.tokenizer, self.labels = parquet_path, tokenizer, labels

    def __len__(self) -> int:
        raise NotImplementedError

    def __getitem__(self, idx):
        raise NotImplementedError
