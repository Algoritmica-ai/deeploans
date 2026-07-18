# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Batch collation for the hierarchical MLM model — flat ``(B, L)`` padding.

Takes a list of unpadded per-loan samples (from :class:`~credit_fm.data.dataset.CreditSequenceDataset`)
and assembles one rectangular batch the model can consume:

1. **mask** each loan's sequence with :func:`~credit_fm.training.masking.mask_tokens` (dynamic,
   per-batch — RoBERTa-style); pass a ``seed`` for deterministic val/test masking.
2. **pad** every field to the batch's longest sequence — ids with ``pad_id`` (``[PAD]``), ``labels``
   with ``IGNORE_INDEX`` (-100), and the structural metadata (``event_index``/``field_type``/
   ``branch``) with ``-1`` — and build an ``attention_mask`` (1 = real, 0 = pad).

Flat ``(B, L)`` layout (decided 27 Jun): the event encoder pools per month using ``event_index``
rather than a nested ``(B, E, T)`` axis — less padding, and the shard contract already carries the
indices. ``PackedCollator`` (varlen, no padding) is the deferred M3 throughput variant.
"""

from __future__ import annotations

import numpy as np
import torch

from credit_fm.training.masking import IGNORE_INDEX, mask_tokens

# metadata columns padded with -1 and carried through to the model untouched
_META = ("event_index", "field_type", "branch")


class MLMCollator:
    """Collate unpadded loan samples into a padded, masked ``(B, L)`` batch.

    Args:
        vocab_size: vocabulary size (for random-token replacement in masking).
        pad_id: id used to pad ``input_ids`` (``[PAD]`` = 0).
        mask: apply MLM masking. ``False`` (inference/embedding) leaves ids untouched and all
            ``labels`` = ``IGNORE_INDEX``.
        seed: ``None`` → fresh random masking each batch (train); an int → deterministic masking
            (val/test, so loss is comparable across epochs — use an unshuffled loader).
        token_rate, event_rate, type_rate: the three masking-strategy rates (see ``mask_tokens``).
    """

    def __init__(self, vocab_size: int, pad_id: int = 0, mask: bool = True,
                 seed: int | None = None, token_rate: float = 0.15,
                 event_rate: float = 0.10, type_rate: float = 0.10):
        self.vocab_size = vocab_size
        self.pad_id = pad_id
        self.mask = mask
        self.seed = seed
        self.rates = {"token_rate": token_rate, "event_rate": event_rate, "type_rate": type_rate}

    def __call__(self, batch: list[dict]) -> dict:
        bsz = len(batch)
        lengths = [int(s["input_ids"].shape[0]) for s in batch]
        max_len = max(lengths)

        input_ids = torch.full((bsz, max_len), self.pad_id, dtype=torch.long)
        labels = torch.full((bsz, max_len), IGNORE_INDEX, dtype=torch.long)
        attention_mask = torch.zeros((bsz, max_len), dtype=torch.long)
        meta = {c: torch.full((bsz, max_len), -1, dtype=torch.long) for c in _META}
        n_events = torch.zeros(bsz, dtype=torch.long)

        rng = np.random.default_rng(self.seed)        # one stream per batch; seeded => reproducible
        for i, s in enumerate(batch):
            n = lengths[i]
            ids = s["input_ids"].numpy()
            if self.mask:
                ids, lab = mask_tokens(
                    ids, s["event_index"].numpy(), s["field_type"].numpy(),
                    vocab_size=self.vocab_size, rng=rng, **self.rates)
                labels[i, :n] = torch.from_numpy(lab)
            input_ids[i, :n] = torch.from_numpy(np.ascontiguousarray(ids))
            attention_mask[i, :n] = 1
            for c in _META:
                meta[c][i, :n] = s[c]
            n_events[i] = int(s["n_events"])

        return {"input_ids": input_ids, "attention_mask": attention_mask, "labels": labels,
                "n_events": n_events, **meta}


class PackedCollator:
    """Deferred M3 throughput variant — varlen packing (no padding) via flash-attn boundaries."""

    def __init__(self, pad_id: int = 0, max_length: int = 512):
        self.pad_id, self.max_length = pad_id, max_length

    def __call__(self, batch):
        raise NotImplementedError
