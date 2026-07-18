# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""DataModule — wires the shard datasets + collators into train/val/test ``DataLoader``s.

One config-driven object so the training script never touches files, workers, or masking policy.
The split in masking policy is deliberate:

* **train** — shuffled; **dynamic** masking (fresh each batch, RoBERTa-style) → ``seed=None``.
* **val / test** — unshuffled; **deterministic** masking (fixed ``val_seed``) so the MLM loss is
  comparable across epochs.

M2-toy → M3-full is a *settings* change (``limit``, ``batch_size``, ``num_workers``), not code.
``vocab_size`` defaults to the value recorded in the train shards' ``manifest.json``.
"""

from __future__ import annotations

import json

import fsspec
from torch.utils.data import DataLoader

from credit_fm.utils import storage

from .collators import MLMCollator
from .dataset import CreditSequenceDataset


class CreditDataModule:
    """Build train/val/test loaders over encode-once shard directories.

    Args:
        train_dir: shard directory for the train split (local or ``gs://``); required.
        val_dir, test_dir: shard directories for the eval splits; optional.
        vocab_size: vocabulary size for masking; ``None`` → read from the train manifest.
        batch_size: train batch size; ``eval_batch_size`` defaults to it.
        num_workers, pin_memory: standard ``DataLoader`` knobs.
        limit: cap loans per split (M2 toy run).
        val_seed: fixed seed for deterministic val/test masking.
        token_rate, event_rate, type_rate: masking-strategy rates passed to :class:`MLMCollator`.
    """

    def __init__(self, train_dir: str, val_dir: str | None = None, test_dir: str | None = None,
                 *, vocab_size: int | None = None, batch_size: int = 32,
                 eval_batch_size: int | None = None, num_workers: int = 0, pad_id: int = 0,
                 limit: int | None = None, val_seed: int = 1234, token_rate: float = 0.15,
                 event_rate: float = 0.10, type_rate: float = 0.10, pin_memory: bool = False,
                 key: str | None = storage.GCS_DEFAULT_KEY):
        self.batch_size = batch_size
        self.eval_batch_size = eval_batch_size or batch_size
        self.num_workers = num_workers
        self.pin_memory = pin_memory

        self.train = CreditSequenceDataset(train_dir, limit=limit, key=key)
        self.val = CreditSequenceDataset(val_dir, limit=limit, key=key) if val_dir else None
        self.test = CreditSequenceDataset(test_dir, limit=limit, key=key) if test_dir else None

        if vocab_size is None:
            vocab_size = self._manifest_vocab_size(train_dir, key)
        rates = {"token_rate": token_rate, "event_rate": event_rate, "type_rate": type_rate}
        # train: dynamic masking (seed=None); eval: deterministic masking (fixed seed)
        self._train_collator = MLMCollator(vocab_size, pad_id=pad_id, seed=None, **rates)
        self._eval_collator = MLMCollator(vocab_size, pad_id=pad_id, seed=val_seed, **rates)
        self.vocab_size = vocab_size

    @staticmethod
    def _manifest_vocab_size(shard_dir: str, key: str | None) -> int:
        storage.ensure_auth(shard_dir, key)
        with fsspec.open(storage.join(shard_dir, "manifest.json"), "r") as f:
            vocab_size = json.load(f).get("vocab_size")
        if vocab_size is None:
            raise ValueError(f"vocab_size not in {shard_dir}/manifest.json — pass vocab_size=...")
        return int(vocab_size)

    def train_dataloader(self) -> DataLoader:
        # Under DDP (a live process group with >1 rank) each rank streams a disjoint shard of the
        # data via DistributedSampler; otherwise the plain shuffled loader (single-GPU path, unchanged).
        import torch.distributed as dist
        if dist.is_available() and dist.is_initialized() and dist.get_world_size() > 1:
            from torch.utils.data.distributed import DistributedSampler
            sampler = DistributedSampler(self.train, shuffle=True)   # set_epoch driven by _cycle
            return DataLoader(self.train, batch_size=self.batch_size, sampler=sampler,
                              num_workers=self.num_workers, pin_memory=self.pin_memory,
                              collate_fn=self._train_collator)
        return DataLoader(self.train, batch_size=self.batch_size, shuffle=True,
                          num_workers=self.num_workers, pin_memory=self.pin_memory,
                          collate_fn=self._train_collator)

    def _eval_loader(self, ds, split: str) -> DataLoader:
        if ds is None:
            raise ValueError(f"no {split} split — pass {split}_dir to CreditDataModule")
        return DataLoader(ds, batch_size=self.eval_batch_size, shuffle=False,
                          num_workers=self.num_workers, pin_memory=self.pin_memory,
                          collate_fn=self._eval_collator)

    def val_dataloader(self) -> DataLoader:
        return self._eval_loader(self.val, "val")

    def test_dataloader(self) -> DataLoader:
        return self._eval_loader(self.test, "test")
