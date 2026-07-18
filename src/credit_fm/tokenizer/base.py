# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Abstract tokenizer interface. All concrete tokenizers implement encode/decode,
vocab_size, and save/load.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class BaseTokenizer(ABC):
    @abstractmethod
    def encode(self, loan_panel) -> list[int]:
        """Encode a single loan panel into token IDs."""
        raise NotImplementedError

    @abstractmethod
    def decode(self, tokens: list[int]):
        """Reconstruct an (approximate) loan panel from token IDs."""
        raise NotImplementedError

    @property
    @abstractmethod
    def vocab_size(self) -> int: ...

    @abstractmethod
    def save(self, path: str | Path) -> None: ...

    @classmethod
    @abstractmethod
    def load(cls, path: str | Path) -> 'BaseTokenizer': ...
