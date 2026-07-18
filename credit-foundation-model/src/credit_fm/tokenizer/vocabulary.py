# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Unified vocabulary across keys, values, and special tokens.

Built on TRAIN only and serialized to JSON so val/test/inference reuse the exact same ids.
Special tokens occupy the first ids; field tokens (e.g. ``"channel=R"``, ``"original_ltv=4"``)
are added during the tokenizer's fit. JSON (de)serialization + basic stats.
"""

from __future__ import annotations

import json
from pathlib import Path

SPECIAL_TOKENS = ['[PAD]', '[BOS]', '[EOS]', '[MASK]', '[UNK]',
                  '[USR]', '[EVT]', '[EVT_START]', '[EVT_END]']


class Vocabulary:
    """Bidirectional token <-> id map. `[UNK]` is the fallback for unseen tokens."""

    def __init__(self) -> None:
        self.token_to_id: dict[str, int] = {}
        self.id_to_token: dict[int, str] = {}
        for tok in SPECIAL_TOKENS:
            self.add(tok)

    def add(self, token: str) -> int:
        """Register a token (idempotent); return its id."""
        if token in self.token_to_id:
            return self.token_to_id[token]
        idx = len(self.token_to_id)
        self.token_to_id[token] = idx
        self.id_to_token[idx] = token
        return idx

    def encode(self, token: str) -> int:
        """Token -> id, falling back to `[UNK]` for anything unseen."""
        return self.token_to_id.get(token, self.token_to_id['[UNK]'])

    def decode(self, idx: int) -> str:
        return self.id_to_token[idx]

    @property
    def size(self) -> int:
        return len(self.token_to_id)

    def to_json(self, path) -> None:
        ordered = [self.id_to_token[i] for i in range(self.size)]
        Path(path).write_text(json.dumps({"tokens": ordered}, indent=2))

    @classmethod
    def from_json(cls, path) -> 'Vocabulary':
        tokens = json.loads(Path(path).read_text())["tokens"]
        vocab = cls.__new__(cls)                       # skip __init__ (which re-adds specials)
        vocab.token_to_id = {t: i for i, t in enumerate(tokens)}
        vocab.id_to_token = {i: t for i, t in enumerate(tokens)}
        return vocab

    def stats(self) -> dict:
        n_special = len(SPECIAL_TOKENS)
        return {"size": self.size, "special": n_special, "field_tokens": self.size - n_special}
