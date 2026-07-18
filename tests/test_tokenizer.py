# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Tokenizer M1 unit tests — numeric bucketing, categorical UNK/NA, vocabulary, roundtrip.

Leakage discipline: bins/categories are fit on TRAIN only; out-of-range or unseen values at
test time must map into existing buckets/`UNK`, never create new tokens.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from credit_fm.tokenizer import CategoricalTokenizer, NumericBucketer, Vocabulary


def test_numeric_bucketer_monotonic_and_special_buckets():
    rng = np.random.default_rng(0)
    train = pd.Series(rng.uniform(1, 100, 20_000))
    nb = NumericBucketer(n_bins=10).fit(train)
    assert int(nb.transform(5)) <= int(nb.transform(95))        # monotonic ranking
    assert nb.transform(0) == "0"                                # zero bucket reserved
    assert nb.transform(np.nan) == "NA"                          # missing bucket
    buckets = nb.transform_series(train)
    assert buckets.isin(nb.vocab()).all()                        # every value lands in vocab


def test_numeric_bucketer_no_test_leak():
    train = pd.Series(list(range(1, 11)) * 100, dtype=float)
    nb = NumericBucketer(n_bins=5).fit(train)
    # a value far beyond the training range clamps to the top bucket, never a new one
    assert nb.transform(99_999) == str(nb.n_bins_)
    assert nb.transform(99_999) in nb.vocab()


def test_categorical_unk_and_na():
    train = pd.Series(["R", "C", "R", "B", "R", "C"])
    ct = CategoricalTokenizer().fit(train)
    assert ct.transform("R") == "R"
    assert ct.transform("Z") == "UNK"                            # unseen at test time
    assert ct.transform(np.nan) == "NA"
    assert set(ct.transform_series(pd.Series(["R", "Z", None]))) == {"R", "UNK", "NA"}


def test_categorical_min_count_and_cap():
    train = pd.Series(["A"] * 10 + ["B"] * 10 + ["rare"])        # 'rare' appears once
    ct = CategoricalTokenizer(max_categories=10, min_count=2).fit(train)
    assert "rare" not in ct.categories_                          # filtered by min_count
    assert ct.transform("rare") == "UNK"


def test_vocabulary_add_encode_decode_and_json(tmp_path):
    v = Vocabulary()
    assert v.encode("[PAD]") == 0                                # specials first
    i = v.add("channel=R")
    assert v.add("channel=R") == i                              # idempotent
    assert v.decode(v.encode("channel=R")) == "channel=R"
    assert v.encode("never-seen-token") == v.encode("[UNK]")     # OOV → [UNK]
    p = tmp_path / "vocab.json"
    v.to_json(p)
    v2 = Vocabulary.from_json(p)
    assert v2.size == v.size and v2.encode("channel=R") == i     # exact ids survive a save/load


def test_fused_token_roundtrip_is_lossless():
    """Fit field tokenizers, register fused 'field=value' tokens, encode→decode a row exactly."""
    train = pd.DataFrame({
        "original_interest_rate": list(np.linspace(2.0, 8.0, 200)),
        "channel": (["R", "C", "B"] * 67)[:200],
    })
    nb = NumericBucketer(n_bins=8).fit(train["original_interest_rate"])
    ct = CategoricalTokenizer().fit(train["channel"])
    vocab = Vocabulary()
    for label in nb.vocab():
        vocab.add(f"original_interest_rate={label}")
    for label in ct.vocab():
        vocab.add(f"channel={label}")

    row_tokens = [
        f"original_interest_rate={nb.transform(6.5)}",
        f"channel={ct.transform('R')}",
    ]
    ids = [vocab.encode(t) for t in row_tokens]
    assert [vocab.decode(i) for i in ids] == row_tokens          # 100% lossless roundtrip
    assert vocab.stats()["field_tokens"] == len(nb.vocab()) + len(ct.vocab())


def test_numeric_bucketer_anchor_forces_boundary():
    rng = np.random.default_rng(0)
    train = pd.Series(rng.uniform(40, 97, 10_000))          # LTV-like
    nb = NumericBucketer(n_bins=10, anchors=[80, 90]).fit(train)
    assert any(abs(e - 80.0) < 1e-9 for e in nb.edges)      # 80 is an actual bin edge
    assert nb.transform(79.9) != nb.transform(80.1)         # the cliff is not blurred
