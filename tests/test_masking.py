# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""MLM masking unit tests — selection rates, BERT 80/10/10, specials safety, determinism.

A toy encoded loan: ``[BOS] [USR]`` then ``n_events`` event blocks, each
``[EVT_START] <fields> [EVT_END]``, then ``[EOS]``. Field tokens get ids ``>= N_SPECIAL_TOKENS``;
specials/structural tokens carry ``event_index == -1`` and ``field_type == -1``.
"""

from __future__ import annotations

import numpy as np

from credit_fm.tokenizer.vocabulary import SPECIAL_TOKENS
from credit_fm.training.masking import (
    IGNORE_INDEX,
    MASK_TOKEN_ID,
    N_SPECIAL_TOKENS,
    mask_tokens,
)

VOCAB_SIZE = 50
BOS, USR = SPECIAL_TOKENS.index('[BOS]'), SPECIAL_TOKENS.index('[USR]')
EOS = SPECIAL_TOKENS.index('[EOS]')
EVT_S, EVT_E = SPECIAL_TOKENS.index('[EVT_START]'), SPECIAL_TOKENS.index('[EVT_END]')


def _toy_loan(n_events=200, n_fields=5):
    """Build (input_ids, event_index, field_type) for one toy loan."""
    rows = [(BOS, -1, -1), (USR, -1, -1)]               # (token_id, event_index, field_type)
    for e in range(n_events):
        rows.append((EVT_S, -1, -1))
        rows += [(N_SPECIAL_TOKENS + f, e, f) for f in range(n_fields)]   # one token per field-type
        rows.append((EVT_E, -1, -1))
    rows.append((EOS, -1, -1))
    ids, ev, ft = (np.array(col) for col in zip(*rows, strict=True))
    return ids, ev, ft


def test_token_rate_selects_expected_fraction():
    ids, ev, ft = _toy_loan()                            # 200*5 = 1000 maskable field tokens
    rng = np.random.default_rng(0)
    _, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                            token_rate=0.15, event_rate=0.0, type_rate=0.0, rng=rng)
    n_maskable = int((ids >= N_SPECIAL_TOKENS).sum())
    frac = (labels != IGNORE_INDEX).sum() / n_maskable
    assert abs(frac - 0.15) < 0.03                       # ~15% of maskable selected


def test_specials_are_never_masked_or_labelled():
    ids, ev, ft = _toy_loan(n_events=50)
    rng = np.random.default_rng(1)
    corrupted, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                                    token_rate=0.5, event_rate=0.5, type_rate=0.5, rng=rng)
    specials = ids < N_SPECIAL_TOKENS
    assert np.array_equal(corrupted[specials], ids[specials])   # structural tokens untouched
    assert (labels[specials] == IGNORE_INDEX).all()             # never a prediction target


def test_labels_match_originals_only_at_selected():
    ids, ev, ft = _toy_loan(n_events=30)
    rng = np.random.default_rng(2)
    _, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE, rng=rng)
    sel = labels != IGNORE_INDEX
    assert np.array_equal(labels[sel], ids[sel])         # label is the *original* id
    assert sel.sum() > 0


def test_whole_event_masking_hits_entire_months():
    ids, ev, ft = _toy_loan(n_events=20)
    rng = np.random.default_rng(3)
    _, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                            token_rate=0.0, type_rate=0.0, event_rate=1.0, rng=rng)
    # every event chosen → every maskable token selected; and selection is whole-event
    maskable = ids >= N_SPECIAL_TOKENS
    assert (labels[maskable] != IGNORE_INDEX).all()
    for e in np.unique(ev[ev >= 0]):
        block = (ev == e) & maskable
        assert (labels[block] != IGNORE_INDEX).all()     # all-or-nothing per event


def test_whole_field_type_masking_hits_field_across_all_events():
    ids, ev, ft = _toy_loan(n_events=40)
    rng = np.random.default_rng(4)
    _, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                            token_rate=0.0, event_rate=0.0, type_rate=1.0, rng=rng)
    for f in np.unique(ft[ft >= 0]):
        col = ft == f
        assert (labels[col] != IGNORE_INDEX).all()       # the field is masked in every month


def test_bert_80_10_10_split_and_no_special_injection():
    ids, ev, ft = _toy_loan(n_events=400)                # 2000 field tokens
    rng = np.random.default_rng(5)
    corrupted, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                                    token_rate=1.0, event_rate=0.0, type_rate=0.0, rng=rng)
    sel = labels != IGNORE_INDEX
    frac_mask = (corrupted[sel] == MASK_TOKEN_ID).mean()
    assert abs(frac_mask - 0.80) < 0.04                  # ~80% become [MASK]
    # random replacements are field tokens, never specials/structural
    changed = sel & (corrupted != MASK_TOKEN_ID) & (corrupted != ids)
    assert (corrupted[changed] >= N_SPECIAL_TOKENS).all()


def test_determinism_same_seed_same_result():
    ids, ev, ft = _toy_loan(n_events=60)
    a = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE, rng=np.random.default_rng(7))
    b = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE, rng=np.random.default_rng(7))
    c = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE, rng=np.random.default_rng(8))
    assert np.array_equal(a[0], b[0]) and np.array_equal(a[1], b[1])   # seed reproducible
    assert not np.array_equal(a[0], c[0])                              # different seed differs


def test_no_maskable_positions_is_a_noop():
    ids = np.array([BOS, USR, EVT_S, EVT_E, EOS])        # all specials, nothing maskable
    ev = np.full(ids.shape, -1)
    ft = np.full(ids.shape, -1)
    corrupted, labels = mask_tokens(ids, ev, ft, vocab_size=VOCAB_SIZE,
                                    rng=np.random.default_rng(9))
    assert np.array_equal(corrupted, ids)
    assert (labels == IGNORE_INDEX).all()
