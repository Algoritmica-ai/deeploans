# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Three-source MLM masking for credit-event sequences.

The pretraining objective hides part of each loan's token sequence and asks the model to
reconstruct it. *What* we hide is deliberate — three complementary strategies, each exercising a
different branch of the hierarchical encoder:

* **token** (``token_rate``) — individual field tokens at random → local field/value structure.
* **event** (``event_rate``) — every token of a whole month → forces temporal inference (History).
* **type**  (``type_rate``)  — a whole field across all months → forces cross-field inference (Event).

The three selections are unioned. Selected positions are corrupted BERT-style
(``mask_prob`` → ``[MASK]`` / ``random_prob`` → a random field token / remainder unchanged) so the
model can never assume a masked slot is literally ``[MASK]``. The 9 structural specials
(``[PAD]``/``[BOS]``/``[EOS]``/``[USR]``/``[EVT_START]``/``[EVT_END]`` …) occupy ids ``0..N-1`` and
are never masked.

Pure NumPy, framework-free: one loan in, ``(corrupted_ids, labels)`` out, with ``labels`` = the
original id at masked positions and :data:`IGNORE_INDEX` (-100) everywhere else (the standard
cross-entropy convention). Masking is applied per batch (dynamic, RoBERTa-style); pass a seeded
``rng`` for deterministic val/test masking.
"""

from __future__ import annotations

import numpy as np

from credit_fm.tokenizer.vocabulary import SPECIAL_TOKENS

IGNORE_INDEX = -100                                   # cross-entropy ignore index (PyTorch default)
N_SPECIAL_TOKENS = len(SPECIAL_TOKENS)                # specials occupy ids 0..N-1
MASK_TOKEN_ID = SPECIAL_TOKENS.index('[MASK]')        # id of the [MASK] token


def mask_tokens(
    input_ids,
    event_index,
    field_type,
    *,
    vocab_size: int,
    token_rate: float = 0.15,
    event_rate: float = 0.10,
    type_rate: float = 0.10,
    mask_prob: float = 0.80,
    random_prob: float = 0.10,
    n_special: int = N_SPECIAL_TOKENS,
    mask_token_id: int = MASK_TOKEN_ID,
    rng: np.random.Generator | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Corrupt one loan's token sequence for masked-language-modelling.

    Args:
        input_ids:   ``(L,)`` token ids for one loan.
        event_index: ``(L,)`` event/month each token belongs to; ``-1`` for profile/structural.
        field_type:  ``(L,)`` field-type id each token encodes; ``-1`` for structural tokens.
        vocab_size:  vocabulary size (exclusive upper bound for random replacements).
        token_rate, event_rate, type_rate: selection probabilities for the three strategies.
        mask_prob, random_prob: of the selected positions, the fraction set to ``[MASK]`` / to a
            random field token (the remainder is left unchanged). Must sum to ``<= 1``.
        n_special:   number of special tokens; ids ``0..n_special-1`` are never masked.
        mask_token_id: id of the ``[MASK]`` token.
        rng:         NumPy generator; pass a seeded one for deterministic (val/test) masking.

    Returns:
        ``(corrupted_ids, labels)`` — both ``(L,)`` int arrays. ``labels`` holds the original id at
        masked positions and :data:`IGNORE_INDEX` everywhere else.
    """
    if rng is None:
        rng = np.random.default_rng()
    ids = np.asarray(input_ids).copy()
    ev = np.asarray(event_index)
    ft = np.asarray(field_type)
    length = ids.shape[0]

    maskable = ids >= n_special                       # specials (ids 0..n_special-1) never masked
    selected = np.zeros(length, dtype=bool)

    # 1) token-level — independent coin flip per maskable position
    if token_rate > 0:
        selected |= maskable & (rng.random(length) < token_rate)

    # 2) event-level — pick whole months, mask all their maskable tokens
    if event_rate > 0:
        events = np.unique(ev[ev >= 0])
        chosen = events[rng.random(events.shape[0]) < event_rate]
        if chosen.size:
            selected |= maskable & np.isin(ev, chosen)

    # 3) type-level — pick whole field-types, mask them across all months
    if type_rate > 0:
        types = np.unique(ft[ft >= 0])
        chosen = types[rng.random(types.shape[0]) < type_rate]
        if chosen.size:
            selected |= maskable & np.isin(ft, chosen)

    labels = np.full(length, IGNORE_INDEX, dtype=np.int64)
    labels[selected] = ids[selected]

    # BERT 80/10/10 corruption on the selected positions
    sel = np.flatnonzero(selected)
    if sel.size:
        u = rng.random(sel.size)
        ids[sel[u < mask_prob]] = mask_token_id
        rand = sel[(u >= mask_prob) & (u < mask_prob + random_prob)]
        if rand.size:                                 # random *field* token, never a special
            ids[rand] = rng.integers(n_special, vocab_size, size=rand.size)
        # remaining selected positions keep their original id (still predicted, via labels)

    return ids, labels
