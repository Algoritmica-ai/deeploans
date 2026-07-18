# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Main key-value-time (KVT) tokenizer.

Composes the per-field tokenizers (numeric bucketing, categorical) into one loan-level encoder.
Fields are tagged to a **branch**: *profile* (static origination facts, emitted once) and *event*
(per-month dynamic facts, emitted per monthly row). Tokens are **fused** ``field=value`` strings
(NVIDIA-TFM style) mapped to ids via the shared :class:`Vocabulary`.

A loan encodes to::

    [BOS] [USR]
      <profile tokens: original_ltv=4, channel=R, ...>           # once, from the loan's first row
      [EVT_START] t=<age_bin> cal=<YYYYQ#> <event tokens: current_interest_rate=7, ...> [EVT_END]
      ...
    [EOS]

The optional ``cal=`` token (config ``calendar: yearquarter|year``) anchors each event in absolute
calendar time, so the History encoder can tell 2005 from 2008 — the macro-regime signal that pure
loan-internal tokens lack. Real macro series (HPI / prevailing rate / unemployment), once joined
into the panel, are just additional ``event`` fields.

Everything is fit on TRAIN only and serialized (vocab + bin edges + categories), so val / test /
inference reuse identical ids. Config (per asset) drives which fields go to which branch.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .base import BaseTokenizer
from .categorical import CategoricalTokenizer
from .numeric_bucketer import NumericBucketer
from .vocabulary import SPECIAL_TOKENS, Vocabulary


class KVTTokenizer(BaseTokenizer):
    """Config-driven KVT tokenizer.

    config keys::
        id_col, time_col, time_field
        profile: {numeric: [...], categorical: [...]}
        event:   {numeric: [...], categorical: [...]}
        n_bins (16), max_categories (256), max_events (60)
        bins:     {field: n_bins}      # per-field granularity override
        anchors:  {field: [cutpoints]} # forced bin boundaries at thresholds (e.g. LTV 80)
        calendar: yearquarter|year|none  # absolute-time token per event (macro regime)
    """

    def __init__(self, config: dict):
        self.config = config
        self.vocabulary: Vocabulary | None = None
        self._num: dict[str, NumericBucketer] = {}
        self._cat: dict[str, CategoricalTokenizer] = {}
        self._time: NumericBucketer | None = None
        self._cal: CategoricalTokenizer | None = None
        self._parse_config()

    def _parse_config(self) -> None:
        c = self.config
        self.id_col = c["id_col"]
        self.time_col = c["time_col"]
        self.time_field = c["time_field"]
        self.p_num = list(c.get("profile", {}).get("numeric", []))
        self.p_cat = list(c.get("profile", {}).get("categorical", []))
        self.e_num = list(c.get("event", {}).get("numeric", []))
        self.e_cat = list(c.get("event", {}).get("categorical", []))
        self.n_bins = int(c.get("n_bins", 16))
        self.max_categories = int(c.get("max_categories", 256))
        self.max_events = int(c.get("max_events", 60))
        self.bins = dict(c.get("bins", {}))          # per-field n_bins overrides
        self.anchors = dict(c.get("anchors", {}))    # per-field forced cut-points
        self.calendar = c.get("calendar", "none")    # 'yearquarter' | 'year' | 'none'

    @staticmethod
    def _calendar(dates, mode: str) -> pd.Series:
        """ISO 'YYYY-MM-DD' reporting dates → absolute-time labels ('2008' or '2008Q1')."""
        s = pd.Series(dates).astype("string")
        year = s.str[:4]
        if mode == "year":
            return year
        month = pd.to_numeric(s.str[5:7], errors="coerce")
        quarter = ((month - 1) // 3 + 1).astype("Int64").astype("string")
        return year + "Q" + quarter

    # ------------------------------------------------------------------ fit
    def fit(self, panel: pd.DataFrame) -> 'KVTTokenizer':
        """Fit every field tokenizer on the training panel and build the vocabulary."""
        self._num = {f: NumericBucketer(self.bins.get(f, self.n_bins), self.anchors.get(f)).fit(panel[f])
                     for f in self.p_num + self.e_num}
        self._cat = {f: CategoricalTokenizer(self.max_categories).fit(panel[f])
                     for f in self.p_cat + self.e_cat}
        self._time = NumericBucketer(self.n_bins).fit(panel[self.time_field])

        vocab = Vocabulary()
        for f in self.p_num + self.e_num:
            for label in self._num[f].vocab():
                vocab.add(f"{f}={label}")
        for f in self.p_cat + self.e_cat:
            for label in self._cat[f].vocab():
                vocab.add(f"{f}={label}")
        for label in self._time.vocab():
            vocab.add(f"t={label}")
        if self.calendar != "none":                  # absolute-time / macro-regime token
            self._cal = CategoricalTokenizer(max_categories=512).fit(
                self._calendar(panel[self.time_col], self.calendar))
            for label in self._cal.vocab():
                vocab.add(f"cal={label}")
        self.vocabulary = vocab
        return self

    def build_vocab(self, train_panel: pd.DataFrame) -> None:
        """Alias for fit() (scaffold-compatible)."""
        self.fit(train_panel)

    # --------------------------------------------------------------- encode
    def _profile_tokens(self, row: pd.Series) -> list[str]:
        toks = [f"{f}={self._num[f].transform(row.get(f))}" for f in self.p_num]
        toks += [f"{f}={self._cat[f].transform(row.get(f))}" for f in self.p_cat]
        return toks

    def _event_tokens(self, row: pd.Series) -> list[str]:
        toks = ["[EVT_START]", f"t={self._time.transform(row.get(self.time_field))}"]
        if self._cal is not None:
            cal_val = self._calendar([row.get(self.time_col)], self.calendar).iloc[0]
            toks.append(f"cal={self._cal.transform(cal_val)}")
        toks += [f"{f}={self._num[f].transform(row.get(f))}" for f in self.e_num]
        toks += [f"{f}={self._cat[f].transform(row.get(f))}" for f in self.e_cat]
        toks.append("[EVT_END]")
        return toks

    def tokens(self, loan_panel: pd.DataFrame) -> list[str]:
        """Encode one loan's rows into the fused token *strings* (pre-id, for QA/inspection)."""
        if self.vocabulary is None:
            raise RuntimeError("tokenizer not fitted — call fit(train_panel) first")
        df = loan_panel.sort_values(self.time_col).tail(self.max_events)
        seq = ["[BOS]", "[USR]"]
        seq += self._profile_tokens(df.iloc[0])
        for _, row in df.iterrows():
            seq += self._event_tokens(row)
        seq.append("[EOS]")
        return seq

    def encode(self, loan_panel: pd.DataFrame) -> list[int]:
        return [self.vocabulary.encode(t) for t in self.tokens(loan_panel)]

    def decode(self, tokens: list[int]) -> list[str]:
        return [self.vocabulary.decode(i) for i in tokens]

    @property
    def field_types(self) -> dict[str, int]:
        """Stable id per field *key* (profile/event fields + ``t``/``cal``) for type-level masking.

        Keys are the part before ``=`` in a fused token (e.g. ``original_ltv``, ``t``, ``cal``).
        Structural specials have no ``=`` and map to ``-1`` at encode time.
        """
        keys = list(self.p_num) + list(self.p_cat) + list(self.e_num) + list(self.e_cat) + ["t"]
        if self.calendar != "none":
            keys.append("cal")
        return {k: i for i, k in enumerate(keys)}

    def encode_with_meta(self, loan_panel: pd.DataFrame) -> dict[str, list[int]]:
        """Encode one loan to ids **plus** the per-token metadata the model + masking need.

        Returns four equal-length lists, aligned with :meth:`tokens`:

        * ``input_ids``   — vocabulary ids.
        * ``event_index`` — 0-based month for every token inside an ``[EVT_START]…[EVT_END]`` block
          (markers included); ``-1`` for ``[BOS]``/``[USR]``/profile/``[EOS]``.
        * ``field_type``  — :attr:`field_types` id per fused token; ``-1`` for structural specials.
        * ``branch``      — ``0`` profile, ``1`` event, ``-1`` structural — routes tokens to branches.
        """
        if self.vocabulary is None:
            raise RuntimeError("tokenizer not fitted — call fit(train_panel) first")
        ftypes = self.field_types
        input_ids, event_index, field_type, branch = [], [], [], []
        cur_event, in_event = -1, False
        for t in self.tokens(loan_panel):
            if t == "[EVT_START]":
                cur_event, in_event = cur_event + 1, True
                ev, ft, br = cur_event, -1, -1
            elif t == "[EVT_END]":
                ev, ft, br, in_event = cur_event, -1, -1, False
            elif t in SPECIAL_TOKENS:                 # [BOS] / [USR] / [EOS]
                ev, ft, br = -1, -1, -1
            else:                                     # a fused field=value token
                ft = ftypes.get(t.split("=", 1)[0], -1)
                ev, br = (cur_event, 1) if in_event else (-1, 0)
            input_ids.append(self.vocabulary.encode(t))
            event_index.append(ev)
            field_type.append(ft)
            branch.append(br)
        return {"input_ids": input_ids, "event_index": event_index,
                "field_type": field_type, "branch": branch}

    @property
    def vocab_size(self) -> int:
        return self.vocabulary.size if self.vocabulary is not None else 0

    # ------------------------------------------------------------ serialize
    @staticmethod
    def _num_state(nb: NumericBucketer) -> dict:
        return {"n_bins": nb.n_bins, "n_bins_": nb.n_bins_,
                "edges": nb.edges.tolist() if nb.edges is not None else None}

    @staticmethod
    def _num_from(state: dict) -> NumericBucketer:
        nb = NumericBucketer(state["n_bins"])
        nb.n_bins_ = state["n_bins_"]
        nb.edges = np.array(state["edges"]) if state["edges"] is not None else None
        return nb

    @staticmethod
    def _cat_state(ct: CategoricalTokenizer) -> dict:
        return {"max_categories": ct.max_categories, "min_count": ct.min_count,
                "categories_": ct.categories_}

    @staticmethod
    def _cat_from(s: dict) -> CategoricalTokenizer:
        ct = CategoricalTokenizer(s["max_categories"], s["min_count"])
        ct.categories_ = s["categories_"]
        return ct

    def save(self, path) -> None:
        state = {
            "config": self.config,
            "numeric": {f: self._num_state(nb) for f, nb in self._num.items()},
            "categorical": {f: self._cat_state(ct) for f, ct in self._cat.items()},
            "time": self._num_state(self._time),
            "cal": self._cat_state(self._cal) if self._cal is not None else None,
            "vocab": [self.vocabulary.id_to_token[i] for i in range(self.vocabulary.size)],
        }
        Path(path).write_text(json.dumps(state))

    @classmethod
    def load(cls, path) -> 'KVTTokenizer':
        state = json.loads(Path(path).read_text())
        obj = cls(state["config"])
        obj._num = {f: cls._num_from(s) for f, s in state["numeric"].items()}
        obj._cat = {f: cls._cat_from(s) for f, s in state["categorical"].items()}
        obj._time = cls._num_from(state["time"])
        obj._cal = cls._cat_from(state["cal"]) if state.get("cal") else None
        vocab = Vocabulary.__new__(Vocabulary)
        vocab.token_to_id = {t: i for i, t in enumerate(state["vocab"])}
        vocab.id_to_token = {i: t for i, t in enumerate(state["vocab"])}
        obj.vocabulary = vocab
        return obj
