# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Vectorized / GPU panel encoder — same output as ``encode_panel``, orders of magnitude faster.

The per-loan Python encoder tokenizes row by row; at millions of loans that is hours of pure
Python. This module exploits the KVT layout's key property: **every event block has a fixed
token width** (``[EVT_START] t= [cal=] <event fields...> [EVT_END]``) and the profile block has a
fixed width too — so the whole panel encodes as a handful of vectorized column transforms plus
one flatten, instead of a Python loop per token.

Engines (NVIDIA-blueprint parity — cuDF/CuPy on GPU, with a CPU fallback):

* ``vector`` — vectorized pandas/NumPy, single process; no new dependencies.
* ``gpu``    — same algorithm on cuDF/CuPy (RAPIDS); column transforms run on the GPU, the final
  ragged per-loan assembly happens on the host (like the blueprint: cuDF transforms, host packing).

Output is **identical** to ``credit_fm.data.encode.encode_panel`` (tested token-for-token); only
row order differs (sorted by loan id instead of first-seen).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _lib(gpu: bool):
    if not gpu:
        return pd, np
    try:
        import cudf
        import cupy
    except ImportError as e:                          # pragma: no cover - GPU-only path
        raise SystemExit(
            "engine: gpu needs RAPIDS (cudf + cupy) in this environment — "
            "install rapids or use engine: vector") from e
    return cudf, cupy


def _to_xp(series, gpu: bool):
    """Backend Series -> xp float/int array on the right device."""
    return series.to_cupy() if gpu else series.to_numpy()


def _num_token_ids(tok, field, series, nb, xdf, xp, gpu):
    """Numeric column -> vocabulary ids of ``field=<bucket>`` (mirrors NumericBucketer._bucket)."""
    vocab = tok.vocabulary.token_to_id
    # lut slots: [0]="0", [1..n]=bins, [n+1]="NA"
    lut = xp.asarray([vocab[f"{field}=0"]]
                     + [vocab[f"{field}={i}"] for i in range(1, nb.n_bins_ + 1)]
                     + [vocab[f"{field}={nb.NA}"]])
    if series is None:                                # column absent from panel -> all NA
        return xp.full(0, 0), lut[-1]                 # sentinel handled by caller
    if not str(series.dtype).startswith(("int", "float", "Int", "Float")):
        series = (xdf.to_numeric(series, errors="coerce") if hasattr(xdf, "to_numeric")
                  else pd.to_numeric(series, errors="coerce"))
    x = _to_xp(series.astype("float64").fillna(float("nan")), gpu)
    na = xp.isnan(x)
    zero = x == 0.0
    if nb.edges is None:
        code = xp.ones(len(x), dtype=xp.int64)
    else:
        edges = xp.asarray(nb.edges[1:-1])
        code = xp.clip(xp.searchsorted(edges, x, side="right") + 1, 1, nb.n_bins_)
    code = xp.where(zero, 0, code)
    code = xp.where(na, nb.n_bins_ + 1, code)
    return lut[code.astype(xp.int64)], None


def _cat_token_ids(tok, field, series, ct, xdf, xp, gpu):
    """Categorical column -> vocabulary ids of ``field=<cat>`` (UNK unseen, NA missing)."""
    vocab = tok.vocabulary.token_to_id
    na_id, unk_id = vocab[f"{field}={ct.NA}"], vocab[f"{field}={ct.UNK}"]
    na_mask = _to_xp(series.isna(), gpu)
    mapping = xdf.DataFrame({
        "_k": ct.categories_,
        "_v": np.asarray([vocab[f"{field}={c}"] for c in ct.categories_], dtype="int64"),
    })
    tmp = xdf.DataFrame({"_k": series.astype("str")})
    tmp["_i"] = xp.arange(len(tmp))
    out = tmp.merge(mapping, on="_k", how="left").sort_values("_i")
    ids = _to_xp(out["_v"].fillna(unk_id).astype("int64"), gpu)
    return xp.where(na_mask, na_id, ids)


def encode_panel_fast(tokenizer, panel: pd.DataFrame, gpu: bool = False) -> pd.DataFrame:
    """Encode a whole panel in vectorized form; one output row per loan (same schema as
    ``encode_panel``: id, input_ids/event_index/field_type/branch, n_tokens, n_events)."""
    tok = tokenizer
    if tok.vocabulary is None:
        raise RuntimeError("tokenizer not fitted")
    xdf, xp = _lib(gpu)
    vocab = tok.vocabulary.token_to_id
    idc, tc, tf = tok.id_col, tok.time_col, tok.time_field
    fields = tok.p_num + tok.p_cat + tok.e_num + tok.e_cat
    cols = [idc, tc] + [c for c in dict.fromkeys([tf] + fields) if c in panel.columns]
    df = panel[[c for c in cols if c in panel.columns]].copy()
    if gpu:
        df = xdf.from_pandas(df)

    # ---- sort + per-loan tail(max_events), matching tokens(): sort_values(time).tail(max) ----
    df = df.sort_values([idc, tc])
    df["_pos"] = df.groupby(idc).cumcount()
    sizes = df.groupby(idc)[tc].count().rename("_n").reset_index()
    df = df.merge(sizes, on=idc, how="left").sort_values([idc, tc])
    df = df[df["_pos"] >= df["_n"] - tok.max_events]
    df = df.sort_values([idc, tc]).reset_index(drop=True)

    # ---- per-column token ids (the heavy part — fully vectorized / on-GPU) ----
    n_rows = len(df)

    def num_ids(field, nb, source_field=None):
        src = source_field or field
        if src not in df.columns:
            lut_na = vocab[f"{field}={nb.NA}"]
            return xp.full(n_rows, lut_na, dtype=xp.int64)
        ids, _ = _num_token_ids(tok, field, df[src], nb, xdf, xp, gpu)
        return ids

    def cat_ids(field, ct, series):
        if series is None:
            return xp.full(n_rows, vocab[f"{field}={ct.NA}"], dtype=xp.int64)
        return _cat_token_ids(tok, field, series, ct, xdf, xp, gpu)

    p_cols = ([("n", f, num_ids(f, tok._num[f])) for f in tok.p_num]
              + [("c", f, cat_ids(f, tok._cat[f], df[f] if f in df.columns else None))
                 for f in tok.p_cat])
    t_ids = num_ids("t", tok._time, source_field=tf)
    cal_ids = None
    if tok._cal is not None:
        s = df[tc].astype("str")
        year = s.str.slice(0, 4)
        if tok.calendar == "year":
            cal_lab = year
        else:
            month = (xdf.to_numeric(s.str.slice(5, 7), errors="coerce")
                     if hasattr(xdf, "to_numeric") else pd.to_numeric(s.str.slice(5, 7),
                                                                      errors="coerce"))
            quarter = ((month - 1) // 3 + 1).astype("Int64").astype("str")
            cal_lab = year + "Q" + quarter
        cal_ids = cat_ids("cal", tok._cal, cal_lab)
    e_cols = ([("n", f, num_ids(f, tok._num[f])) for f in tok.e_num]
              + [("c", f, cat_ids(f, tok._cat[f], df[f] if f in df.columns else None))
                 for f in tok.e_cat])

    # ---- bring to host + assemble ragged per-loan sequences ----
    def host(a):
        return a.get() if gpu else a

    loan_arr = (df[idc].to_pandas() if gpu else df[idc]).to_numpy()
    starts = np.concatenate([[0], np.flatnonzero(loan_arr[1:] != loan_arr[:-1]) + 1])
    n_ev = np.diff(np.concatenate([starts, [len(loan_arr)]]))
    loans = loan_arr[starts]

    ft_map = tok.field_types
    sp = {t: vocab[t] for t in ("[BOS]", "[USR]", "[EVT_START]", "[EVT_END]", "[EOS]")}

    # event block: fixed width W, one row per panel row
    ev_stack = [np.full(n_rows, sp["[EVT_START]"], dtype=np.int64), host(t_ids)]
    ev_ft = [-1, ft_map["t"]]
    if cal_ids is not None:
        ev_stack.append(host(cal_ids))
        ev_ft.append(ft_map["cal"])
    for _, f, ids in e_cols:
        ev_stack.append(host(ids))
        ev_ft.append(ft_map[f])
    ev_stack.append(np.full(n_rows, sp["[EVT_END]"], dtype=np.int64))
    ev_ft.append(-1)
    W = len(ev_stack)
    ev_flat = np.stack(ev_stack, axis=1).reshape(-1)                  # (n_rows*W,) row-major
    ev_br = np.array([-1] + [1] * (W - 2) + [-1], dtype=np.int64)
    ev_ft = np.array(ev_ft, dtype=np.int64)

    # profile block: from each loan's first kept row
    P = len(p_cols)
    if P:
        prof_mat = np.stack([host(ids)[starts] for _, _, ids in p_cols], axis=1)
        prof_ft = np.array([ft_map[f] for _, f, _ in p_cols], dtype=np.int64)
    else:
        prof_mat = np.zeros((len(loans), 0), dtype=np.int64)
        prof_ft = np.zeros(0, dtype=np.int64)

    head_ids = np.array([sp["[BOS]"], sp["[USR]"]], dtype=np.int64)
    neg = np.full(2, -1, dtype=np.int64)
    eos = np.array([sp["[EOS]"]], dtype=np.int64)
    m1 = np.array([-1], dtype=np.int64)
    zeros_p = np.zeros(P, dtype=np.int64)
    negs_p = np.full(P, -1, dtype=np.int64)

    records = []
    for i, lid in enumerate(loans):
        k = int(n_ev[i])
        ev_slice = ev_flat[starts[i] * W:(starts[i] + k) * W]
        input_ids = np.concatenate([head_ids, prof_mat[i], ev_slice, eos])
        event_index = np.concatenate([neg, negs_p, np.repeat(np.arange(k), W), m1])
        field_type = np.concatenate([neg, prof_ft, np.tile(ev_ft, k), m1])
        branch = np.concatenate([neg, zeros_p, np.tile(ev_br, k), m1])
        records.append({idc: lid, "input_ids": input_ids.tolist(),
                        "event_index": event_index.tolist(),
                        "field_type": field_type.tolist(), "branch": branch.tolist(),
                        "n_tokens": len(input_ids), "n_events": k})
    return pd.DataFrame.from_records(records)
