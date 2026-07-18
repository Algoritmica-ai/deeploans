# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Encode-once: turn a per-loan monthly panel into token-id **shards** for the data loader.

The model trains over each loan many times; re-tokenizing on every epoch would starve the GPUs.
So we encode every loan exactly once here (via :meth:`KVTTokenizer.encode_with_meta`) and persist
the result. Each row of a shard is one loan with four aligned ragged columns —
``input_ids`` / ``event_index`` / ``field_type`` / ``branch`` (the contract the hierarchical model
and the MLM masking both read) — plus ``n_tokens`` / ``n_events`` for batching and bucketing.

``encode_panel`` (one DataFrame → one shard DataFrame) is the testable core. ``encode_to_shards``
writes a whole panel to sharded parquet + returns the shard list, **optionally across worker
processes** (``workers > 1``) — the per-loan Python tokenization is CPU-bound, so this is the
parallelism that makes a full-corpus encode (millions of loans) feasible. ``iter_shards`` is the
in-process generator used by tests.
"""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd


def _n_events(event_index: list[int]) -> int:
    """Number of event blocks = max month index + 1 (0 if the loan has no events)."""
    months = [e for e in event_index if e >= 0]
    return (max(months) + 1) if months else 0


def encode_panel(tokenizer, panel: pd.DataFrame) -> pd.DataFrame:
    """Encode every loan in ``panel`` to one shard DataFrame (one row per loan).

    Columns: ``<id_col>``, ``input_ids``, ``event_index``, ``field_type``, ``branch`` (ragged int
    lists), ``n_tokens``, ``n_events``.
    """
    idc = tokenizer.id_col
    records = []
    for loan_id, loan in panel.groupby(idc, sort=False):
        meta = tokenizer.encode_with_meta(loan)
        records.append({
            idc: loan_id,
            "input_ids": meta["input_ids"],
            "event_index": meta["event_index"],
            "field_type": meta["field_type"],
            "branch": meta["branch"],
            "n_tokens": len(meta["input_ids"]),
            "n_events": _n_events(meta["event_index"]),
        })
    return pd.DataFrame.from_records(records)


def _iter_subpanels(panel: pd.DataFrame, id_col: str, shard_size: int):
    """Yield ``(shard_id, sub_panel)`` — loans assigned to shards in first-seen order, kept whole."""
    order = {lid: i for i, lid in enumerate(panel[id_col].drop_duplicates())}
    shard_of = panel[id_col].map(order) // shard_size
    for sid, sub in panel.groupby(shard_of, sort=True):
        yield int(sid), sub


def iter_shards(tokenizer, panel: pd.DataFrame, shard_size: int) -> Iterator[pd.DataFrame]:
    """Yield encoded shard DataFrames of at most ``shard_size`` loans each (in-process).

    Loans are kept whole (grouped by id) and assigned to shards in first-seen order, so a loan's
    rows never split across shards.
    """
    idc = tokenizer.id_col
    for _, sub in _iter_subpanels(panel, idc, shard_size):
        yield encode_panel(tokenizer, sub)


# ----------------------------------------------------------------- parallel encode
_WORKER_TOK = None  # per-process tokenizer, set by the pool initializer


def _worker_init(tokenizer_path: str, key) -> None:
    """Pool initializer: load the tokenizer once per worker process (cheaper than pickling it)."""
    global _WORKER_TOK
    from credit_fm.tokenizer import KVTTokenizer
    from credit_fm.utils import storage
    storage.ensure_auth(tokenizer_path, key)
    _WORKER_TOK = KVTTokenizer.load(tokenizer_path)


def _encode_shard(task):
    """Worker task: encode one sub-panel and write its shard parquet; return (name, loans, tokens)."""
    from credit_fm.utils import storage
    sid, sub, out_dir, key, prefix = task
    name = f"{prefix}{sid:05d}.parquet"
    shard = encode_panel(_WORKER_TOK, sub)
    storage.ensure_auth(out_dir, key)
    storage.write_parquet(shard, storage.join(out_dir, name))
    return name, len(shard), int(shard["n_tokens"].sum())


def encode_panel_parallel(tokenizer, tokenizer_path: str, panel: pd.DataFrame, *,
                          workers: int = 0, key=None, shard_size: int = 50_000,
                          engine: str = "cpu") -> pd.DataFrame:
    """One-row-per-loan encode of ``panel`` using the shard worker pool; returns one DataFrame.

    The in-process ``encode_panel`` is fine for thousands of loans but takes hours for millions
    (it single-threads the tokenizer). This fans the work out across ``workers`` spawn processes
    via a local temp dir and concatenates the shards. ``workers <= 1`` (or a panel smaller than
    one shard) falls back to ``encode_panel``. Row order is not preserved.
    """
    if engine in ("vector", "gpu"):
        from credit_fm.tokenizer.fast_encode import encode_panel_fast
        return encode_panel_fast(tokenizer, panel, gpu=(engine == "gpu"))
    if not workers or workers <= 1 or panel[tokenizer.id_col].nunique() <= shard_size:
        return encode_panel(tokenizer, panel)
    import shutil
    import tempfile

    from credit_fm.utils import storage
    tmp = tempfile.mkdtemp(prefix="encode_obs_")
    try:
        names, _, _ = encode_to_shards(tokenizer, tokenizer_path, panel, tmp,
                                       shard_size=shard_size, workers=workers, key=key)
        return pd.concat([storage.read_parquet(storage.join(tmp, n)) for n in names],
                         ignore_index=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def encode_to_shards(tokenizer, tokenizer_path: str, panel: pd.DataFrame, out_dir: str, *,
                     shard_size: int = 50_000, workers: int = 0, key=None,
                     name_prefix: str = "shard-", log=print):
    """Encode ``panel`` to sharded parquet under ``out_dir``; return ``(shard_names, n_loans, n_tokens)``.

    ``workers <= 1`` encodes in-process. ``workers > 1`` fans the shards out across that many worker
    processes (each loads ``tokenizer_path`` once) — the speed-up that makes a full-corpus encode
    feasible. Shard names are deterministic (``<name_prefix><id>.parquet``, default
    ``shard-<id>.parquet``) regardless of completion order; the streaming encode (G3.2) passes a
    per-bucket prefix so buckets don't collide in one output dir.
    """
    from credit_fm.utils import storage
    idc = tokenizer.id_col
    names, n_loans, n_tokens = [], 0, 0

    def _tasks():
        for sid, sub in _iter_subpanels(panel, idc, shard_size):
            yield sid, sub, out_dir, key, name_prefix

    if workers and workers > 1:
        import multiprocessing as mp
        # Use 'spawn', NOT 'fork': the parent already opened gRPC/gcsfs (reading the panel from
        # GCS), and forking after gRPC init deadlocks the workers when they write shards back to
        # GCS. spawn gives each worker a clean process that builds its own gcsfs connection.
        ctx = mp.get_context("spawn")
        with ctx.Pool(workers, initializer=_worker_init, initargs=(tokenizer_path, key)) as pool:
            for name, nl, nt in pool.imap_unordered(_encode_shard, _tasks()):
                names.append(name)
                n_loans += nl
                n_tokens += nt
                log(f"  wrote {name}  ({nl:,} loans, {nt:,} tokens)")
    else:
        for sid, sub, od, k, prefix in _tasks():
            name = f"{prefix}{sid:05d}.parquet"
            shard = encode_panel(tokenizer, sub)
            storage.write_parquet(shard, storage.join(od, name))
            names.append(name)
            n_loans += len(shard)
            n_tokens += int(shard["n_tokens"].sum())
            log(f"  wrote {name}  ({len(shard):,} loans, {int(shard['n_tokens'].sum()):,} tokens)")

    return sorted(names), n_loans, n_tokens
