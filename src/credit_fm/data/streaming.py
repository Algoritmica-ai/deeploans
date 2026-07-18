# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Streaming data path — split + encode without ever holding the panel in RAM (v1.1 G3.2).

The v1.0 pipeline loads the whole panel (``storage.read_parquet``) before splitting/encoding —
fine at a 4–10% sample, impossible at the 100% corpus (~3.3B loan-months). This module streams
instead:

* :func:`iter_fragments` — walk a parquet file OR directory (e.g. the G3.1 ingest shard dir)
  in bounded row batches via ``pyarrow.dataset``; only one batch is in memory at a time.
* :func:`stream_loan_origination` — **pass 1** of the streaming split: project just the id /
  origination (or seasoning) columns and reduce to one origination per loan. The running
  per-loan state is the only thing kept, so RAM scales with *loans*, not rows.
* :func:`stream_split_to_buckets` — **pass 2**: stream every column, route each row to its
  split, and within a split to a **loan-hash bucket** (``hash(loan_id) % buckets``). A loan's
  entire history lands in exactly one bucket directory, so downstream encode can process one
  bucket at a time and still see whole loans — the property the encoder needs and that plain
  time-partitioned shards would break (a loan spans many quarters).
* :func:`list_buckets` — discover ``bucket-*`` subdirs, how ``encode_dataset`` detects a
  streamed split.

Output layout per split (readable as ONE parquet dataset — ``storage.read_parquet(<dir>)``)::

    <out>/train/bucket-000/part-00000.parquet
    <out>/train/bucket-000/part-00017.parquet     # same bucket, later input batch
    <out>/train/bucket-001/part-00000.parquet
    ...
"""

from __future__ import annotations

import re
from collections.abc import Iterator

import fsspec
import pandas as pd

from credit_fm.utils import storage

SPLITS = ("train", "val", "test")


def _dataset(url: str, columns=None):
    """Open a parquet file or directory as a pyarrow dataset (local or fsspec-backed).

    Shard dirs from the sharded ingest may disagree on all-null column types (a field empty in
    one quarter, populated in another) — unify the schema up front so ``to_batches`` can't die
    mid-stream on ``Unsupported cast from string to null``.
    """
    import pyarrow.dataset as pds
    if "://" not in str(url):
        ds, fs, path = pds.dataset(str(url), format="parquet"), None, str(url)
    else:
        from pyarrow.fs import FSSpecHandler, PyFileSystem
        storage.ensure_auth(url)
        raw_fs, path = fsspec.core.url_to_fs(url)
        fs = PyFileSystem(FSSpecHandler(raw_fs))
        ds = pds.dataset(path, filesystem=fs, format="parquet")
    schema = storage._unify_fragment_schemas(ds)
    if not schema.equals(ds.schema):
        ds = pds.dataset(path, filesystem=fs, format="parquet", schema=schema)
    return ds


def iter_fragments(url: str, *, columns: list[str] | None = None,
                   batch_rows: int = 2_000_000) -> Iterator[pd.DataFrame]:
    """Yield DataFrames of at most ``batch_rows`` rows from a parquet file/dir at ``url``.

    ``columns`` projects (pass 1 reads 2–3 columns of billions of rows this way). Files whose
    names start with ``_`` or ``.`` (ingest sidecars, manifests) are ignored by pyarrow's
    dataset discovery. Batches follow file/row-group order — deterministic for a fixed input.
    """
    ds = _dataset(url)
    for batch in ds.to_batches(columns=columns, batch_size=batch_rows):
        if batch.num_rows:
            yield batch.to_pandas()


# --------------------------------------------------------------------------- pass 1
def stream_loan_origination(url: str, *, id_col: str, origination_col: str | None,
                            reporting_col: str, seasoning_col: str | None = None,
                            reporting_max=None, batch_rows: int = 2_000_000,
                            reduce_every: int = 20_000_000, log=print) -> pd.Series:
    """One origination timestamp per loan, computed without loading the panel.

    Mirrors ``prepare_data._loan_origination`` exactly (explicit column, or derive =
    ``reporting - seasoning``), including the ``reporting_max`` row cap, so the resulting
    :func:`~credit_fm.data.splits.temporal_loan_split` assignment is identical to the in-RAM
    path. Per-batch per-loan minima are concatenated and re-reduced whenever the running state
    exceeds ``reduce_every`` entries, bounding memory by loans (plus slack), never rows.
    """
    explicit = bool(origination_col)
    cap = pd.to_datetime(str(reporting_max)) if reporting_max else None
    cols = [id_col] + ([origination_col] if explicit else [seasoning_col])
    if cap is not None or not explicit:         # reporting only read when the cap/derive needs it
        cols.insert(1, reporting_col)

    parts: list[pd.Series] = []
    pending = 0
    n_rows = 0

    def _reduce() -> None:
        nonlocal parts, pending
        acc = pd.concat(parts).groupby(level=0).min()
        parts, pending = [acc], len(acc)

    for frag in iter_fragments(url, columns=cols, batch_rows=batch_rows):
        n_rows += len(frag)
        if cap is not None:
            frag = frag[pd.to_datetime(frag[reporting_col], errors="coerce") <= cap]
        if not len(frag):
            continue
        if explicit:
            part = pd.to_datetime(frag.groupby(id_col)[origination_col].min())
        else:                                   # derive: reporting period - seasoning months
            rep = pd.to_datetime(frag[reporting_col]).dt.to_period("M")
            op = rep - frag[seasoning_col].astype(int)
            part = (pd.DataFrame({id_col: frag[id_col].to_numpy(), "op": op})
                    .groupby(id_col)["op"].min())
        parts.append(part)
        pending += len(part)
        if pending >= reduce_every:
            _reduce()

    if not parts:
        raise SystemExit(f"{url}: no rows survive reporting_max={reporting_max} — nothing to split")
    _reduce()
    out = parts[0]
    if not explicit:
        out = out.dt.to_timestamp()             # PeriodIndex values -> month-start timestamps
    log(f"  pass 1: {n_rows:,} rows -> {len(out):,} loans (streamed)")
    return out.rename("origination").rename_axis(id_col)


# --------------------------------------------------------------------------- pass 2
def stream_split_to_buckets(url: str, assignment: dict, out_dir: str, *, id_col: str,
                            reporting_col: str | None = None, reporting_max=None,
                            buckets: int = 64, batch_rows: int = 2_000_000,
                            log=print) -> dict[str, int]:
    """Route every row of ``url`` into ``<out_dir>/<split>/bucket-<k>/part-<i>.parquet``.

    ``assignment`` is the ``loan_id -> split`` dict from ``temporal_loan_split``. The bucket is
    ``hash(loan_id) % buckets`` — deterministic, so every row of a loan lands in the same bucket
    dir regardless of which input batch carried it. Returns rows written per split.
    """
    cap = pd.to_datetime(str(reporting_max)) if (reporting_max and reporting_col) else None
    rows = dict.fromkeys(SPLITS, 0)

    for i, frag in enumerate(iter_fragments(url, batch_rows=batch_rows)):
        if cap is not None:
            frag = frag[pd.to_datetime(frag[reporting_col], errors="coerce") <= cap]
        if not len(frag):
            continue
        split = frag[id_col].map(assignment)
        if split.isna().any():                  # every surviving loan must be assigned (pass 1
            bad = frag.loc[split.isna(), id_col].astype(str).unique()[:5]
            raise SystemExit(f"loans with no split assignment (pass-1/pass-2 filter mismatch?): "
                             f"{list(bad)}")    # saw the same reporting_max-filtered rows)
        bucket = pd.util.hash_pandas_object(frag[id_col].astype(str), index=False) % buckets
        for (s, b), sub in frag.groupby([split.to_numpy(), bucket.to_numpy()], sort=True):
            storage.write_parquet(
                sub, storage.join(out_dir, str(s), f"bucket-{int(b):03d}", f"part-{i:05d}.parquet"))
            rows[str(s)] += len(sub)
        done = sum(rows.values())
        log(f"  pass 2: batch {i}  ({done:,} rows routed)")
    return rows


def list_buckets(url: str) -> list[str]:
    """Sorted ``bucket-*`` subdir URLs under ``url`` — empty if ``url`` isn't a bucketed dir.

    How ``encode_dataset`` detects a streamed split: non-empty means "iterate buckets, each one
    holds whole loans"; empty means the legacy single-parquet (or plain dir) read path.
    """
    try:
        fs, path = fsspec.core.url_to_fs(str(url))
        if not fs.isdir(path):
            return []
        entries = fs.ls(path, detail=False)
    except (FileNotFoundError, OSError):
        return []
    proto = str(url).split("://")[0] + "://" if "://" in str(url) else ""
    out = []
    for e in entries:
        name = str(e).rstrip("/").rsplit("/", 1)[-1]
        if re.fullmatch(r"bucket-\d+", name):
            out.append(proto + str(e) if proto and "://" not in str(e) else str(e))
    return sorted(out)
