# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Pluggable storage I/O over fsspec.

Every location is a URL, so the backend is swappable by changing the scheme — local paths,
``gs://`` (GCS), or ``s3://`` (AWS, future) — with no code change. Credentials come from the
environment: GCS via a service-account JSON (``GOOGLE_APPLICATION_CREDENTIALS``, auto-pointed at
the container key if present); S3 via the standard AWS chain (env / profile / instance role).

Requires ``fsspec``; ``gcsfs`` for ``gs://`` and ``s3fs`` for ``s3://`` (install per backend).
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

import fsspec
import pandas as pd

# Default gs:// service-account key: the CREDIT_FM_GCS_KEY env var wins (set it on any machine —
# setup_container.sh exports it on the reference box); the historical container path is the
# fallback so existing boxes keep working with zero configuration (v1.1 G5.1 de-hardcode).
GCS_DEFAULT_KEY = os.environ.get("CREDIT_FM_GCS_KEY", "/workspace/.gcloud/credit-fm-sa.json")

T = TypeVar("T")

# Substrings that mark a *transient* cloud/network failure (safe to retry) rather than a real error
# (missing object, bad schema, auth denied). Cloud object stores + OAuth token refresh occasionally
# drop a TLS handshake or return a 5xx/429 under load — fatal to a multi-hour ingest/encode if not
# retried, harmless with a short backoff. Matched case-insensitively against `type(exc): message`.
_TRANSIENT_MARKERS = (
    "ssl", "eof", "oauth2", "max retries", "connection reset", "connection aborted",
    "connectionerror", "connectionreseterror", "transporterror", "timed out", "timeout",
    "temporary failure", "broken pipe", "remotedisconnected", " 500", " 502", " 503", " 504",
    " 429", "internalerror", "serviceunavailable", "badstatusline",
)


def _is_transient(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(m in text for m in _TRANSIENT_MARKERS)


def retry(fn: Callable[[], T], *, tries: int = 5, base_delay: float = 2.0, what: str = "") -> T:
    """Call ``fn`` with exponential backoff, retrying only *transient* network/cloud failures.

    Non-transient errors (missing file, schema mismatch, permission denied) re-raise immediately —
    we never mask a real bug behind retries. Delays: ``base_delay * 2**(attempt-1)`` (2,4,8,16s).
    """
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — classified by _is_transient, real errors re-raise
            if attempt >= tries or not _is_transient(exc):
                raise
            delay = base_delay * (2 ** (attempt - 1))
            where = f" on {what}" if what else ""
            print(f"  [storage] transient error{where} (attempt {attempt}/{tries}): "
                  f"{type(exc).__name__}; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover


def ensure_auth(url: str, key: str | None = GCS_DEFAULT_KEY) -> None:
    """If ``url`` is ``gs://`` and a key file exists, point gcsfs at it (idempotent, no-op else)."""
    if str(url).startswith("gs://") and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        if key and Path(key).exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key


def join(base: str, *parts: str) -> str:
    """Join a base location (local path or URL) with sub-parts, preserving any scheme.

    Unlike ``pathlib.Path``, this does not collapse ``gs://`` to ``gs:/``.
    """
    out = str(base).rstrip("/")
    for p in parts:
        out += "/" + str(p).strip("/")
    return out


def _fs(url: str, storage_options: dict[str, Any] | None):
    ensure_auth(url)
    return fsspec.core.url_to_fs(url, **(storage_options or {}))


def makedirs(url: str, storage_options: dict[str, Any] | None = None) -> None:
    """Create a directory/prefix (no-op on object stores that lack real directories)."""
    fs, path = _fs(url, storage_options)
    fs.makedirs(path, exist_ok=True)


def write_parquet(df: pd.DataFrame, url: str, storage_options: dict[str, Any] | None = None) -> None:
    """Write a DataFrame to ``url`` (local/gs:///s3://).

    Streams through the fsspec file handle rather than ``df.to_parquet(url)`` so it works even when
    this pyarrow build was compiled without native cloud-filesystem (GCS/S3) support.
    """
    def _write() -> None:
        fs, path = _fs(url, storage_options)
        parent = path.rsplit("/", 1)[0]
        if parent and parent != path:
            fs.makedirs(parent, exist_ok=True)
        with fs.open(path, "wb") as f:                       # 'wb' truncates → a retry restarts clean
            df.to_parquet(f, index=False)

    retry(_write, what=url)


def _unify_fragment_schemas(dataset):
    """Union of all fragments' schemas, promoting ``null``-typed columns to their real type.

    A per-source shard where a column is ENTIRELY missing (e.g. REO/modification fields in
    year-2000 Fannie quarters) stores that column as arrow ``null``; later shards store strings.
    A directory scan then fails with ``Unsupported cast from string to null`` because pyarrow
    adopts the first fragment's schema. This builds the union: first non-null type wins per
    column, so all-null columns are read as their real dtype (values stay NA).
    """
    import pyarrow as pa
    fields: dict[str, Any] = {}
    order: list[str] = []
    for frag in dataset.get_fragments():
        for f in frag.physical_schema:
            if f.name not in fields:
                fields[f.name] = f
                order.append(f.name)
            elif pa.types.is_null(fields[f.name].type) and not pa.types.is_null(f.type):
                fields[f.name] = f
    return pa.schema([fields[n] for n in order])


def _dataset_table(path: str, filesystem, columns) -> pd.DataFrame:
    """Scan a parquet file/dir as a dataset; on a shard-schema clash, retry with a unified schema."""
    import pyarrow as pa
    import pyarrow.dataset as pds
    dataset = pds.dataset(path, filesystem=filesystem, format="parquet")
    try:
        return dataset.to_table(columns=columns).to_pandas()
    except (pa.ArrowNotImplementedError, pa.ArrowTypeError, pa.ArrowInvalid):
        schema = _unify_fragment_schemas(dataset)
        dataset = pds.dataset(path, filesystem=filesystem, format="parquet", schema=schema)
        return dataset.to_table(columns=columns).to_pandas()


def read_parquet(url: str, columns=None, storage_options: dict[str, Any] | None = None) -> pd.DataFrame:
    """Read parquet — a single file or a partitioned directory — from local/gs:///s3://.

    Uses the fsspec filesystem (gcsfs/s3fs) for IO, so it works when pyarrow lacks native cloud
    support; pyarrow only parses the bytes. Directories of per-source shards may disagree on
    all-null column types (G3.1 sharded ingest) — handled by :func:`_unify_fragment_schemas`.
    """
    if "://" not in str(url):
        if os.path.isdir(url):                              # local shard dir: same unified scan
            return _dataset_table(str(url), None, columns)
        return pd.read_parquet(url, columns=columns)        # local single file: fastest path
    from pyarrow.fs import FSSpecHandler, PyFileSystem

    def _read() -> pd.DataFrame:                             # rebuild fs+dataset each try (fresh creds)
        fs, path = _fs(url, storage_options)
        return _dataset_table(path, PyFileSystem(FSSpecHandler(fs)), columns)

    return retry(_read, what=url)


def exists(url: str, storage_options: dict[str, Any] | None = None) -> bool:
    """True if ``url`` exists (file or directory/prefix) on its backend."""
    fs, path = _fs(url, storage_options)
    return bool(fs.exists(path))


def isdir(url: str, storage_options: dict[str, Any] | None = None) -> bool:
    """True if ``url`` is a directory/prefix (e.g. a shard dir) rather than a single file."""
    fs, path = _fs(url, storage_options)
    return bool(fs.isdir(path))


def read_text(url: str, storage_options: dict[str, Any] | None = None) -> str:
    """Read a text file from ``url`` (local/gs:///s3://)."""
    ensure_auth(url)
    with fsspec.open(url, "r", **(storage_options or {})) as f:
        return f.read()


def write_text(text: str, url: str, storage_options: dict[str, Any] | None = None) -> None:
    """Write a text file to ``url`` (local/gs:///s3://), creating the parent prefix first."""
    ensure_auth(url)
    parent = url.rsplit("/", 1)[0]
    makedirs(parent, storage_options)
    with fsspec.open(url, "w", **(storage_options or {})) as f:
        f.write(text)


def sha256(url: str, storage_options: dict[str, Any] | None = None) -> str:
    """Stream a file from ``url`` and return its SHA-256 hex digest (backend-agnostic)."""
    ensure_auth(url)
    h = hashlib.sha256()
    with fsspec.open(url, "rb", **(storage_options or {})) as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
