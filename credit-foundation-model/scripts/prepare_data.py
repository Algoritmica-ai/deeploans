# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Split a raw credit panel into loan-stratified temporal train/val/test parquets.

Writes ``<out-dir>/{train,val,test}.parquet`` (the whole 24-cutoff history of a loan stays in
one split), a ``splits.csv`` (``loan_id -> split``), and ``splits.meta.json`` — a
reproducibility/audit trail (seed, source SHA-256, loan counts, origination ranges, commit).

``--out-dir`` (and ``--input``) are **pluggable locations**: a local path, ``gs://…``, or
``s3://…`` — only the URL scheme changes (see ``credit_fm.utils.storage``). So the splits can be
written straight back into the cloud bucket under a new folder, e.g.
``--out-dir gs://sriram-credit-fm-data/processed/mortgage_performance/run_2016_2017``.

Origination key (what the temporal split orders by) comes from one of two modes:
  * ``--origination-col COL``  — use an explicit origination-date column directly.
  * derive (default)           — the Dutch RMBS panel has no origination-date column, so
    derive a month-precise origination from ``reporting_date - seasoning_months``.

**Streaming mode (v1.1 G3.2)** — ``stream: true`` splits a panel of ANY size (the 100% corpus)
without loading it: pass 1 streams only the id/origination columns to compute the loan-hash
split; pass 2 streams every row into ``<out-dir>/<split>/bucket-<k>/part-<i>.parquet``, bucketed
by ``hash(loan_id)`` so each bucket dir holds whole loans (what ``encode_dataset`` consumes
bucket-by-bucket). The split *assignment* is identical to the in-RAM path (same seedless
deterministic ordering); only the output layout changes — per-split **directories** instead of
single parquets, still readable as one dataset via ``storage.read_parquet(<dir>)``.

Config-driven (recipe: ``configs/mortgage_performance/prepare.yaml``)::

    python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml
    # 100% corpus from the G3.1 ingest shard dir, streamed:
    python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
        --input gs://.../raw/mortgage_performance/panel_2000_2024 --stream true --buckets 256
    # Dutch panel (derive mode): null origination_col derives reporting - seasoning
    python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
        --input data/raw/all_cutoffs.parquet --origination_col null --out_dir data/processed
"""

from __future__ import annotations

import json
import subprocess
from datetime import date

import pandas as pd

from credit_fm.data.splits import SPLITS, temporal_loan_split
from credit_fm.data.streaming import stream_loan_origination, stream_split_to_buckets
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize
from credit_fm.utils.reproducibility import set_seed


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "unknown"


def _loan_origination(panel: pd.DataFrame, cfg) -> pd.Series:
    """Return one origination date per loan, indexed by id_col."""
    orig_col = cfg.get_path("origination_col")
    if orig_col:
        if orig_col not in panel.columns:
            raise SystemExit(
                f"Column '{orig_col}' not in panel. Available: {list(panel.columns)}")
        s = panel.groupby(cfg.id_col)[orig_col].min()
        return pd.to_datetime(s)

    # derive month-precise origination = reporting_date - seasoning_months
    for col in (cfg.reporting_col, cfg.seasoning_col):
        if col not in panel.columns:
            raise SystemExit(
                f"Derive mode needs '{col}'. Available: {list(panel.columns)} "
                f"(or set origination_col).")
    rep = pd.to_datetime(panel[cfg.reporting_col]).dt.to_period("M")
    orig_period = rep - panel[cfg.seasoning_col].astype(int)
    per_loan = (
        pd.DataFrame({cfg.id_col: panel[cfg.id_col].to_numpy(), "op": orig_period})
        .groupby(cfg.id_col)["op"].min()           # constant per loan; min is defensive
    )
    return per_loan.dt.to_timestamp()


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/mortgage_performance/prepare.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'input', 'origination_col', 'out_dir', 'fractions', 'seed', 'stream')}",
          flush=True)
    set_seed(cfg.seed)

    fractions = tuple(float(x) for x in cfg.fractions)
    in_path, out = cfg.input, cfg.out_dir.rstrip("/")
    storage.ensure_auth(in_path, cfg.key)
    storage.ensure_auth(out, cfg.key)
    rmax = cfg.get_path("reporting_max")
    stream = bool(cfg.get_path("stream"))
    buckets = int(cfg.get_path("buckets", 64) or 64)
    batch_rows = int(cfg.get_path("batch_rows", 2_000_000) or 2_000_000)

    if stream:                                   # G3.2 pass 1: origination scan, never the panel
        print(f"Streaming {in_path} (pass 1: origination scan, "
              f"batches of {batch_rows:,} rows) ...", flush=True)
        origination = stream_loan_origination(
            in_path, id_col=cfg.id_col, origination_col=cfg.get_path("origination_col"),
            reporting_col=cfg.reporting_col, seasoning_col=cfg.get_path("seasoning_col"),
            reporting_max=rmax, batch_rows=batch_rows)
        panel = None
    else:
        print(f"Loading {in_path} ...")
        panel = storage.read_parquet(in_path)
        if rmax:
            dt = pd.to_datetime(panel[cfg.reporting_col], errors="coerce")
            n0 = len(panel)
            panel = panel[dt <= pd.to_datetime(str(rmax))]
            print(f"reporting_max {rmax}: {n0:,} -> {len(panel):,} rows "
                  "(temporal cap — keeps the pretrain corpus blind to the OOT test era)")
        if cfg.id_col not in panel.columns:
            raise SystemExit(f"Column '{cfg.id_col}' not in panel. Available: {list(panel.columns)}")
        origination = _loan_origination(panel, cfg)

    mode = cfg.get_path("origination_col") or f"derived({cfg.reporting_col}-{cfg.seasoning_col})"
    print(f"Origination key: {mode}  "
          f"({str(origination.min().date())} -> {str(origination.max().date())})")

    assignment = temporal_loan_split(origination, fractions=fractions)
    split_series = pd.Series(assignment, name="split")

    counts: dict[str, int] = {}
    ranges: dict[str, list[str]] = {}
    if stream:                                   # G3.2 pass 2: route rows to loan-hash buckets
        print(f"Streaming pass 2: routing rows to {buckets} loan-hash buckets per split ...",
              flush=True)
        rows = stream_split_to_buckets(
            in_path, assignment, out, id_col=cfg.id_col, reporting_col=cfg.reporting_col,
            reporting_max=rmax, buckets=buckets, batch_rows=batch_rows)
        for s in SPLITS:
            orig_in = origination[split_series[split_series == s].index]
            counts[s] = int(split_series.eq(s).sum())
            ranges[s] = [str(orig_in.min().date()), str(orig_in.max().date())]
            print(f"  {s:>5}: {counts[s]:>7,} loans  {rows[s]:>10,} rows -> {out}/{s}/  "
                  f"origination {ranges[s][0]} -> {ranges[s][1]}")
    else:
        # write per-split parquets — a loan's entire history travels together
        panel = panel.assign(_split=panel[cfg.id_col].map(assignment))
        for s in SPLITS:
            sub = panel[panel["_split"] == s].drop(columns="_split")
            storage.write_parquet(sub, storage.join(out, f"{s}.parquet"))
            orig_in = origination[split_series[split_series == s].index]
            counts[s] = int(split_series.eq(s).sum())
            ranges[s] = [str(orig_in.min().date()), str(orig_in.max().date())]
            print(f"  {s:>5}: {counts[s]:>7,} loans  {len(sub):>10,} rows  "
                  f"origination {ranges[s][0]} -> {ranges[s][1]}")

    # loan_id -> split
    csv = split_series.rename_axis(cfg.id_col).reset_index().to_csv(index=False)
    storage.write_text(csv, storage.join(out, "splits.csv"))

    # audit manifest (a shard-DIR source has no single-file hash — record the layout instead)
    meta = {
        "seed": cfg.seed,
        "split_date": date.today().isoformat(),
        "source_panel": in_path,
        "source_panel_sha256": ("(directory)" if storage.isdir(in_path)
                                else storage.sha256(in_path)),
        "out_layout": f"bucketed_dirs({buckets})" if stream else "single_parquet",
        "n_loans": counts,
        "split_criterion": "loan_stratified_temporal_origination",
        "origination_key": mode,
        "fractions": list(fractions),
        "id_col": cfg.id_col,
        "origination_range": ranges,
        "out_dir": out,
        "code_commit": _git_commit(),
        "config": cfg.to_dict(),                       # lineage
    }
    storage.write_text(json.dumps(meta, indent=2, default=str),
                       storage.join(out, "splits.meta.json"))
    print(f"Wrote splits + splits.csv + splits.meta.json to {out}")


if __name__ == "__main__":
    main()
