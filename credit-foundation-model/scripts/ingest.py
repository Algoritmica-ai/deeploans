# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Ingest a dataset via its adapter — sharded, resumable, asset-blind (v1.1 G1.4 + G3.1).

Reads the recipe's ``dataset:`` pointer, resolves the asset's
:class:`~credit_fm.data.adapter.DatasetAdapter` (e.g. ``reference_implementations/fannie_mae``),
and writes the contract-conforming panel. Two modes:

* **sharded (default, G3.1)** — one ``part-<tag>.parquet`` per source (for Fannie, per reporting
  quarter), written *as each source completes*, with a ``_meta-<tag>.json`` sidecar written
  strictly AFTER its shard. Rerunning the same command **skips completed sources** (sidecar
  present), so a hard kill — OOM, preemption, dropped SSH — costs only the in-flight sources:
  the 100% corpus ingest becomes disk-bound and restartable. Downstream reads the shard
  *directory* like one parquet (``storage.read_parquet`` handles partitioned dirs; the
  ``_``-prefixed sidecars are ignored by dataset discovery), e.g.
  ``prepare_data --input <out>/panel_2000_2024``. Validate a shard with
  ``validate_ingest --panel <dir>/part-<tag>.parquet``.
* ``sharded: false`` — the v1.0 single-file path (``adapter.load_panel()`` → ``combined_name``).

``combine: true`` additionally concatenates the shards into ``<combined_name>`` (v1.0-compatible
single file). That loads the whole panel in RAM — fine for sampled runs, never the 100% corpus.

Config-driven (recipe: ``configs/fannie_mae/ingest.yaml``)::

    python scripts/ingest.py -c configs/fannie_mae/ingest_2000_2024.yaml
    # killed mid-run? just rerun — completed quarters are skipped:
    python scripts/ingest.py -c configs/fannie_mae/ingest_2000_2024.yaml
    python scripts/ingest.py -c configs/fannie_mae/ingest_2000_2024.yaml \
        --sample_pct 100 --combined_name panel_2000_2024_100pct.parquet
"""

from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from credit_fm.data.adapter import get_adapter
from credit_fm.data.dataset_config import load_dataset_config
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def _maybe_auth(key: str | None) -> None:
    """Point gcsfs at the service-account key if one is available and not already set."""
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    if key and Path(key).exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
        print(f"Using GCS key: {key}")


def _default_tag(source: str) -> str:
    """Shard tag for a source with no adapter ``source_tag``: sanitized basename."""
    base = str(source).rstrip("/").rsplit("/", 1)[-1].removesuffix(".parquet")
    return re.sub(r"[^A-Za-z0-9_=-]+", "-", base)


def _source_tags(adapter, sources: list[str]) -> list[str]:
    """One unique shard tag per source (adapter's ``source_tag`` if it has one)."""
    tagger = getattr(adapter, "source_tag", None)
    tags = [tagger(s) if callable(tagger) else _default_tag(s) for s in sources]
    dupes = sorted({t for t in tags if tags.count(t) > 1})
    if dupes:
        raise SystemExit(
            f"source tags collide: {dupes} — every source must map to a unique shard name "
            "(rename the sources, or give the adapter a source_tag() that disambiguates).")
    return tags


def _sidecar(shard_dir: str, tag: str) -> str:
    return storage.join(shard_dir, f"_meta-{tag}.json")


def ingest_sharded(adapter, shard_dir: str, *, workers: int = 8, log=print) -> dict:
    """Write one ``part-<tag>.parquet`` per adapter source under ``shard_dir``; resume-safe.

    A source is *complete* iff its ``_meta-<tag>.json`` sidecar exists — the sidecar is written
    only after the shard parquet write returned, so a killed write leaves no sidecar and the
    source is redone next run (``write_parquet`` truncates the partial file). Completed sources
    are skipped without reading them. Returns a summary assembled from ALL sidecars.
    """
    sources = adapter.sources()
    tags = _source_tags(adapter, sources)
    id_col, time_col = adapter.config.id_col, adapter.config.time_col

    pending: list[tuple[str, str]] = []
    for src, tag in zip(sources, tags):
        if storage.exists(_sidecar(shard_dir, tag)):
            log(f"  skip part-{tag}.parquet (already complete)")
        else:
            pending.append((src, tag))

    def _one(task: tuple[str, str]) -> dict:
        src, tag = task
        df = adapter.load_source(src)
        name = f"part-{tag}.parquet"
        storage.write_parquet(df, storage.join(shard_dir, name))
        side = {"source": str(src), "shard": name, "rows": int(len(df)),
                "loans": int(df[id_col].nunique())}
        if time_col in df.columns and len(df):
            side["reporting"] = [str(df[time_col].min()), str(df[time_col].max())]
        # completion marker: written strictly AFTER the shard parquet is fully on disk
        storage.write_text(json.dumps(side), _sidecar(shard_dir, tag))
        return side

    if pending:
        log(f"Reading {len(pending)} pending source(s) with {workers} parallel workers "
            f"({len(sources) - len(pending)} already complete) ...")
        with ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
            for side in ex.map(_one, pending):
                log(f"  wrote {side['shard']}  ({side['rows']:,} rows, {side['loans']:,} loans)")

    sides = [json.loads(storage.read_text(_sidecar(shard_dir, tag))) for tag in tags]
    return {"shards": [s["shard"] for s in sides], "rows": sum(s["rows"] for s in sides),
            "written": len(pending), "skipped": len(sources) - len(pending), "per_shard": sides}


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/ingest.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'dataset', 'sources', 'out', 'sample_pct', 'workers', 'sharded', 'combine')}",
          flush=True)

    ds = load_dataset_config(cfg.dataset)
    if ds.adapter == "generic":
        raise SystemExit(f"dataset '{ds.name}' uses adapter: generic — its panel already "
                         "conforms; point prepare_data at it directly (nothing to ingest).")

    _maybe_auth(cfg.get_path("key"))
    out = cfg.out.rstrip("/")                           # local path or gs:///s3:// URL
    storage.ensure_auth(out, cfg.get_path("key"))
    adapter = get_adapter(ds, stage=cfg)                # asset logic lives behind the contract

    sharded = bool(cfg.get_path("sharded", True)) and hasattr(adapter, "load_source")
    if not sharded:                                     # v1.0 single-file path
        panel = adapter.load_panel()
        panel_path = storage.join(out, cfg.combined_name)
        storage.write_parquet(panel, panel_path)        # pluggable: local / gs:// / s3://
        print(f"\nWrote {panel_path}: {len(panel):,} rows, "
              f"{panel[ds.id_col].nunique():,} loans, "
              f"reporting {panel[ds.time_col].min()} -> {panel[ds.time_col].max()}, "
              f"origination {panel[ds.origination_col].min()} -> {panel[ds.origination_col].max()}")
        print(f"sources: {len(adapter.sources())} (adapter: {ds.adapter})")
        print("Next: python scripts/prepare_data.py -c configs/fannie_mae/prepare.yaml")
        return

    shard_dir = storage.join(out, cfg.combined_name.rsplit(".", 1)[0])
    summary = ingest_sharded(adapter, shard_dir,
                             workers=int(cfg.get_path("workers", 8) or 8))
    manifest = {"dataset": ds.name, "adapter": ds.adapter, "shard_dir": shard_dir,
                "n_shards": len(summary["shards"]), "rows": summary["rows"],
                "shards": summary["per_shard"], "config": cfg.to_dict()}     # lineage
    storage.write_text(json.dumps(manifest, indent=2, default=str),
                       storage.join(shard_dir, "_ingest.meta.json"))

    # NB: per-shard loan counts do NOT sum to unique loans — a loan spans many quarters.
    print(f"\nWrote {len(summary['shards'])} shard(s) -> {shard_dir}: {summary['rows']:,} rows "
          f"({summary['written']} written, {summary['skipped']} skipped as complete)")
    if cfg.get_path("combine"):
        panel = storage.read_parquet(shard_dir)         # v1.0 compat: whole panel in RAM
        panel_path = storage.join(out, cfg.combined_name)
        storage.write_parquet(panel, panel_path)
        print(f"combined -> {panel_path}: {len(panel):,} rows, "
              f"{panel[ds.id_col].nunique():,} loans")
    print(f"Next: python scripts/prepare_data.py -c configs/fannie_mae/prepare.yaml "
          f"--input {shard_dir}")


if __name__ == "__main__":
    main()
