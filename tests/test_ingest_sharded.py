# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Sharded, resumable ingest tests (v1.1 G3.1) — the driver in scripts/ingest.py.

The contract under test: one ``part-<tag>.parquet`` per source with a ``_meta-<tag>.json``
sidecar written strictly after it; a rerun skips sources whose sidecar exists (kill-safe resume);
a shard without a sidecar (killed mid-write) is redone; the shard *directory* reads back as one
panel equal to the combined file. No network — a fake in-memory adapter plays the sources.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from credit_fm.utils import storage

ROOT = Path(__file__).resolve().parent.parent


def _load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ING = _load("scripts/ingest.py", "ingest_script")


class _Cfg:
    """Just enough DatasetConfig for the driver: id/time column names."""
    id_col = "loan_id"
    time_col = "reporting_date"


class _FakeAdapter:
    """Four quarterly 'sources' served from memory; counts reads; can die on demand.

    ``fail_on`` simulates a hard kill: reading that source raises, exactly like an OOM or a
    dropped connection mid-ingest — everything already written must survive and be skipped
    on the rerun.
    """

    config = _Cfg()

    def __init__(self, frames: dict[str, pd.DataFrame], fail_on: str | None = None):
        self.frames = frames
        self.fail_on = fail_on
        self.reads: list[str] = []

    def sources(self) -> list[str]:
        return list(self.frames)

    def source_tag(self, source: str) -> str:
        return source                                   # sources ARE the quarter tags here

    def load_source(self, source: str) -> pd.DataFrame:
        if source == self.fail_on:
            raise RuntimeError(f"simulated kill while reading {source}")
        self.reads.append(source)
        return self.frames[source]

    def load_panel(self) -> pd.DataFrame:               # protocol completeness (unused here)
        return pd.concat(self.frames.values(), ignore_index=True)


def _quarters(n_loans: int = 6) -> dict[str, pd.DataFrame]:
    """4 quarters; every loan reports every quarter (so loans overlap across shards, like Fannie)."""
    out = {}
    for qi, q in enumerate(["2016Q1", "2016Q2", "2016Q3", "2016Q4"]):
        out[q] = pd.DataFrame({
            "loan_id": [f"L{i}" for i in range(n_loans)],
            "reporting_date": [f"2016-{3 * qi + 1:02d}-31"] * n_loans,
            "balance": [1000.0 * (i + 1) + qi for i in range(n_loans)],
        })
    return out


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["loan_id", "reporting_date"]).reset_index(drop=True)[
        ["loan_id", "reporting_date", "balance"]]


# ------------------------------------------------------------------ happy path
def test_shard_per_source_with_sidecars_and_dir_readback(tmp_path):
    frames = _quarters()
    ad = _FakeAdapter(frames)
    shard_dir = str(tmp_path / "panel")
    summary = ING.ingest_sharded(ad, shard_dir, workers=2, log=lambda *a: None)

    assert sorted(summary["shards"]) == [f"part-2016Q{i}.parquet" for i in (1, 2, 3, 4)]
    assert summary["rows"] == sum(len(f) for f in frames.values())
    assert summary["written"] == 4 and summary["skipped"] == 0
    for q in frames:
        assert (tmp_path / "panel" / f"part-{q}.parquet").exists()
        side = json.loads((tmp_path / "panel" / f"_meta-{q}.json").read_text())
        assert side["rows"] == len(frames[q]) and side["loans"] == 6
        assert side["reporting"][0].startswith("2016-")

    # the shard DIR reads back as one panel == concat of the sources (sidecars ignored)
    whole = storage.read_parquet(shard_dir)
    assert _norm(whole).equals(_norm(pd.concat(frames.values(), ignore_index=True)))


# ------------------------------------------------------------------ resume
def test_rerun_skips_all_completed_sources(tmp_path):
    frames = _quarters()
    shard_dir = str(tmp_path / "panel")
    ING.ingest_sharded(_FakeAdapter(frames), shard_dir, workers=1, log=lambda *a: None)

    ad2 = _FakeAdapter(frames)
    summary = ING.ingest_sharded(ad2, shard_dir, workers=1, log=lambda *a: None)
    assert ad2.reads == []                              # nothing re-read
    assert summary["skipped"] == 4 and summary["written"] == 0
    assert summary["rows"] == sum(len(f) for f in frames.values())   # totals from sidecars


def test_kill_after_three_quarters_then_rerun_reads_only_the_rest(tmp_path):
    """The design's acceptance case: hard-kill mid-ingest, rerun, only the missing quarter runs."""
    frames = _quarters()
    shard_dir = str(tmp_path / "panel")
    ad = _FakeAdapter(frames, fail_on="2016Q4")         # dies on the 4th source
    with pytest.raises(RuntimeError, match="simulated kill"):
        ING.ingest_sharded(ad, shard_dir, workers=1, log=lambda *a: None)
    assert sorted(ad.reads) == ["2016Q1", "2016Q2", "2016Q3"]

    ad2 = _FakeAdapter(frames)                          # 'restart' — no failure this time
    summary = ING.ingest_sharded(ad2, shard_dir, workers=1, log=lambda *a: None)
    assert ad2.reads == ["2016Q4"]                      # ONLY the missing quarter is re-read
    assert summary["skipped"] == 3 and summary["written"] == 1
    whole = storage.read_parquet(shard_dir)             # and the result is complete
    assert _norm(whole).equals(_norm(pd.concat(frames.values(), ignore_index=True)))


def test_shard_without_sidecar_is_redone(tmp_path):
    """A parquet with no sidecar = killed mid-write; the source must be re-read and rewritten."""
    frames = _quarters()
    shard_dir = str(tmp_path / "panel")
    ING.ingest_sharded(_FakeAdapter(frames), shard_dir, workers=1, log=lambda *a: None)

    (tmp_path / "panel" / "_meta-2016Q2.json").unlink()             # simulate the crash window
    (tmp_path / "panel" / "part-2016Q2.parquet").write_bytes(b"garbage-partial-write")

    ad = _FakeAdapter(frames)
    summary = ING.ingest_sharded(ad, shard_dir, workers=1, log=lambda *a: None)
    assert ad.reads == ["2016Q2"]                       # exactly the crashed source
    assert summary["written"] == 1 and summary["skipped"] == 3
    redone = pd.read_parquet(tmp_path / "panel" / "part-2016Q2.parquet")
    assert _norm(redone).equals(_norm(frames["2016Q2"]))            # clean overwrite


# ------------------------------------------------------------------ combined == sharded
def test_combined_file_equals_shard_dir_content(tmp_path):
    frames = _quarters()
    shard_dir = str(tmp_path / "panel")
    ING.ingest_sharded(_FakeAdapter(frames), shard_dir, workers=2, log=lambda *a: None)

    combined = storage.read_parquet(shard_dir)          # what `combine: true` writes out
    storage.write_parquet(combined, str(tmp_path / "panel.parquet"))
    assert _norm(pd.read_parquet(tmp_path / "panel.parquet")).equals(
        _norm(pd.concat(frames.values(), ignore_index=True)))


# ------------------------------------------------------------------ tags
def test_duplicate_tags_are_rejected():
    class _Dupe(_FakeAdapter):
        def source_tag(self, source):
            return "same"
    with pytest.raises(SystemExit, match="collide"):
        ING._source_tags(_Dupe(_quarters()), list(_quarters()))


def test_default_tag_sanitizes_basename():
    assert ING._default_tag("gs://b/dir/My File (v2).parquet") == "My-File-v2-"
    assert ING._default_tag("/data/part_01.parquet") == "part_01"


# ------------------------------------------------------------------ script-run regression
def test_ingest_script_subprocess_resolves_the_adapter(tmp_path):
    """Run `python scripts/ingest.py` EXACTLY as the container does (subprocess from the repo
    root, real fannie_mae adapter). Regression for the 15 Jul field failure: script-run puts
    scripts/ on sys.path instead of the repo root, so the lazy reference_implementations import
    raised ModuleNotFoundError and get_adapter reported 'no adapter registered' — in-process
    tests never saw it because pytest has the root on sys.path."""
    import subprocess
    import sys
    raw = pd.DataFrame({
        "loan_identifier": [f"10{i}" for i in range(10)],
        "monthly_reporting_period": ["012016"] * 10,
        "origination_date": ["062015"] * 10,
        "current_loan_delinquency_status": ["0"] * 10,
        "zero_balance_code": [""] * 10,
    })
    srcs = []
    for q in ("q1", "q2"):
        p = tmp_path / f"raw_{q}.parquet"
        raw.to_parquet(p, index=False)
        srcs.append(str(p))
    cfg = tmp_path / "ingest_test.yaml"
    cfg.write_text(
        "dataset: configs/fannie_mae/dataset.yaml\n"          # the real contract + adapter name
        f"sources: {{files: [{srcs[0]}, {srcs[1]}], root: null, reporting: null}}\n"
        f"out: {tmp_path / 'out'}\ncombined_name: panel.parquet\n"
        "sample_pct: 100\nworkers: 1\nsharded: true\ncombine: false\nkey: null\n")

    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "ingest.py"), "-c", str(cfg)],
                       capture_output=True, text=True, cwd=ROOT)
    assert r.returncode == 0, r.stderr
    shard_dir = tmp_path / "out" / "panel"
    parts = sorted(p.name for p in shard_dir.glob("part-*.parquet"))
    assert parts == ["part-raw_q1.parquet", "part-raw_q2.parquet"]
    assert (shard_dir / "_ingest.meta.json").exists()
    assert storage.read_parquet(str(shard_dir))["loan_id"].nunique() == 10


def test_fannie_source_tag_extracts_quarter():
    fan = _load("reference_implementations/fannie_mae/adapter.py", "fannie_adapter_g31")
    from credit_fm.data.dataset_config import DatasetConfig
    cfg = DatasetConfig(name="fannie_mae", adapter="fannie_mae", id_col="loan_id",
                        time_col="reporting_date", origination_col="origination_date",
                        origination_derived=False)
    ad = fan.FannieMaeAdapter(cfg, stage={})
    assert ad.source_tag("gs://b/root/reporting_year=2016/reporting_quarter=Q1") == "2016Q1"
    assert ad.source_tag("gs://b/files/extract 2020.parquet") == "extract-2020"
