# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Streaming split + encode tests (v1.1 G3.2).

The design gates, verbatim: (1) the STREAMED split equals the in-RAM split — same loan→split
assignment, same rows per split — on a multi-fragment panel; (2) ``validate_splits`` passes on
the bucketed outputs unchanged (and still fails a poisoned one); (3) the bucket-by-bucket encode
produces the same per-loan token sequences as the whole-panel encode. Plus the structural
invariant that makes streaming encode legal at all: every loan lands in exactly one bucket.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from credit_fm.data import streaming
from credit_fm.utils import storage

ROOT = Path(__file__).resolve().parent.parent


def _load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run(script: str, *args) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(ROOT / script), *args],
                          capture_output=True, text=True)


def _synth_panel(n_loans: int = 240, months: int = 5) -> pd.DataFrame:
    """Loans spread over origination years, several monthly rows each — Fannie-shaped."""
    rng = np.random.default_rng(7)
    rows = []
    for i in range(n_loans):
        lid = f"{100003700000 + i}"                               # numeric-string ids, like Fannie
        oy = 2000 + (i % 20)
        for m in range(months):
            rows.append((lid, f"{oy}-01-31", f"2021-{m + 1:02d}-28",
                         int(rng.integers(40, 97)), ["R", "C"][i % 2],
                         200_000.0 - 1_000 * m))
    return pd.DataFrame(rows, columns=["loan_id", "origination_date", "reporting_date",
                                       "original_ltv", "channel", "current_upb"])


def _write_fragmented(panel: pd.DataFrame, d: Path, n_parts: int = 4) -> Path:
    """Write the panel as a DIRECTORY of parquet parts split by reporting month — the shape a
    G3.1 ingest shard dir has (a loan's rows scattered across parts, NOT whole per part)."""
    d.mkdir(parents=True, exist_ok=True)
    for i, (_, sub) in enumerate(panel.groupby(panel["reporting_date"].str[:7])):
        sub.to_parquet(d / f"part-{i:05d}.parquet", index=False)
    return d


# ------------------------------------------------------------------ streaming primitives
def test_iter_fragments_streams_a_dir_with_projection(tmp_path):
    panel = _synth_panel()
    src = _write_fragmented(panel, tmp_path / "panel")
    got = list(streaming.iter_fragments(str(src), columns=["loan_id"], batch_rows=100))
    assert all(list(f.columns) == ["loan_id"] for f in got)
    assert all(len(f) <= 100 for f in got)
    assert sum(len(f) for f in got) == len(panel)


def test_streamed_origination_equals_in_ram(tmp_path):
    PD = _load("scripts/prepare_data.py", "prepare_data_stream_test")

    class _Cfg(dict):
        def __getattr__(self, k):
            return self[k]

        def get_path(self, dotted, default=None):
            return self.get(dotted, default)

    panel = _synth_panel()
    src = _write_fragmented(panel, tmp_path / "panel")
    in_ram = PD._loan_origination(panel, _Cfg(id_col="loan_id",
                                              origination_col="origination_date"))
    streamed = streaming.stream_loan_origination(
        str(src), id_col="loan_id", origination_col="origination_date",
        reporting_col="reporting_date", batch_rows=97, reduce_every=50, log=lambda *a: None)
    pd.testing.assert_series_equal(in_ram.sort_index(), streamed.sort_index(),
                                   check_names=False)


def test_streamed_origination_derive_mode_and_reporting_max(tmp_path):
    PD = _load("scripts/prepare_data.py", "prepare_data_stream_test2")

    class _Cfg(dict):
        def __getattr__(self, k):
            return self[k]

        def get_path(self, dotted, default=None):
            return self.get(dotted, default)

    panel = _synth_panel().assign(seasoning_months=lambda d:
                                  (pd.to_datetime(d.reporting_date).dt.to_period("M")
                                   - pd.to_datetime(d.origination_date).dt.to_period("M"))
                                  .map(lambda p: p.n))
    cap = "2021-03-31"                                 # drops the last 2 reporting months
    src = _write_fragmented(panel, tmp_path / "panel")
    capped = panel[panel.reporting_date <= cap]
    in_ram = PD._loan_origination(capped, _Cfg(id_col="loan_id", origination_col=None,
                                               reporting_col="reporting_date",
                                               seasoning_col="seasoning_months"))
    streamed = streaming.stream_loan_origination(
        str(src), id_col="loan_id", origination_col=None, reporting_col="reporting_date",
        seasoning_col="seasoning_months", reporting_max=cap, batch_rows=113,
        log=lambda *a: None)
    pd.testing.assert_series_equal(in_ram.sort_index(), streamed.sort_index(),
                                   check_names=False)


# ------------------------------------------------------------------ streamed == in-RAM split
def _prepare_cfg(tmp: Path, inp, out, **extra) -> Path:
    cfg = {"input": str(inp), "id_col": "loan_id", "origination_col": "origination_date",
           "reporting_col": "reporting_date", "seasoning_col": "seasoning_months",
           "out_dir": str(out), "fractions": [0.8, 0.1, 0.1], "seed": 42, "key": None}
    cfg.update(extra)
    p = tmp / f"prepare_{len(str(out))}.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def test_streamed_split_equals_in_ram_split(tmp_path):
    panel = _synth_panel()
    src = _write_fragmented(panel, tmp_path / "panel")

    out_ram, out_str = tmp_path / "ram", tmp_path / "streamed"
    r1 = _run("scripts/prepare_data.py", "-c", str(_prepare_cfg(tmp_path, src, out_ram)))
    assert r1.returncode == 0, r1.stderr
    r2 = _run("scripts/prepare_data.py", "-c",
              str(_prepare_cfg(tmp_path, src, out_str, stream=True, buckets=8, batch_rows=100)))
    assert r2.returncode == 0, r2.stderr

    # identical loan -> split assignment
    csv_ram = (out_ram / "splits.csv").read_text()
    csv_str = (out_str / "splits.csv").read_text()
    assert csv_ram == csv_str

    # identical rows per split (content, not layout)
    for s in ("train", "val", "test"):
        a = pd.read_parquet(out_ram / f"{s}.parquet")
        b = storage.read_parquet(str(out_str / s))                 # bucketed dir reads as one
        key = ["loan_id", "reporting_date"]
        pd.testing.assert_frame_equal(
            a.sort_values(key).reset_index(drop=True)[sorted(a.columns)],
            b.sort_values(key).reset_index(drop=True)[sorted(a.columns)])

    # manifests agree on counts; streamed one records the layout
    m_ram = json.loads((out_ram / "splits.meta.json").read_text())
    m_str = json.loads((out_str / "splits.meta.json").read_text())
    assert m_ram["n_loans"] == m_str["n_loans"]
    assert m_str["out_layout"].startswith("bucketed_dirs")


def test_every_loan_lands_in_exactly_one_bucket(tmp_path):
    panel = _synth_panel()
    src = _write_fragmented(panel, tmp_path / "panel")
    out = tmp_path / "streamed"
    r = _run("scripts/prepare_data.py", "-c",
             str(_prepare_cfg(tmp_path, src, out, stream=True, buckets=8, batch_rows=100)))
    assert r.returncode == 0, r.stderr

    for s in ("train", "val", "test"):
        seen: dict[str, str] = {}
        for bdir in streaming.list_buckets(str(out / s)):
            ids = storage.read_parquet(bdir, columns=["loan_id"])["loan_id"].unique()
            for lid in ids:
                assert seen.setdefault(lid, bdir) == bdir, \
                    f"loan {lid} split across {seen[lid]} and {bdir}"


def test_validate_splits_passes_on_streamed_output_and_catches_poison(tmp_path):
    panel = _synth_panel()
    src = _write_fragmented(panel, tmp_path / "panel")
    out = tmp_path / "streamed"
    assert _run("scripts/prepare_data.py", "-c",
                str(_prepare_cfg(tmp_path, src, out, stream=True, buckets=4,
                                 batch_rows=100))).returncode == 0

    v = _run("scripts/validate_splits.py", "--dir", str(out))
    assert v.returncode == 0, v.stdout + v.stderr
    assert "ALL CHECKS PASSED" in v.stdout

    # negative control: copy a train loan's rows into a test bucket -> disjointness must FAIL
    train = storage.read_parquet(str(out / "train"))
    leaked = train[train["loan_id"] == train["loan_id"].iloc[0]]
    leaked.to_parquet(out / "test" / "bucket-000" / "part-poison.parquet", index=False)
    v2 = _run("scripts/validate_splits.py", "--dir", str(out))
    assert v2.returncode != 0
    assert "FAIL" in v2.stdout and "disjoint" in v2.stdout


# ------------------------------------------------------------------ bucketed encode == whole encode
CONFIG = {"id_col": "loan_id", "time_col": "reporting_date", "time_field": "loan_age",
          "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
          "event": {"numeric": ["current_upb"], "categorical": []},
          "n_bins": 8, "max_categories": 64, "max_events": 60, "calendar": "yearquarter"}


def _encode_cfg(tmp: Path, inp, out, tok_path) -> Path:
    cfg = {"split": "train", "input": str(inp), "output": str(out), "tokenizer": str(tok_path),
           "shard_size": 40, "workers": 1, "engine": "cpu", "key": None}
    p = tmp / f"encode_{len(str(out))}.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def test_bucketed_encode_matches_whole_panel_encode(tmp_path):
    from credit_fm.tokenizer import KVTTokenizer
    panel = _synth_panel(n_loans=90).assign(
        loan_age=lambda d: pd.to_datetime(d.reporting_date).dt.month + 10)
    tok = KVTTokenizer(CONFIG).fit(panel)
    tok_path = tmp_path / "tokenizer.json"
    tok.save(tok_path)

    # legacy input: one parquet; streamed input: bucketed dirs (as prepare --stream writes them)
    single = tmp_path / "train.parquet"
    panel.to_parquet(single, index=False)
    bucketed = tmp_path / "train"
    bucket = pd.util.hash_pandas_object(panel["loan_id"].astype(str), index=False) % 3
    for b, sub in panel.groupby(bucket.to_numpy()):
        (bucketed / f"bucket-{int(b):03d}").mkdir(parents=True)
        sub.to_parquet(bucketed / f"bucket-{int(b):03d}" / "part-00000.parquet", index=False)

    out_a, out_b = tmp_path / "enc_single", tmp_path / "enc_bucketed"
    r1 = _run("scripts/encode_dataset.py", "-c", str(_encode_cfg(tmp_path, single, out_a, tok_path)))
    assert r1.returncode == 0, r1.stderr
    r2 = _run("scripts/encode_dataset.py", "-c", str(_encode_cfg(tmp_path, bucketed, out_b, tok_path)))
    assert r2.returncode == 0, r2.stderr
    assert "bucketed input: 3 loan-hash buckets" in r2.stdout

    ma = json.loads((out_a / "manifest.json").read_text())
    mb = json.loads((out_b / "manifest.json").read_text())
    assert ma["n_loans"] == mb["n_loans"] == 90
    assert ma["n_tokens"] == mb["n_tokens"]
    assert all(s.startswith("shard-0") and s.count("-") == 2 for s in mb["shards"])

    # per-loan encodings are identical (order-free comparison on the ragged token columns)
    def _loans(out_dir, manifest):
        df = pd.concat([pd.read_parquet(out_dir / s) for s in manifest["shards"]],
                       ignore_index=True)
        return {r.loan_id: list(r.input_ids) for r in df.itertuples()}

    la, lb = _loans(out_a, ma), _loans(out_b, mb)
    assert la == lb


def test_list_buckets_empty_for_plain_inputs(tmp_path):
    (tmp_path / "flat").mkdir()
    _synth_panel(10).to_parquet(tmp_path / "flat" / "x.parquet", index=False)
    assert streaming.list_buckets(str(tmp_path / "flat")) == []
    assert streaming.list_buckets(str(tmp_path / "flat" / "x.parquet")) == []
    assert streaming.list_buckets(str(tmp_path / "nope")) == []
