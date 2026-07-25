# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Tests for the split stage (scripts/prepare_data.py) + its artifact validator.

Two layers, matching the repo convention:
  * unit tests for the script's origination logic (`_loan_origination`, both modes + error);
  * an end-to-end run of the real script on a synthetic panel, then `scripts/validate_splits.py`
    re-checks the produced files — PASS on a clean split, FAIL on a corrupted one (negative control).

The pure `temporal_loan_split` function is already covered in tests/test_data.py.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent


def _load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


PD = _load("scripts/prepare_data.py", "prepare_data_script")


class _Cfg(dict):
    """Minimal config stand-in (dotted get_path + attribute access), like the ingest tests."""

    def __getattr__(self, k):
        return self[k]

    def get_path(self, dotted, default=None):
        cur = self
        for p in dotted.split("."):
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur


# ------------------------------------------------------------------ _loan_origination
def test_origination_explicit_one_date_per_loan():
    # loan L0 has two monthly rows (same origination); the function must collapse to one row/loan
    panel = pd.DataFrame({
        "loan_id": ["L0", "L0", "L1"],
        "origination_date": ["2015-03-31", "2015-03-31", "2018-07-31"],
        "reporting_date": ["2016-01-31", "2016-02-29", "2019-01-31"],
    })
    cfg = _Cfg(id_col="loan_id", origination_col="origination_date")
    out = PD._loan_origination(panel, cfg)
    assert list(out.index) == ["L0", "L1"]                        # one row per loan
    assert out["L0"] == pd.Timestamp("2015-03-31")
    assert out["L1"] == pd.Timestamp("2018-07-31")


def test_origination_derive_from_reporting_minus_seasoning():
    # derive mode: origination = reporting_date - seasoning_months (month precision)
    panel = pd.DataFrame({
        "loan_id": ["L0", "L1"],
        "reporting_date": ["2020-06-30", "2020-06-30"],
        "seasoning_months": [12, 0],
    })
    cfg = _Cfg(id_col="loan_id", origination_col=None,
               reporting_col="reporting_date", seasoning_col="seasoning_months")
    out = PD._loan_origination(panel, cfg)
    # derive uses monthly periods -> month-start timestamps
    assert out["L0"] == pd.Timestamp("2019-06-01")                # 12 months before June-2020
    assert out["L1"] == pd.Timestamp("2020-06-01")                # 0 seasoning -> same month


def test_origination_missing_column_errors():
    cfg = _Cfg(id_col="loan_id", origination_col="not_here")
    with pytest.raises(SystemExit, match="not in panel"):
        PD._loan_origination(pd.DataFrame({"loan_id": ["L0"]}), cfg)


# ------------------------------------------------------------------ end-to-end + validator
def _synth_panel(path: Path, n_loans: int = 300, months: int = 6) -> pd.DataFrame:
    """A panel with a real origination_date spread across years and multi-month histories."""
    import numpy as np
    rng = np.random.default_rng(3)
    rows = []
    for i in range(n_loans):
        lid = f"{100003700000 + i}"                                # numeric-string ids, like Mortgage
        oy = 2000 + (i % 20)                                       # spread origination 2000..2019
        for m in range(months):
            rows.append((lid, f"{oy}-01-31", f"{2021}-{m + 1:02d}-28", rng.integers(0, 3)))
    df = pd.DataFrame(rows, columns=["loan_id", "origination_date", "reporting_date", "dlq_num"])
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    return df


def _prepare_cfg(tmp: Path, panel: Path, out: Path, reporting_max=None) -> Path:
    cfg = {"input": str(panel), "id_col": "loan_id", "origination_col": "origination_date",
           "reporting_col": "reporting_date", "seasoning_col": "seasoning_months",
           "out_dir": str(out), "fractions": [0.8, 0.1, 0.1], "seed": 42, "key": None}
    if reporting_max:
        cfg["reporting_max"] = reporting_max
    p = tmp / "prepare_test.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def _run(script: str, *args) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(ROOT / script), *args],
                          capture_output=True, text=True)


def test_prepare_then_validate_passes(tmp_path):
    panel = tmp_path / "panel.parquet"
    df = _synth_panel(panel)
    out = tmp_path / "processed"
    cfg = _prepare_cfg(tmp_path, panel, out)

    r = _run("scripts/prepare_data.py", "-c", str(cfg))
    assert r.returncode == 0, r.stderr
    # all three parquets + csv + meta exist
    for f in ("train.parquet", "val.parquet", "test.parquet", "splits.csv", "splits.meta.json"):
        assert (out / f).exists(), f
    # every loan's whole history stayed together: total rows preserved, loans partitioned
    tot = sum(len(pd.read_parquet(out / f"{s}.parquet")) for s in ("train", "val", "test"))
    assert tot == len(df)

    v = _run("scripts/validate_splits.py", "--dir", str(out))
    assert v.returncode == 0, v.stdout + v.stderr
    assert "ALL CHECKS PASSED" in v.stdout


def test_validator_catches_a_leaked_loan(tmp_path):
    """Negative control: inject a train loan's rows into test -> the disjointness check must FAIL."""
    panel = tmp_path / "panel.parquet"
    _synth_panel(panel)
    out = tmp_path / "processed"
    cfg = _prepare_cfg(tmp_path, panel, out)
    assert _run("scripts/prepare_data.py", "-c", str(cfg)).returncode == 0

    train = pd.read_parquet(out / "train.parquet")
    test = pd.read_parquet(out / "test.parquet")
    leaked_id = train["loan_id"].iloc[0]
    poisoned = pd.concat([test, train[train["loan_id"] == leaked_id]], ignore_index=True)
    poisoned.to_parquet(out / "test.parquet")                     # same loan now in train AND test

    v = _run("scripts/validate_splits.py", "--dir", str(out))
    assert v.returncode != 0
    assert "FAIL" in v.stdout and "disjoint" in v.stdout


def test_validator_respects_reporting_max(tmp_path):
    panel = tmp_path / "panel.parquet"
    _synth_panel(panel)                                            # reporting months in 2021
    out = tmp_path / "processed"
    cfg = _prepare_cfg(tmp_path, panel, out, reporting_max="2021-03-31")

    assert _run("scripts/prepare_data.py", "-c", str(cfg)).returncode == 0
    v = _run("scripts/validate_splits.py", "--dir", str(out))
    assert v.returncode == 0, v.stdout + v.stderr
    assert "reporting_max" in v.stdout
    # the cap actually dropped the later months
    assert pd.read_parquet(out / "train.parquet")["reporting_date"].max() <= "2021-03-31"
