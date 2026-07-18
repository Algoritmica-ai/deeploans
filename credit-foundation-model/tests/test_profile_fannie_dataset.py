# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Tests for the Fannie dataset glossary + profiler (scripts/profile_fannie_dataset.py).

Two layers, matching the repo convention:
  * glossary logic — every published field is documented, positions are consistent with the schema;
  * profiler artifact — the produced JSON's per-column stats and delinquency-by-year re-derive
    exactly from a synthetic panel via an independent pandas ground-truth.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def _load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


G = _load("reference_implementations/fannie_mae/fannie_glossary.py", "fannie_glossary")
CMP = _load("scripts/compare_profiles.py", "compare_profiles")


# ------------------------------------------------------------------ glossary
def _schema_cols() -> list[tuple[int, str]]:
    out = []
    for line in (ROOT / "configs/fannie_mae/raw_schema.yaml").read_text().splitlines():
        m = re.search(r"index:\s*(\d+),\s*name:\s*(\w+)", line)
        if m:
            out.append((int(m.group(1)), m.group(2)))
    return out


def test_glossary_covers_every_published_field():
    schema = _schema_cols()
    assert len(schema) == 113
    names = {n for _, n in schema}
    assert set(G.RAW_FIELDS) == names, (
        f"missing {names - set(G.RAW_FIELDS)}; extra {set(G.RAW_FIELDS) - names}")


def test_glossary_positions_match_schema_order():
    for idx, name in _schema_cols():
        assert G.RAW_FIELDS[name][0] == idx + 1, name   # 1-based field position


def test_derived_fields_documented_and_positionless():
    assert set(G.DERIVED_FIELDS) == {
        "loan_id", "reporting_date", "dlq_num", "default_event", "prepay_event", "is_performing"}
    assert all(v[0] is None for v in G.DERIVED_FIELDS.values())


def test_describe_falls_back_to_name_for_unknown():
    assert G.describe("not_a_column") == "not_a_column"
    assert G.describe("original_ltv").startswith("Original Loan-to-Value")


# ------------------------------------------------------------------ profiler artifact
def _synth_panel(path: Path, n: int = 40_000) -> pd.DataFrame:
    import calendar
    rng = np.random.default_rng(7)
    ry = rng.choice(range(2019, 2023), n)
    rm = rng.integers(1, 13, n)
    dlq = rng.choice([0, 0, 0, 1, 3, 6, 12], n)
    zbc = rng.choice(["", "01", "09"], n, p=[0.7, 0.2, 0.1])

    def iso(y, m):
        return [f"{a}-{b:02d}-{calendar.monthrange(a, b)[1]:02d}" for a, b in zip(y, m)]

    df = pd.DataFrame({
        "loan_id": ["L%05d" % i for i in rng.integers(0, 5000, n)],
        "reporting_date": iso(ry, rm),
        "origination_date": iso(rng.choice(range(2015, 2019), n), np.ones(n, int)),
        "dlq_num": pd.array(dlq, dtype="Int64"),
        "default_event": pd.array((dlq >= 6) | np.isin(zbc, ["09"]), dtype="boolean"),
        "prepay_event": (zbc == "01"),
        "is_performing": pd.array((dlq == 0) & (~np.isin(zbc, ["01", "09"])), dtype="boolean"),
        "original_ltv": rng.integers(40, 97, n),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    return df


def _run_profiler(panel: Path, out: Path) -> dict:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/profile_fannie_dataset.py"),
         "--panel", str(panel), "--out", str(out), "--batch-rows", "7000"],
        check=True, capture_output=True, text=True)
    return json.loads(out.read_text())


def test_profile_matches_pandas_ground_truth(tmp_path):
    panel = tmp_path / "panel.parquet"
    df = _synth_panel(panel)
    prof = _run_profiler(panel, tmp_path / "prof.json")

    assert prof["n_rows"] == len(df)
    assert prof["n_loans"] == df["loan_id"].nunique()

    # numeric column stats (exact aggregates; quantiles are reservoir-approximate so not compared)
    ltv = prof["columns"]["original_ltv"]["numeric"]
    assert ltv["min"] == float(df["original_ltv"].min())
    assert ltv["max"] == float(df["original_ltv"].max())
    assert abs(ltv["mean"] - df["original_ltv"].mean()) < 1e-5    # mean rounded to 6 dp

    # distinct count exactness
    assert prof["columns"]["original_ltv"]["n_unique"] == df["original_ltv"].nunique()

    # delinquency-by-year re-derived independently
    df["year"] = df["reporting_date"].str[:4].astype(int)
    for row in prof["delinquency_by_reporting_year"]:
        g = df[df["year"] == row["year"]]
        assert row["loan_months"] == len(g)
        exp_d180 = 100 * (g["dlq_num"] >= 6).sum() / g["dlq_num"].notna().sum()
        assert abs(row["d180_plus_pct"] - exp_d180) < 1e-3    # pct rounded to 4 dp
        exp_deft = 100 * g["default_event"].fillna(False).sum() / len(g)
        assert abs(row["default_event_pct"] - exp_deft) < 1e-3


def test_profile_vintage_default_is_loan_level(tmp_path):
    panel = tmp_path / "panel.parquet"
    df = _synth_panel(panel)
    prof = _run_profiler(panel, tmp_path / "prof.json")

    df["oy"] = df["origination_date"].str[:4].astype(int)
    ever = df.groupby("loan_id").agg(oy=("oy", "first"),
                                     deft=("default_event", lambda s: bool(s.fillna(False).max())))
    for row in prof["vintage_default_by_origination_year"]:
        g = ever[ever["oy"] == row["origination_year"]]
        assert row["n_loans"] == len(g)                 # loans, not loan-months
        assert row["n_ever_default"] == int(g["deft"].sum())


def test_delinquency_only_matches_full_delinquency_tables(tmp_path):
    panel = tmp_path / "panel.parquet"
    _synth_panel(panel)
    full = _run_profiler(panel, tmp_path / "full.json")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/profile_fannie_dataset.py"),
         "--panel", str(panel), "--out", str(tmp_path / "donly.json"),
         "--batch-rows", "7000", "--delinquency-only"], check=True, capture_output=True, text=True)
    donly = json.loads((tmp_path / "donly.json").read_text())
    assert donly["delinquency_only"] is True
    assert donly["columns"] == {}                                    # per-column profile skipped
    assert donly["delinquency_by_reporting_year"] == full["delinquency_by_reporting_year"]
    assert donly["vintage_default_by_origination_year"] == full["vintage_default_by_origination_year"]
    assert donly["reporting_range"] == full["reporting_range"]       # ranges still tracked


# ------------------------------------------------------------------ profile comparison
def _mk_profile(rows_by_year: dict[int, tuple[int, int]], n_rows: int) -> dict:
    """rows_by_year: year -> (loan_months, n_default). Minimal profile for compare tests."""
    table = []
    for y, (lm, nd) in rows_by_year.items():
        table.append({"year": y, "loan_months": lm, "known_status": lm,
                      "dpd30_plus": nd, "dpd30_plus_pct": round(100 * nd / lm, 4),
                      "d180_plus": nd, "d180_plus_pct": round(100 * nd / lm, 4),
                      "default_event": nd, "default_event_pct": round(100 * nd / lm, 4),
                      "performing_pct": round(100 * (lm - nd) / lm, 4)})
    return {"source": "x", "n_rows": n_rows, "n_loans": 10,
            "delinquency_by_reporting_year": table}


def test_pooled_is_loan_month_weighted():
    # 2 years, unequal size: pooled rate must weight by loan-months, not average the two years
    p = _mk_profile({2020: (1000, 10), 2021: (100, 5)}, 1100)   # 10/1000=1%, 5/100=5%
    pooled = CMP._pooled(p)
    assert pooled["default_event_pct"] == round(100 * 15 / 1100, 4)   # 1.3636, not (1+5)/2
    assert pooled["loan_months"] == 1100


def test_year_table_diff_and_rel():
    a = _mk_profile({2020: (1000, 12)}, 1000)     # 1.2%
    b = _mk_profile({2020: (5000, 50)}, 5000)     # 1.0%  (reference)
    yt = CMP._year_table(a, b, "A", "B")
    row = yt.loc[2020]
    assert row["default_event_pct__diff_pp"] == round(1.2 - 1.0, 4)
    assert row["default_event_pct__diff_rel%"] == CMP._rel(1.2, 1.0)   # +20%


def test_compare_verdict_representative_when_pooled_gap_small(tmp_path):
    a = _mk_profile({2020: (4000, 41), 2021: (4000, 39)}, 8000)     # pooled 1.0%
    b = _mk_profile({2020: (100000, 1000), 2021: (100000, 1000)}, 200000)   # pooled 1.0%
    (tmp_path / "a.json").write_text(json.dumps(a))
    (tmp_path / "b.json").write_text(json.dumps(b))
    out = tmp_path / "cmp.json"
    subprocess.run([sys.executable, str(ROOT / "scripts/compare_profiles.py"),
                    "--a", str(tmp_path / "a.json"), "--b", str(tmp_path / "b.json"),
                    "--out", str(out)], check=True, capture_output=True, text=True)
    summary = json.loads(out.read_text())["summary"]
    assert summary["verdict"] == "REPRESENTATIVE"
    assert abs(summary["default_event_pct"]["pooled_diff_pp"]) < 0.05
