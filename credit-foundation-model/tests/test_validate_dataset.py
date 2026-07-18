# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Contract-auditor tests (v1.1 G1.5) — green on a conforming panel, and the negative controls:
the validator MUST FAIL on int ids, a leakage column smuggled into the schema, duplicated
entity-periods, and gated-in-AND-terminal rows."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml

REPO = Path(__file__).resolve().parent.parent

DATASET = {
    "dataset": {"name": "toy", "adapter": "generic", "id_col": "loan_id",
                "time_col": "reporting_date", "origination_col": "origination_date"},
    "labels": {"default_12m": {"type": "forward_event", "event_col": "default_event",
                               "horizon_months": 12, "gate_col": "is_performing"}},
    "leakage": ["default_event", "is_performing", "current_dlq"],
    "exclude": ["deal_name"],
}


def _panel() -> pd.DataFrame:
    return pd.DataFrame({
        "loan_id": ["L1", "L1", "L2", "L2"],
        "reporting_date": ["2020-01-31", "2020-02-29", "2020-01-31", "2020-02-29"],
        "origination_date": ["2019-12-31"] * 4,
        "default_event": [False, False, False, True],
        "is_performing": [True, True, True, False],
        "original_ltv": [80.0, 80.0, 95.0, 95.0],
    })


def _schema() -> dict:
    return {"profile": {"numeric": ["original_ltv"]}, "event": {"numeric": []}}


def _run(tmp_path: Path, panel: pd.DataFrame, dataset=None, schema=None):
    (tmp_path / "dataset.yaml").write_text(yaml.safe_dump(dataset or DATASET))
    panel.to_parquet(tmp_path / "panel.parquet", index=False)
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(yaml.safe_dump(schema or _schema()))
    return subprocess.run(
        [sys.executable, "scripts/validate_dataset.py",
         "--dataset", str(tmp_path / "dataset.yaml"),
         "--panel", str(tmp_path / "panel.parquet"), "--schema", str(schema_path)],
        cwd=REPO, capture_output=True, text=True,
        env={"PYTHONPATH": str(REPO / "src"), "PATH": "/usr/bin:/bin"})


def test_conforming_panel_passes(tmp_path):
    proc = _run(tmp_path, _panel())
    assert proc.returncode == 0, proc.stdout + proc.stderr[-1500:]
    assert "ALL CHECKS PASSED" in proc.stdout
    for tag in ("A:", "B:", "C:", "D:", "E:", "F:", "G:"):
        assert f"PASS  {tag}" in proc.stdout


def test_negative_control_int_ids_fail(tmp_path):
    panel = _panel()
    panel["loan_id"] = [1, 1, 2, 2]                      # the Fannie CSV-round-trip trap
    proc = _run(tmp_path, panel)
    assert proc.returncode == 1
    assert "FAIL  B: id column is string-typed" in proc.stdout


def test_negative_control_smuggled_leakage_in_schema_fails(tmp_path):
    schema = {"profile": {"numeric": ["original_ltv"]},
              "event": {"numeric": ["current_dlq"]}}     # leakage smuggled into the feature schema
    proc = _run(tmp_path, _panel(), schema=schema)
    assert proc.returncode == 1
    assert "FAIL  F:" in proc.stdout and "current_dlq" in proc.stdout


def test_negative_control_duplicate_entity_period_fails(tmp_path):
    panel = pd.concat([_panel(), _panel().iloc[[0]]], ignore_index=True)   # duplicate (L1, Jan)
    proc = _run(tmp_path, panel)
    assert proc.returncode == 1
    assert "FAIL  D:" in proc.stdout


def test_negative_control_gated_in_and_terminal_fails(tmp_path):
    panel = _panel()
    panel.loc[3, "is_performing"] = True                 # defaulted AND performing — impossible
    proc = _run(tmp_path, panel)
    assert proc.returncode == 1
    assert "FAIL  G:" in proc.stdout


def test_missing_contract_column_fails(tmp_path):
    proc = _run(tmp_path, _panel().drop(columns=["origination_date"]))
    assert proc.returncode == 1
    assert "FAIL  A:" in proc.stdout and "origination_date" in proc.stdout
