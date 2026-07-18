# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""classify_schema contract-enforcement tests (v1.1 G1.3).

Runs the real script end-to-end on a synthetic panel and proves the dataset contract's
``leakage:``/``exclude:`` columns are dropped BEFORE classification — they can never appear in
the emitted feature schema, no matter how informative they look.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parent.parent


def _panel(n_loans=30, n_months=4) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for lid in range(n_loans):
        ltv = float(rng.integers(40, 97))
        for m in range(n_months):
            rows.append({
                "loan_id": f"L{lid}", "reporting_date": f"2020-0{m+1}-28",
                "original_ltv": ltv,                                   # static numeric feature
                "current_rate": float(rng.uniform(3, 8)),              # dynamic numeric feature
                "current_dlq": int(rng.integers(0, 4)),                # LEAKAGE — looks useful!
                "deal_name": "DEAL-A",                                 # EXCLUDE (constant here too)
                "default_event": bool(rng.random() < 0.05),            # LEAKAGE (the label)
                "is_performing": True,                                 # LEAKAGE (the gate)
            })
    return pd.DataFrame(rows)


def _write_configs(tmp_path: Path) -> tuple[str, str, str]:
    panel = tmp_path / "train.parquet"
    _panel().to_parquet(panel, index=False)
    dataset = tmp_path / "dataset.yaml"
    dataset.write_text(yaml.safe_dump({
        "dataset": {"name": "toy", "adapter": "generic", "id_col": "loan_id",
                    "time_col": "reporting_date", "origination_col": "origination_date",
                    "origination_derived": True},
        "labels": {"default_12m": {"type": "forward_event", "event_col": "default_event",
                                   "horizon_months": 12, "gate_col": "is_performing"}},
        "leakage": ["current_dlq", "default_event", "is_performing"],
        "exclude": ["deal_name"],
    }))
    recipe = tmp_path / "classify.yaml"
    out = tmp_path / "schema.gen.yaml"
    recipe.write_text(yaml.safe_dump({
        "input": str(panel), "id_col": "loan_id", "time_col": "reporting_date",
        "dataset": str(dataset), "drop": [], "out": str(out), "key": None,
    }))
    return str(recipe), str(out), str(dataset)


def _run(recipe: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/classify_schema.py", "-c", recipe],
        cwd=REPO, capture_output=True, text=True,
        env={"PYTHONPATH": str(REPO / "src"), "PATH": "/usr/bin:/bin"})


def test_leakage_and_exclude_never_reach_the_schema(tmp_path):
    recipe, out, _ = _write_configs(tmp_path)
    proc = _run(recipe)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "LEAKAGE dropped pre-classification (3" in proc.stdout       # printed as its own category
    assert "EXCLUDE dropped pre-classification (1" in proc.stdout

    text = Path(out).read_text()
    for banned in ("current_dlq", "default_event", "is_performing", "deal_name"):
        assert banned not in text, f"banned column '{banned}' leaked into the generated schema"
    schema = yaml.safe_load(text)
    fields = [c for role in ("profile", "event")
              for cols in (schema.get(role) or {}).values() for c in cols]
    assert "original_ltv" in fields and "current_rate" in fields        # real features survive
    assert "Leakage/exclude enforced from" in text                       # provenance in the header


def test_without_dataset_pointer_behavior_is_unchanged(tmp_path):
    recipe, out, _ = _write_configs(tmp_path)
    r = Path(recipe)
    cfg = yaml.safe_load(r.read_text())
    del cfg["dataset"]                                   # legacy recipe: no contract wired
    r.write_text(yaml.safe_dump(cfg))
    proc = _run(recipe)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "LEAKAGE dropped" not in proc.stdout          # old behavior preserved (back-compat)
    assert "current_dlq" in Path(out).read_text()        # ...which is exactly why G1.3 exists
