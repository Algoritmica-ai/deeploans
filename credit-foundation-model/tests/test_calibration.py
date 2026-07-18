# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Calibration tests (v1.1 G6.1) — the module, the stage script, and the validator's check I.

Design gates, verbatim: a synthetic known distortion is recovered (Brier drops, level lands on
the base rate, ranks untouched); the leakage negative-control holds (calibrating on a test-window
cutoff is REFUSED); and validate_scores' check I passes on honestly calibrated PDs while failing
on miscalibrated ones.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from credit_fm.inference.calibration import (apply_calibrator, brier, fit_calibrator,
                                             load_calibrator, reliability_table, save_calibrator)

ROOT = Path(__file__).resolve().parent.parent


def _distorted(n=6000, seed=0):
    """True PDs + outcomes + a raw score that RANKS right but sits far above the true level —
    exactly what a rebalance-trained softmax does."""
    rng = np.random.default_rng(seed)
    p_true = rng.beta(1, 30, size=n)                       # rare-event PDs, mean ~3%
    y = (rng.random(n) < p_true).astype(int)
    raw = p_true ** 0.25                                   # monotone distortion: mean ~0.4
    return p_true, y, raw


# ------------------------------------------------------------------ module
@pytest.mark.parametrize("method", ["isotonic", "platt"])
def test_known_distortion_is_recovered(method):
    _, y, raw = _distorted()
    cal = fit_calibrator(raw, y, method=method)
    pd_hat = apply_calibrator(cal, raw)

    assert cal["meta"]["brier_after"] < cal["meta"]["brier_before"] * 0.5   # big Brier win
    assert abs(pd_hat.mean() - y.mean()) < 0.01            # level lands on the base rate
    # monotone by construction -> ranking preserved (allowing isotonic ties)
    order = np.argsort(raw)
    assert (np.diff(pd_hat[order]) >= -1e-12).all()


def test_rank_metrics_unchanged_by_calibration():
    from sklearn.metrics import roc_auc_score
    _, y, raw = _distorted(seed=1)
    pd_hat = apply_calibrator(fit_calibrator(raw, y, "platt"), raw)   # strictly monotone
    assert roc_auc_score(y, pd_hat) == pytest.approx(roc_auc_score(y, raw), abs=1e-12)


def test_save_load_round_trip(tmp_path):
    _, y, raw = _distorted(n=800)
    cal = fit_calibrator(raw, y)
    path = tmp_path / "cal.json"
    save_calibrator(cal, str(path))
    loaded = load_calibrator(str(path))
    assert np.allclose(apply_calibrator(loaded, raw), apply_calibrator(cal, raw))
    with pytest.raises(ValueError, match="not a calibrator"):
        (tmp_path / "bad.json").write_text(json.dumps({"method": "magic"}))
        load_calibrator(str(tmp_path / "bad.json"))


def test_fit_requires_both_classes():
    with pytest.raises(ValueError, match="BOTH outcomes"):
        fit_calibrator(np.array([0.1, 0.2]), np.array([0, 0]))


def test_reliability_table_shape():
    _, y, raw = _distorted()
    rows = reliability_table(y, apply_calibrator(fit_calibrator(raw, y), raw))
    assert sum(r["n"] for r in rows) == len(y)
    assert all(0 <= r["mean_pd"] <= 1 and 0 <= r["realized"] <= 1 for r in rows)


def test_brier():
    assert brier(np.array([0, 1]), np.array([0.0, 1.0])) == 0.0
    assert brier(np.array([0, 1]), np.array([1.0, 0.0])) == 1.0


# ------------------------------------------------------------------ stage script + check I
def _run(script, *args):
    return subprocess.run([sys.executable, str(ROOT / script), *args],
                          capture_output=True, text=True)


def _scores_artifacts(tmp: Path, cutoff="2021-12-31", n=400, seed=3):
    """A scores parquet + manifest (as score_portfolio writes them) and a labeled panel whose
    forward window realizes defaults correlated with the score."""
    rng = np.random.default_rng(seed)
    ids = [f"{100003700000 + i}" for i in range(n)]
    p_true = rng.beta(1, 12, size=n)
    raw = p_true ** 0.25                                    # miscalibrated but rank-right
    defaulted = rng.random(n) < p_true

    scores = pd.DataFrame({"loan_id": ids, "score": raw, "n_events": 6, "cutoff": cutoff})
    scores_path = tmp / "cal_scores.parquet"
    scores.to_parquet(scores_path, index=False)
    (tmp / "cal_scores_manifest.json").write_text(json.dumps(
        {"id_col": "loan_id", "cutoff": cutoff, "n_scored": n, "checkpoint": "toy.pt",
         "score": {"min": float(raw.min()), "mean": float(raw.mean()), "max": float(raw.max())}}))

    month_after = (pd.to_datetime(cutoff) + pd.DateOffset(months=3)).strftime("%Y-%m-%d")
    panel = pd.DataFrame({
        "loan_id": ids * 2,
        "reporting_date": [cutoff] * n + [month_after] * n,
        "default_event": [False] * n + list(defaulted),
    })
    panel_path = tmp / "panel.parquet"
    panel.to_parquet(panel_path, index=False)
    return scores_path, panel_path


def _cal_cfg(tmp: Path, scores, panel, out, **extra) -> Path:
    cfg = {"scores": str(scores), "labeled_panel": str(panel), "label_col": "default_event",
           "time_col": "reporting_date", "horizon_months": 12, "method": "isotonic",
           "test_cutoffs": ["2022-12-31", "2023-12-31"], "bins": 10, "out": str(out), "key": None}
    cfg.update(extra)
    p = tmp / "calibrate_test.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def test_calibrate_script_end_to_end(tmp_path):
    scores, panel = _scores_artifacts(tmp_path)
    out = tmp_path / "calibrator.json"
    r = _run("scripts/calibrate.py", "-c", str(_cal_cfg(tmp_path, scores, panel, out)))
    assert r.returncode == 0, r.stderr
    assert "Brier" in r.stdout and "reliability" in r.stdout

    cal = json.loads(out.read_text())
    assert cal["method"] == "isotonic"
    assert cal["meta"]["brier_after"] < cal["meta"]["brier_before"]
    assert cal["lineage"]["cutoff"] == "2021-12-31"        # self-describing artifact


def test_calibrate_refuses_the_test_window(tmp_path):
    """The design's leakage negative-control: fitting on a test cutoff must be refused."""
    scores, panel = _scores_artifacts(tmp_path, cutoff="2022-12-31")   # a TEST cutoff
    out = tmp_path / "calibrator.json"
    r = _run("scripts/calibrate.py", "-c", str(_cal_cfg(tmp_path, scores, panel, out)))
    assert r.returncode != 0
    assert "REFUSED" in (r.stdout + r.stderr) and "test window" in (r.stdout + r.stderr)
    assert not out.exists()                                # nothing written


def test_validate_scores_check_i_passes_calibrated_and_fails_miscalibrated(tmp_path):
    scores_path, panel_path = _scores_artifacts(tmp_path)
    out = tmp_path / "calibrator.json"
    assert _run("scripts/calibrate.py", "-c",
                str(_cal_cfg(tmp_path, scores_path, panel_path, out))).returncode == 0

    # apply the calibrator the way score_portfolio does -> honest pd column
    cal = json.loads(out.read_text())
    df = pd.read_parquet(scores_path)
    df["pd"] = apply_calibrator(cal, df["score"].to_numpy())
    df.to_parquet(scores_path, index=False)
    v = _run("scripts/validate_scores.py", "--scores", str(scores_path),
             "--labeled-panel", str(panel_path))
    assert v.returncode == 0, v.stdout + v.stderr
    assert "I: calibrated pd in [0,1]" in v.stdout
    assert "I: calibration-in-the-large" in v.stdout and "Brier=" in v.stdout
    assert "ALL CHECKS PASSED" in v.stdout

    # negative control: a 'pd' column at the raw (miscalibrated) level must FAIL check I
    df["pd"] = df["score"]                                 # mean ~0.4 vs realized ~0.07
    df.to_parquet(scores_path, index=False)
    v2 = _run("scripts/validate_scores.py", "--scores", str(scores_path),
              "--labeled-panel", str(panel_path))
    assert v2.returncode != 0
    assert "FAIL" in v2.stdout and "calibration-in-the-large" in v2.stdout
