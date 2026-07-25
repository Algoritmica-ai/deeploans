# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Fit a score → PD calibrator on a held-out calibration window (v1.1 G6.1).

The fine-tuned scorer ranks loans well but its raw softmax is not a probability (it was trained
on a rebalanced sample). This stage fits a monotone mapping raw-score → PD on a **labeled PAST
cutoff** and writes a JSON calibrator that ``score_portfolio.py --calibrator`` applies.

Workflow::

    # 1. score the portfolio at the CALIBRATION cutoff (past; outcomes exist; NOT a test cutoff)
    python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
        --cutoff 2021-12-31 --out gs://.../calibration_scores.parquet
    # 2. fit the calibrator on those scores + realized outcomes
    python scripts/calibrate.py -c configs/mortgage_performance/calibrate.yaml
    # 3. score for real, calibrated
    python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
        --calibrator gs://.../calibrator.json

**Embargo guard**: the recipe lists the protocol's ``test_cutoffs``; this script REFUSES to fit
on any of them (or later). Calibrating on the window you report metrics on flatters Brier the
same way peeking flatters ROC — the same discipline as the loan-disjoint/temporal split, extended.

Rank metrics are untouched by construction: both methods are monotone, so ROC/recall@K on the
calibrated PDs equal those on the raw scores.
"""

from __future__ import annotations

import json

import pandas as pd

from credit_fm.inference.calibration import (brier, fit_calibrator, reliability_table,
                                             save_calibrator)
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def forward_labels(scores: pd.DataFrame, panel: pd.DataFrame, *, id_col: str, time_col: str,
                   label_col: str, horizon_months: int, cutoff) -> pd.Series:
    """0/1 outcome per scored loan: did ``label_col`` fire within the horizon after the cutoff?

    Same join as ``validate_scores``'s quality block — kept consistent so the calibrator is fit
    on exactly the labels the auditor scores against.
    """
    cutoff = pd.to_datetime(cutoff)
    hi = cutoff + pd.DateOffset(months=horizon_months)
    dt = pd.to_datetime(panel[time_col], errors="coerce")
    window = panel[(dt > cutoff) & (dt <= hi) & panel[label_col].fillna(False).astype(bool)]
    defaulted = set(window[id_col].astype(str))
    return scores[id_col].astype(str).isin(defaulted).astype(int)


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/mortgage_performance/calibrate.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'scores', 'labeled_panel', 'method', 'horizon_months', 'test_cutoffs', 'out')}",
          flush=True)

    storage.ensure_auth(cfg.scores, cfg.key)
    scores = storage.read_parquet(cfg.scores)
    manifest_path = str(cfg.scores).rsplit(".", 1)[0] + "_manifest.json"
    manifest = json.loads(storage.read_text(manifest_path))
    id_col = manifest["id_col"]
    cutoff = pd.to_datetime(manifest["cutoff"])

    # ---- embargo guard: never fit on the test window -------------------------------------
    test_cutoffs = [pd.to_datetime(str(c)) for c in (cfg.get_path("test_cutoffs") or [])]
    if test_cutoffs and cutoff >= min(test_cutoffs):
        raise SystemExit(
            f"REFUSED: calibration cutoff {cutoff.date()} is in the test window "
            f"(test cutoffs start {min(test_cutoffs).date()}). Fitting the calibrator on the "
            "window you report metrics on is leakage — score an earlier cutoff (e.g. the last "
            "fit-window cutoff) and calibrate on that.")

    storage.ensure_auth(cfg.labeled_panel, cfg.key)
    panel = storage.read_parquet(cfg.labeled_panel,
                                 columns=[id_col, cfg.time_col, cfg.label_col])
    y = forward_labels(scores, panel, id_col=id_col, time_col=cfg.time_col,
                       label_col=cfg.label_col, horizon_months=cfg.horizon_months, cutoff=cutoff)
    s = scores["score"].to_numpy()
    print(f"calibration window: cutoff {cutoff.date()}, {len(s):,} loans, "
          f"{int(y.sum()):,} positives (base rate {y.mean()*100:.3f}%)", flush=True)

    cal = fit_calibrator(s, y.to_numpy(), method=cfg.get_path("method", "isotonic"))
    cal["lineage"] = {"scores": str(cfg.scores), "cutoff": str(cutoff.date()),
                      "labeled_panel": str(cfg.labeled_panel), "label_col": cfg.label_col,
                      "horizon_months": int(cfg.horizon_months),
                      "checkpoint": manifest.get("checkpoint"), "config": cfg.to_dict()}

    m = cal["meta"]
    print(f"method {cal['method']}: Brier {m['brier_before']:.6f} -> {m['brier_after']:.6f}  "
          f"(mean raw score {s.mean():.4f} vs base rate {m['base_rate']:.4f})", flush=True)
    from credit_fm.inference.calibration import apply_calibrator
    print("reliability (calibrated):")
    for row in reliability_table(y.to_numpy(), apply_calibrator(cal, s),
                                 bins=int(cfg.get_path("bins", 10) or 10)):
        print(f"  {row['bin']}  n={row['n']:>7,}  mean_pd={row['mean_pd']:.4f}  "
              f"realized={row['realized']:.4f}")
    assert m["brier_after"] <= brier(y.to_numpy(), s) + 1e-12   # calibration must not hurt in-fit

    storage.ensure_auth(cfg.out, cfg.key)
    save_calibrator(cal, cfg.out)
    print(f"wrote calibrator -> {cfg.out}\n"
          f"apply it:  python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml "
          f"--calibrator {cfg.out}")


if __name__ == "__main__":
    main()
