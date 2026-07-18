# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Tests for the portfolio scorer (src/credit_fm/inference/scoring.py + scripts/score_portfolio.py).

Two layers, matching the repo convention:
  * logic — a real (tiny) model + tokenizer round-trips through save → load_finetuned → score_panel;
    scores are valid, the performing-gate is honoured, and the **leakage negative-control** holds
    (post-cutoff rows cannot change any score);
  * artifact — ``scripts/score_portfolio.py`` produces a scores file the validator passes, and
    ``scripts/validate_scores.py`` FAILS on a corrupted one.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import yaml

from credit_fm.inference.scoring import apply_lora, load_finetuned, observe_panel, score_panel
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer

ROOT = Path(__file__).resolve().parent.parent
CUTOFF = "2019-06-30"

CONFIG = {
    "id_col": "loan_id", "time_col": "reporting_date", "time_field": "loan_age",
    "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
    "event": {"numeric": ["current_interest_rate", "current_upb"], "categorical": []},
    "n_bins": 8, "max_categories": 64, "max_events": 60, "calendar": "yearquarter",
}


def _panel(n_loans=12, corrupt_future=False, non_performing=("L0", "L1")) -> pd.DataFrame:
    """Rows Jan–Aug 2019 (Jan–Jun <= cutoff, Jul–Aug after). Two loans are non-performing at the
    cutoff (gated out). ``corrupt_future`` mangles the post-cutoff rows — a scorer that respects the
    cutoff must ignore them entirely."""
    rng = np.random.default_rng(0)
    rows = []
    for k in range(n_loans):
        lid = f"L{k}"
        ltv = int(rng.integers(40, 97))
        chan = rng.choice(["R", "C", "B"])
        rate = float(rng.uniform(3, 8))
        for m in range(1, 9):                                   # months 1..8 of 2019
            after = m > 6
            r = rate
            upb = 200_000 - m * 1_000
            if after and corrupt_future:                        # poison the future only
                r = 99.0
                upb = -1
            rows.append({
                "loan_id": lid, "reporting_date": f"2019-{m:02d}-28",
                "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                "current_interest_rate": r, "current_upb": upb,
                # gate: non-performing at the cutoff = the June row is False
                "is_performing": not (lid in non_performing and m == 6),
                # forward label: L2/L5 default in month 7 (after the June cutoff) — for eval tests
                "default_event": (lid in ("L2", "L5") and m == 7),
            })
    return pd.DataFrame(rows)


def _tok(tmp: Path, panel: pd.DataFrame):
    tok = KVTTokenizer(CONFIG).fit(panel)
    p = tmp / "tok.json"
    tok.save(str(p))
    return tok, str(p)


def _save_ft_checkpoint(tmp: Path, tok, mode="full", name="ft.pt") -> Path:
    """A real (tiny) model saved in the finetune.py --save format."""
    torch.manual_seed(0)
    cfg = dict(vocab_size=tok.vocab_size, n_field_types=len(tok.field_types),
               dim=32, n_heads=2, profile_layers=1, event_layers=1, history_layers=1)
    model = CreditFoundationModel(**cfg)
    meta = {"mode": mode, "lora": ({"rank": 4, "alpha": 8} if mode == "lora" else None),
            "task": {"gate_col": "is_performing", "horizon_months": 12, "label_col": "default_event"},
            "metrics": {"val_roc": 0.80, "test_roc": 0.82, "test_ap": 0.011}}
    if mode == "lora":
        apply_lora(model, 4, 8)
    path = tmp / name
    torch.save({"config": cfg, "model": model.state_dict(), "finetune": meta}, path)
    return path


# ------------------------------------------------------------------ logic layer
def test_scores_are_valid_and_gate_is_honoured(tmp_path):
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    model, meta = load_finetuned(_save_ft_checkpoint(tmp_path, tok))

    df = score_panel(model, tok, tok_path, panel, "loan_id", "reporting_date", CUTOFF,
                     gate_col="is_performing")
    assert set(df.columns) == {"loan_id", "score", "n_events", "cutoff"}
    assert ((df["score"] >= 0) & (df["score"] <= 1)).all()          # valid probabilities
    assert not df["loan_id"].duplicated().any()                     # one row per loan
    assert (df["n_events"] >= 1).all()
    assert set(df["loan_id"]) == set(f"L{k}" for k in range(2, 12))  # L0/L1 gated out
    assert df["cutoff"].nunique() == 1


def test_ungated_scores_every_loan(tmp_path):
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    model, _ = load_finetuned(_save_ft_checkpoint(tmp_path, tok))
    df = score_panel(model, tok, tok_path, panel, "loan_id", "reporting_date", CUTOFF, gate_col=None)
    assert set(df["loan_id"]) == set(f"L{k}" for k in range(12))     # gate off → all loans


def test_leakage_negative_control_future_cannot_change_scores(tmp_path):
    """The core guard: two panels identical up to the cutoff but with wildly different post-cutoff
    rows must produce byte-identical scores — proving the scorer only sees history <= cutoff."""
    tok, tok_path = _tok(tmp_path, _panel())
    model, _ = load_finetuned(_save_ft_checkpoint(tmp_path, tok))

    clean = score_panel(model, tok, tok_path, _panel(corrupt_future=False),
                        "loan_id", "reporting_date", CUTOFF, gate_col="is_performing")
    poisoned = score_panel(model, tok, tok_path, _panel(corrupt_future=True),
                           "loan_id", "reporting_date", CUTOFF, gate_col="is_performing")
    a = clean.sort_values("loan_id").reset_index(drop=True)
    b = poisoned.sort_values("loan_id").reset_index(drop=True)
    assert (a["loan_id"].to_numpy() == b["loan_id"].to_numpy()).all()
    assert np.array_equal(a["score"].to_numpy(), b["score"].to_numpy())   # exact, not approx


def test_observe_panel_truncates_history_to_cutoff(tmp_path):
    obs = observe_panel(_panel(), "loan_id", "reporting_date", CUTOFF, gate_col=None)
    assert obs["reporting_date"].max() <= CUTOFF                     # nothing after the cutoff


def test_lora_checkpoint_round_trips(tmp_path):
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    model, meta = load_finetuned(_save_ft_checkpoint(tmp_path, tok, mode="lora", name="lora.pt"))
    assert meta["mode"] == "lora"
    df = score_panel(model, tok, tok_path, panel, "loan_id", "reporting_date", CUTOFF,
                     gate_col="is_performing")
    assert ((df["score"] >= 0) & (df["score"] <= 1)).all()


# ------------------------------------------------------------------ artifact layer
def _write_scoring_cfg(tmp: Path, ckpt: Path, tok_path: str, panel_path: Path, out: Path) -> Path:
    schema = tmp / "schema.yaml"
    schema.write_text(yaml.safe_dump({"id_col": "loan_id", "time_col": "reporting_date"}))
    cfg = {"checkpoint": str(ckpt), "tokenizer": tok_path, "schema": str(schema),
           "panel": str(panel_path), "cutoff": CUTOFF, "gate": True, "gate_col": "is_performing",
           "out": str(out), "limit": 0, "batch_size": 64, "workers": 0, "engine": "cpu",
           "key": None, "seed": 42}
    p = tmp / "scoring.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def _run(script, *args):
    return subprocess.run([sys.executable, str(ROOT / script), *args],
                          capture_output=True, text=True)


def test_score_portfolio_cli_then_validate_passes(tmp_path):
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    ckpt = _save_ft_checkpoint(tmp_path, tok)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path)
    out = tmp_path / "scores.parquet"
    cfg = _write_scoring_cfg(tmp_path, ckpt, tok_path, panel_path, out)

    r = _run("scripts/score_portfolio.py", "-c", str(cfg))
    assert r.returncode == 0, r.stderr
    assert out.exists() and (tmp_path / "scores_manifest.json").exists()
    manifest = json.loads((tmp_path / "scores_manifest.json").read_text())
    assert manifest["n_scored"] == 10                               # 12 loans - 2 gated

    v = _run("scripts/validate_scores.py", "--scores", str(out))
    assert v.returncode == 0, v.stdout + v.stderr
    assert "ALL CHECKS PASSED" in v.stdout


def test_validate_scores_fails_on_bad_scores(tmp_path):
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    ckpt = _save_ft_checkpoint(tmp_path, tok)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path)
    out = tmp_path / "scores.parquet"
    cfg = _write_scoring_cfg(tmp_path, ckpt, tok_path, panel_path, out)
    assert _run("scripts/score_portfolio.py", "-c", str(cfg)).returncode == 0

    df = pd.read_parquet(out)
    df.loc[0, "score"] = 1.5                                         # out of [0,1]
    df.to_parquet(out)
    v = _run("scripts/validate_scores.py", "--scores", str(out))
    assert v.returncode != 0
    assert "FAIL" in v.stdout


def test_validate_scores_quality_eval_runs(tmp_path):
    """--labeled-panel joins the forward default label and reports ROC/AP (both classes present)."""
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    ckpt = _save_ft_checkpoint(tmp_path, tok)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path)
    out = tmp_path / "scores.parquet"
    cfg = _write_scoring_cfg(tmp_path, ckpt, tok_path, panel_path, out)
    assert _run("scripts/score_portfolio.py", "-c", str(cfg)).returncode == 0

    v = _run("scripts/validate_scores.py", "--scores", str(out),
             "--labeled-panel", str(panel_path), "--horizon", "12")
    assert v.returncode == 0, v.stdout + v.stderr
    assert "forward-label eval" in v.stdout and "ROC=" in v.stdout
    assert "population" in v.stdout and "matched_in_scored" in v.stdout   # reconciliation shown
    assert "recall @ top-K" in v.stdout and "lift" in v.stdout            # lift table shown
    assert "G: scored loans all exist in the labeled panel" in v.stdout
    assert "ALL CHECKS PASSED" in v.stdout


def test_validate_scores_catches_wrong_population(tmp_path):
    """A scored file with a loan that isn't in the labeled panel = wrong snapshot -> G FAILS."""
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    ckpt = _save_ft_checkpoint(tmp_path, tok)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path)
    out = tmp_path / "scores.parquet"
    cfg = _write_scoring_cfg(tmp_path, ckpt, tok_path, panel_path, out)
    assert _run("scripts/score_portfolio.py", "-c", str(cfg)).returncode == 0

    df = pd.read_parquet(out)
    df.loc[df.index[0], "loan_id"] = "L9999_not_in_panel"       # a loan the panel never had
    df.to_parquet(out)
    v = _run("scripts/validate_scores.py", "--scores", str(out), "--labeled-panel", str(panel_path))
    assert v.returncode != 0
    assert "G: scored loans all exist in the labeled panel" in v.stdout and "FAIL" in v.stdout


def test_validate_scores_min_roc_gate_can_fail(tmp_path):
    """A random-init model can't clear ROC 0.99 -> the --min-roc gate FAILS (proves it bites)."""
    panel = _panel()
    tok, tok_path = _tok(tmp_path, panel)
    ckpt = _save_ft_checkpoint(tmp_path, tok)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path)
    out = tmp_path / "scores.parquet"
    cfg = _write_scoring_cfg(tmp_path, ckpt, tok_path, panel_path, out)
    assert _run("scripts/score_portfolio.py", "-c", str(cfg)).returncode == 0

    v = _run("scripts/validate_scores.py", "--scores", str(out),
             "--labeled-panel", str(panel_path), "--min-roc", "0.99")
    assert v.returncode != 0
    assert "H: ROC-AUC >= 0.99" in v.stdout
