# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Serving-example tests (v1.1 G6.2) — reference_implementations/fannie_mae/serve.py.

The HTTP path must be the batch path: same gate, same cutoff truncation, same calibrated PDs.
Skips cleanly when fastapi/httpx aren't installed (they're in the [serving]/[dev] extras).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import torch

pytest.importorskip("fastapi", reason="serving extra not installed")
pytest.importorskip("httpx", reason="httpx needed for fastapi TestClient")
from fastapi.testclient import TestClient  # noqa: E402

from credit_fm.inference.calibration import fit_calibrator, save_calibrator  # noqa: E402
from credit_fm.models import CreditFoundationModel  # noqa: E402
from credit_fm.tokenizer import KVTTokenizer  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "fannie_serve", ROOT / "reference_implementations" / "fannie_mae" / "serve.py")
serve = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(serve)

CUTOFF = "2019-06-30"
CONFIG = {"id_col": "loan_id", "time_col": "reporting_date", "time_field": "loan_age",
          "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
          "event": {"numeric": ["current_upb"], "categorical": []},
          "n_bins": 8, "max_categories": 64, "max_events": 60, "calendar": "yearquarter"}


def _panel(n_loans=8) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for k in range(n_loans):
        for m in range(1, 7):                              # Jan..Jun 2019 (all <= cutoff)
            rows.append({"loan_id": f"L{k}", "reporting_date": f"2019-{m:02d}-28",
                         "loan_age": 12 + m, "original_ltv": int(rng.integers(40, 97)),
                         "channel": ["R", "C"][k % 2], "current_upb": 200_000 - m * 1_000,
                         # L0 is non-performing at the cutoff (June row False) -> gated out
                         "is_performing": not (k == 0 and m == 6)})
    return pd.DataFrame(rows)


@pytest.fixture()
def client_and_panel(tmp_path):
    panel = _panel()
    tok = KVTTokenizer(CONFIG).fit(panel)
    tok_path = tmp_path / "tok.json"
    tok.save(str(tok_path))

    torch.manual_seed(0)
    cfg = dict(vocab_size=tok.vocab_size, n_field_types=len(tok.field_types),
               dim=32, n_heads=2, profile_layers=1, event_layers=1, history_layers=1)
    model = CreditFoundationModel(**cfg)
    ckpt = tmp_path / "ft.pt"
    torch.save({"config": cfg, "model": model.state_dict(),
                "finetune": {"mode": "full", "metrics": {"test_roc": 0.82}}}, ckpt)

    rng = np.random.default_rng(1)
    raw = rng.uniform(0.2, 0.9, 600)
    y = (rng.random(600) < raw * 0.1).astype(int)          # calibrated level ~ raw*0.1
    cal_path = tmp_path / "cal.json"
    save_calibrator(fit_calibrator(raw, y, "isotonic"), str(cal_path))

    app = serve.create_app(str(ckpt), str(tok_path), calibrator=str(cal_path), device="cpu")
    return TestClient(app), panel


def test_health(client_and_panel):
    client, _ = client_and_panel
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["calibrated"] is True
    assert body["calibrator_method"] == "isotonic"


def test_score_endpoint_scores_gated_loans_with_pd(client_and_panel):
    client, panel = client_and_panel
    r = client.post("/score", json={"cutoff": CUTOFF,
                                    "loans": json.loads(panel.to_json(orient="records"))})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["n_scored"] == 7 and body["calibrated"] is True     # L0 gated out
    ids = {s["loan_id"] for s in body["scores"]}
    assert "L0" not in ids and len(ids) == 7
    for s in body["scores"]:
        assert 0.0 <= s["score"] <= 1.0 and 0.0 <= s["pd"] <= 1.0
    ranks = [s["rank"] for s in body["scores"]]
    scores = [s["score"] for s in body["scores"]]
    assert ranks == list(range(1, 8)) and scores == sorted(scores, reverse=True)


def test_gate_off_scores_every_loan(client_and_panel):
    client, panel = client_and_panel
    r = client.post("/score", json={"cutoff": CUTOFF, "gate": False,
                                    "loans": json.loads(panel.to_json(orient="records"))})
    assert r.status_code == 200
    assert r.json()["n_scored"] == 8


def test_http_scores_equal_batch_scores(client_and_panel, tmp_path):
    """The example's core promise: an HTTP score == the score_panel batch score, exactly."""
    from credit_fm.inference.scoring import load_finetuned, score_panel
    client, panel = client_and_panel
    r = client.post("/score", json={"cutoff": CUTOFF,
                                    "loans": json.loads(panel.to_json(orient="records"))})
    http = {s["loan_id"]: s["score"] for s in r.json()["scores"]}

    # rebuild the same model/tokenizer the app holds (same seed/artifacts via the fixture files)
    app_state = client.app                                  # noqa: F841 — fixture artifacts reused
    tok = KVTTokenizer.load(str(tmp_path / "tok.json"))
    model, _ = load_finetuned(str(tmp_path / "ft.pt"))
    batch = score_panel(model, tok, str(tmp_path / "tok.json"), panel,
                        "loan_id", "reporting_date", CUTOFF, "is_performing")
    for _, row in batch.iterrows():
        assert http[row["loan_id"]] == pytest.approx(row["score"], abs=1e-9)


def test_missing_columns_rejected(client_and_panel):
    client, _ = client_and_panel
    r = client.post("/score", json={"cutoff": CUTOFF, "loans": [{"foo": 1}]})
    assert r.status_code == 422
    assert "loan_id" in r.text
    r2 = client.post("/score", json={"cutoff": CUTOFF, "loans": []})
    assert r2.status_code == 422
