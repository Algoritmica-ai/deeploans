# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""FastAPI serving example — score loans over HTTP with a fine-tuned Credit FM (v1.1 G6.2).

**Explicitly an example, not a production service**: no auth, no TLS, no horizontal scaling —
it shows the shape of a serving integration and reuses the batch inference path
(``credit_fm.inference.scoring``) end-to-end, so an HTTP score is *identical* to a
``score_portfolio.py`` score: history truncated to the cutoff, performing-gate honoured, and
(with a calibrator) the same calibrated PD.

Run (needs ``pip install "credit_fm[serving]"``)::

    python reference_implementations/fannie_mae/serve.py \
        --checkpoint runs/m_100m_ft.pt --tokenizer configs/fannie_mae/tokenizer.json \
        --calibrator runs/calibrator.json --port 8000

Score (loan rows in the contract shape — one row per loan-month, history up to the cutoff)::

    curl -s localhost:8000/score -H 'Content-Type: application/json' -d '{
      "cutoff": "2023-12-31",
      "loans": [
        {"loan_id": "L1", "reporting_date": "2023-11-30", "loan_age": 23,
         "original_ltv": 80, "channel": "R", "current_upb": 190000,
         "current_interest_rate": 6.5, "is_performing": true},
        {"loan_id": "L1", "reporting_date": "2023-12-31", "loan_age": 24,
         "original_ltv": 80, "channel": "R", "current_upb": 189000,
         "current_interest_rate": 6.5, "is_performing": true}
      ]
    }'
"""

# NB: no `from __future__ import annotations` here — FastAPI must see the request model as a
# real class at runtime; postponed annotations turn the closure-local `ScoreRequest` hint into
# an unresolvable string and the body silently degrades to a query parameter.
import pandas as pd

from credit_fm.inference.calibration import apply_calibrator, load_calibrator
from credit_fm.inference.scoring import load_finetuned, score_panel
from credit_fm.tokenizer import KVTTokenizer


def create_app(checkpoint: str, tokenizer: str, *, id_col: str = "loan_id",
               time_col: str = "reporting_date", gate_col: str | None = "is_performing",
               calibrator: str | None = None, device: str = "cpu", key: str | None = None):
    """Build the FastAPI app: the model/tokenizer/calibrator are loaded ONCE, here."""
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel, Field

    model, meta = load_finetuned(checkpoint, key)
    model.to(device)
    tok = KVTTokenizer.load(tokenizer)
    cal = load_calibrator(calibrator, key) if calibrator else None

    class ScoreRequest(BaseModel):
        cutoff: str = Field(description="observation date YYYY-MM-DD; history after it is ignored")
        loans: list[dict] = Field(description=f"panel rows (one per loan-month) with at least "
                                              f"'{id_col}' and '{time_col}'")
        gate: bool = Field(default=True, description="score only loans performing at the cutoff")

    app = FastAPI(
        title="credit_fm scoring example",
        description="Example only — no auth/TLS/scaling. Reuses credit_fm.inference.scoring.")

    @app.get("/health")
    def health():
        return {"status": "ok", "checkpoint": checkpoint,
                "mode": meta.get("mode"), "metrics": meta.get("metrics"),
                "calibrated": cal is not None,
                "calibrator_method": cal["method"] if cal else None}

    @app.post("/score")
    def score(req: ScoreRequest):
        panel = pd.DataFrame(req.loans)
        missing = [c for c in (id_col, time_col) if c not in panel.columns]
        if len(panel) == 0 or missing:
            raise HTTPException(status_code=422,
                                detail=f"loans must be non-empty rows with columns "
                                       f"{[id_col, time_col]} (missing: {missing})")
        panel[id_col] = panel[id_col].astype(str)
        try:
            scores = score_panel(model, tok, tokenizer, panel, id_col, time_col, req.cutoff,
                                 gate_col if req.gate else None, device=device, key=key)
        except (ValueError, KeyError) as exc:              # bad field values / unknown columns
            raise HTTPException(status_code=422, detail=f"could not score panel: {exc}") from exc
        if cal is not None and len(scores):
            scores["pd"] = apply_calibrator(cal, scores["score"].to_numpy())
        scores = scores.sort_values("score", ascending=False).reset_index(drop=True)
        scores["rank"] = scores.index + 1                  # 1 = riskiest
        return {"cutoff": req.cutoff, "n_scored": int(len(scores)),
                "calibrated": cal is not None,
                "scores": scores.to_dict(orient="records")}

    return app


def main() -> None:
    import argparse

    import uvicorn
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--checkpoint", required=True, help="fine-tuned checkpoint (finetune.py --save)")
    ap.add_argument("--tokenizer", default="configs/fannie_mae/tokenizer.json")
    ap.add_argument("--calibrator", default=None, help="calibrator.json from calibrate.py (adds pd)")
    ap.add_argument("--id-col", default="loan_id")
    ap.add_argument("--time-col", default="reporting_date")
    ap.add_argument("--gate-col", default="is_performing")
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--key", default=None, help="GCS service-account key (for gs:// artifacts)")
    args = ap.parse_args()

    app = create_app(args.checkpoint, args.tokenizer, id_col=args.id_col, time_col=args.time_col,
                     gate_col=args.gate_col, calibrator=args.calibrator, device=args.device,
                     key=args.key)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
