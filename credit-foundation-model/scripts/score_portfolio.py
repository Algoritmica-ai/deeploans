# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Batch-score a portfolio with a fine-tuned Credit FM — deliverable #6 (inference).

Given a fine-tuned checkpoint, the frozen tokenizer, a portfolio panel and an observation date,
writes a per-loan default score. Leakage-safe: each loan's history is truncated to ``<= cutoff``
and (with a gate) only loans *performing* at the cutoff are scored — the same observation contract
as ``extract_embeddings.py`` / ``finetune.py``, so a score means "12-month default risk as of the
cutoff, from the past only".

Outputs ``<out>`` (parquet: ``loan_id, score, n_events, cutoff``) plus ``<out>_manifest.json``
(model/tokenizer lineage, cutoff, row count, score summary) for the artifact validator.

**Calibrated PDs (v1.1 G6.1)**: with ``--calibrator <calibrator.json>`` (from
``scripts/calibrate.py``) an extra ``pd`` column carries the calibrated probability of default.
``score`` stays the raw rank score; the mapping is monotone, so rankings are identical — ``pd``
just fixes the level (raw softmax ≫ base rate after rebalanced fine-tuning).

Config-driven (recipe: ``configs/mortgage_performance/scoring.yaml``)::

    python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml
    python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
        --panel gs://.../portfolio.parquet --cutoff 2023-12-31 --limit 1000
    python scripts/score_portfolio.py -c configs/mortgage_performance/scoring.yaml \
        --calibrator gs://.../calibrator.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import torch
import yaml

from credit_fm.inference.scoring import load_finetuned, score_panel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize
from credit_fm.utils.reproducibility import set_seed


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/mortgage_performance/scoring.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'checkpoint', 'panel', 'cutoff', 'gate', 'out')}", flush=True)
    set_seed(cfg.get_path("seed", 42))

    device = cfg.get_path("runtime.device") or ("cuda" if torch.cuda.is_available() else "cpu")
    use_amp = cfg.get_path("runtime.bf16", False) and device.startswith("cuda")

    model, meta = load_finetuned(cfg.checkpoint, cfg.key)
    model.to(device)
    schema = yaml.safe_load(open(cfg.schema))
    id_col, time_col = schema["id_col"], schema["time_col"]
    gate_col = None if cfg.get_path("gate") is False else (
        cfg.get_path("gate_col") or (meta.get("task") or {}).get("gate_col"))
    print(f"model: mode={meta.get('mode', '?')} "
          f"(test ROC {(meta.get('metrics') or {}).get('test_roc', '?')}); "
          f"gate={gate_col or 'none'}, cutoff={cfg.cutoff}", flush=True)

    tok = KVTTokenizer.load(cfg.tokenizer)
    storage.ensure_auth(cfg.panel, cfg.key)
    panel = storage.read_parquet(cfg.panel)

    scores = score_panel(
        model, tok, cfg.tokenizer, panel, id_col, time_col, cfg.cutoff, gate_col,
        limit=cfg.get_path("limit", 0), workers=cfg.get_path("workers", 0),
        engine=cfg.get_path("engine", "cpu"), key=cfg.key, device=device,
        bsz=cfg.get_path("batch_size", 256), use_amp=use_amp)

    cal = None
    cal_path = cfg.get_path("calibrator")
    if cal_path:                                       # G6.1: raw rank score -> calibrated PD
        from credit_fm.inference.calibration import apply_calibrator, load_calibrator
        cal = load_calibrator(cal_path, cfg.key)
        scores["pd"] = apply_calibrator(cal, scores["score"].to_numpy())
        print(f"calibrator: {cal['method']} <- {cal_path} "
              f"(fit on {cal['meta']['n']:,} loans @ base rate {cal['meta']['base_rate']:.4f}); "
              f"mean pd {scores['pd'].mean():.4f}" if len(scores) else "calibrator loaded", flush=True)

    out = cfg.out
    storage.ensure_auth(out, cfg.key)
    storage.write_parquet(scores, out)

    s = scores["score"]
    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "checkpoint": cfg.checkpoint, "tokenizer": cfg.tokenizer, "schema": cfg.schema,
        "panel": cfg.panel, "cutoff": str(cfg.cutoff), "gate_col": gate_col,
        "id_col": id_col, "n_scored": int(len(scores)),
        "score": {"min": float(s.min()) if len(s) else None,
                  "mean": float(s.mean()) if len(s) else None,
                  "max": float(s.max()) if len(s) else None},
        "calibrator": ({"path": str(cal_path), "method": cal["method"],
                        "pd_mean": float(scores["pd"].mean()) if len(scores) else None}
                       if cal else None),
        "finetune": meta,
    }
    storage.write_text(json.dumps(manifest, indent=2, default=str),
                       str(out).rsplit(".", 1)[0] + "_manifest.json")
    print(f"\nWrote {out}: {len(scores):,} loans scored"
          + (f" (score {s.min():.4f}..{s.max():.4f}, mean {s.mean():.4f})" if len(s) else ""))


if __name__ == "__main__":
    main()
