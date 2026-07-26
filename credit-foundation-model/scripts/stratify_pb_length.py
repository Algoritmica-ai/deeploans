# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Length-stratified FM-vs-baseline comparison for the payment-behaviours late-payment task.

The overall verdict (rolling-stats XGBoost beats the FM) hides *where* each model wins. The
foundation-model thesis is "sequence models help most on long, rich histories". This script
tests that directly: it scores **both** models on the identical test observations and reports
ROC / PR-AUC broken down by how many invoices of history a customer has at the cutoff.

Both models see the same set by construction — the observations/labels/loan-disjoint split are
built exactly as in ``baseline_payment_behaviours.py`` / ``finetune.py``. The baseline is
XGBoost on rolling-DPD features; the FM is scored via the real
:func:`~credit_fm.inference.scoring.score_panel` path on the saved fine-tuned checkpoint (no
reimplementation, so its numbers match the fine-tune). Bucketing uses the *true* history length;
note the tokenizer caps the FM's view at ``max_events`` invoices, so in the longest bucket the FM
only sees the most recent window — itself part of what we are measuring.

    python scripts/stratify_pb_length.py -c configs/payment_behaviours/finetune_late30.yaml
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score

# sibling script (scripts/ is on sys.path when this runs) — single source of the feature logic
from baseline_payment_behaviours import FEATURES, era_frame
from credit_fm.data.dataset_config import load_dataset_config
from credit_fm.data.labels import resolve_label_spec
from credit_fm.inference.scoring import load_finetuned, score_panel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli

# history-length buckets (invoices at the cutoff); last bucket exceeds the FM's max_events cap
BUCKETS = [(1, 3), (4, 8), (9, 16), (17, 32), (33, 64), (65, 128), (129, 10**9)]


def _metrics(y, p):
    """(ROC, PR-AUC) or (nan, base-rate) when a bucket has too few/one-class positives."""
    if len(y) < 30 or y.sum() < 5 or y.sum() == len(y):
        return float("nan"), float(y.mean()) if len(y) else float("nan")
    return roc_auc_score(y, p), average_precision_score(y, p)


def _hash_split(tr, te, id_col):
    """Loan-disjoint hash split — identical to finetune.py / the baseline (test set matches)."""
    overlap = set(pd.unique(tr[id_col])) & set(pd.unique(te[id_col]))
    if overlap:
        ov = pd.Series(sorted(overlap))
        to_test = set(ov[pd.util.hash_pandas_object(ov, index=False).to_numpy() % 2 == 0])
        to_train = overlap - to_test
        tr = tr[~tr[id_col].isin(to_test)]
        te = te[~te[id_col].isin(to_train)]
        print(f"  loan-disjoint: {len(overlap):,} loans span both eras (hash-split)", flush=True)
    return tr, te


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/payment_behaviours/finetune_late30.yaml")
    ds = load_dataset_config(cfg.dataset)
    spec = resolve_label_spec(cfg, ds)
    id_col, time_col = ds.id_col, ds.time_col
    train_cutoffs = [str(c) for c in cfg.task.train_cutoffs]
    test_cutoffs = [str(c) for c in cfg.task.test_cutoffs]

    storage.ensure_auth(cfg.panel, cfg.key)
    print(f"loading panel {cfg.panel} ...", flush=True)
    panel = storage.read_parquet(cfg.panel)
    panel[id_col] = panel[id_col].astype(str)

    tr = era_frame(panel, spec, id_col, time_col, train_cutoffs, "train", print)
    te = era_frame(panel, spec, id_col, time_col, test_cutoffs, "test", print)
    tr, te = _hash_split(tr, te, id_col)
    print(f"obs train {len(tr):,} ({tr['y'].mean()*100:.2f}%) | "
          f"test {len(te):,} ({te['y'].mean()*100:.2f}%)", flush=True)

    # --- baseline predictions on the test observations ---
    n_pos = max(int(tr["y"].sum()), 1)
    clf = xgb.XGBClassifier(
        n_estimators=400, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, scale_pos_weight=(len(tr) - n_pos) / n_pos, eval_metric="aucpr",
        n_jobs=0, tree_method="hist")
    clf.fit(tr[FEATURES].to_numpy(np.float32), tr["y"].to_numpy())
    te = te.copy()
    te["p_base"] = clf.predict_proba(te[FEATURES].to_numpy(np.float32))[:, 1]

    # --- FM predictions via the real scoring path (saved fine-tuned checkpoint) ---
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _ = load_finetuned(cfg.save, cfg.key)
    model.to(device)
    tok = KVTTokenizer.load(cfg.tokenizer)
    print(f"scoring FM ({cfg.save}) on {device} ...", flush=True)
    fm_parts = []
    for co in test_cutoffs:
        s = score_panel(model, tok, cfg.tokenizer, panel, id_col, time_col, co, spec.gate_col,
                        workers=cfg.get_path("workers", 0), engine=cfg.get_path("engine", "cpu"),
                        key=cfg.key, device=device, bsz=512, use_amp=(device == "cuda"))
        fm_parts.append(s.rename(columns={"score": "p_fm"}))
    fm = pd.concat(fm_parts, ignore_index=True)
    m = te.merge(fm[[id_col, "cutoff", "p_fm"]], on=[id_col, "cutoff"], how="inner")
    print(f"joined {len(m):,}/{len(te):,} test obs with FM scores", flush=True)

    # --- stratify by history length ---
    lines = []
    header = f"{'history (invoices)':>20} | {'obs':>8} {'%pos':>6} | {'base ROC':>8} {'FM ROC':>7}" \
             f" | {'base AP':>8} {'FM AP':>7} | winner"
    print("\n" + header)
    print("-" * len(header))
    lines += [f"# Length-stratified FM vs baseline — {spec.name}", "",
              f"Test obs: {len(m):,} ({m['y'].mean()*100:.2f}% positive). "
              "Both models scored on the identical set.", "",
              "| history (invoices) | obs | %pos | base ROC | FM ROC | base AP | FM AP | winner |",
              "|---|--:|--:|--:|--:|--:|--:|:--|"]
    for lo, hi in BUCKETS:
        b = m[(m["n"] >= lo) & (m["n"] <= hi)]
        if len(b) == 0:
            continue
        br, ba = _metrics(b["y"].to_numpy(), b["p_base"].to_numpy())
        fr, fa = _metrics(b["y"].to_numpy(), b["p_fm"].to_numpy())
        label = f"{lo}-{hi}" if hi < 10**9 else f"{lo}+"
        win = "—" if np.isnan(fr) or np.isnan(br) else ("FM" if fr > br else "baseline")
        print(f"{label:>20} | {len(b):>8,} {b['y'].mean()*100:>5.1f}% | "
              f"{br:>8.4f} {fr:>7.4f} | {ba:>8.4f} {fa:>7.4f} | {win}")
        lines.append(f"| {label} | {len(b):,} | {b['y'].mean()*100:.1f}% | {br:.4f} | {fr:.4f} "
                     f"| {ba:.4f} | {fa:.4f} | {win} |")
    ovr_b = _metrics(m["y"].to_numpy(), m["p_base"].to_numpy())
    ovr_f = _metrics(m["y"].to_numpy(), m["p_fm"].to_numpy())
    print(f"{'OVERALL':>20} | {len(m):>8,} {m['y'].mean()*100:>5.1f}% | "
          f"{ovr_b[0]:>8.4f} {ovr_f[0]:>7.4f} | {ovr_b[1]:>8.4f} {ovr_f[1]:>7.4f} |")
    lines += ["", f"**Overall**: baseline ROC {ovr_b[0]:.4f} / AP {ovr_b[1]:.4f}  ·  "
              f"FM ROC {ovr_f[0]:.4f} / AP {ovr_f[1]:.4f}", ""]

    report = cfg.get_path("report_stratify", "reports/pb_late30_stratified.md")
    storage.write_text("\n".join(lines) + "\n", report)
    print(f"\nwrote {report}")


if __name__ == "__main__":
    main()
