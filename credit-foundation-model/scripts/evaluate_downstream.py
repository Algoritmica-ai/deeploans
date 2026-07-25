# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Downstream verdict — do the FM's ``[USR]`` embeddings beat features-XGBoost at predicting default?

Given per-loan embeddings (from ``extract_embeddings.py``, observed at ``--cutoff``) and the full
processed panel, this:

1. builds the **forward-default label** — 1 if the loan defaults within ``--horizon-months`` *after*
   the cutoff (the same D180 / zero-balance event the baseline uses), 0 otherwise;
2. builds the **baseline feature snapshot** as-of the cutoff (the tokenizer's profile+event fields —
   i.e. the *same information* the FM saw, so the comparison isolates *representation*);
3. splits loans **disjointly** (hash on id), then trains a head three ways —
   **features / FM embeddings / combined** (XGBoost) plus a **linear probe** on the embeddings —
   and reports **ROC-AUC / PR-AUC** on the held-out loans.

The winner is decided on the held-out set: if *embeddings* (or *combined*) beats *features*, the FM
adds signal beyond the raw as-of-cutoff features. (For a calendar-OOT verdict vs the 0.757/0.784
bars, run this on a multi-year panel with a train-years/test-years split — see the runbook.)

Config-driven (recipe: ``configs/mortgage_performance/evaluate.yaml``)::

    python scripts/evaluate_downstream.py -c configs/mortgage_performance/evaluate.yaml
    python scripts/evaluate_downstream.py -c configs/mortgage_performance/evaluate.yaml --task.horizon_months 6
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import yaml
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.preprocessing import StandardScaler

from credit_fm.data.labels import forward_event_entities, resolve_label_spec
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def features_asof(panel, id_col, time_col, cols, cutoff):
    """Each loan's most-recent row at/<= cutoff, restricted to the feature columns (the snapshot)."""
    hist = panel[pd.to_datetime(panel[time_col], errors="coerce") <= pd.to_datetime(cutoff)]
    snap = hist.sort_values(time_col).groupby(id_col).tail(1).set_index(id_col)
    return snap[[c for c in cols if c in snap.columns]]


def encode_features(df, cats, nums, cmap=None):
    """Map categoricals to ints (fit on train), coerce numerics — returns (X, cmap)."""
    df = df.copy()
    fit = cmap is None
    cmap = cmap or {}
    for c in cats:
        if fit:
            cmap[c] = {v: i for i, v in enumerate(pd.Series(df[c].dropna().unique()))}
        df[c] = df[c].map(cmap[c]).astype("float")
    for c in nums:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[cats + nums], cmap


def _xgb(device):
    return xgb.XGBClassifier(n_estimators=300, max_depth=6, learning_rate=0.05, subsample=0.8,
                             colsample_bytree=0.8, eval_metric="aucpr", tree_method="hist",
                             device=device, n_jobs=-1)


def _score(model, Xtr, ytr, Xte, yte):
    model.fit(Xtr, ytr)
    p = model.predict_proba(Xte)[:, 1]
    return roc_auc_score(yte, p), average_precision_score(yte, p)


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/mortgage_performance/evaluate.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'embeddings', 'panel', 'task', 'split', 'xgb_device', 'report')}",
          flush=True)
    cutoff = cfg.task.cutoff
    spec = resolve_label_spec(cfg)                   # task.label from dataset.yaml (or legacy keys)
    horizon = spec.horizon_months

    schema = yaml.safe_load(open(cfg.schema))
    id_col, time_col = schema["id_col"], schema["time_col"]
    cats = list(schema["profile"]["categorical"]) + list(schema["event"]["categorical"])
    nums = list(schema["profile"]["numeric"]) + list(schema["event"]["numeric"])

    storage.ensure_auth(cfg.embeddings, cfg.key)
    emb = storage.read_parquet(cfg.embeddings)
    panel = storage.read_parquet(cfg.panel)
    ecols = [c for c in emb.columns if c.startswith("e") and c[1:].isdigit()]
    print(f"embeddings: {len(emb):,} loans x {len(ecols)} dims; panel {len(panel):,} rows", flush=True)

    # 1) forward-default label
    defaulted = forward_event_entities(panel, spec, id_col=id_col, time_col=time_col, cutoff=cutoff)
    emb["y"] = emb[id_col].isin(defaulted).astype(int)

    # 2) baseline feature snapshot as-of cutoff
    feats = features_asof(panel, id_col, time_col, cats + nums, cutoff)
    df = emb.merge(feats, left_on=id_col, right_index=True, how="left").reset_index(drop=True)

    # 3) loan-disjoint split (seeded rng — reproducible)
    rng = np.random.default_rng(cfg.seed)
    is_test = rng.random(len(df)) < cfg.split.test_frac   # one row per loan -> loan-disjoint
    tr, te = df[~is_test], df[is_test]
    print(f"split: train {len(tr):,} ({tr.y.mean()*100:.2f}% default) | "
          f"test {len(te):,} ({te.y.mean()*100:.2f}% default)", flush=True)

    Xtr_f, cmap = encode_features(tr, cats, nums)
    Xte_f, _ = encode_features(te, cats, nums, cmap)
    Xtr_e, Xte_e = tr[ecols].to_numpy(), te[ecols].to_numpy()

    # blueprint recipe: PCA-compress embeddings before XGBoost sees them — raw 384 dims next to
    # ~30 features dilute the tree splits; ~64 principal dims keep the signal, drop the noise
    pca_dims = cfg.get_path("pca_dims")
    emb_tag = "FM embeddings (XGB)"
    if pca_dims:
        from sklearn.decomposition import PCA
        pca = PCA(n_components=int(pca_dims), random_state=cfg.seed).fit(Xtr_e)
        Xtr_p, Xte_p = pca.transform(Xtr_e), pca.transform(Xte_e)
        print(f"PCA: {Xtr_e.shape[1]} -> {pca_dims} dims "
              f"({pca.explained_variance_ratio_.sum()*100:.0f}% variance kept)", flush=True)
        emb_tag = f"FM embeddings (XGB, PCA-{pca_dims})"
    else:
        Xtr_p, Xte_p = Xtr_e, Xte_e
    Xtr_c = np.hstack([Xtr_f.to_numpy(), Xtr_p])
    Xte_c = np.hstack([Xte_f.to_numpy(), Xte_p])
    ytr, yte = tr.y.to_numpy(), te.y.to_numpy()

    results = []
    results.append(("features (XGB)", *_score(_xgb(cfg.xgb_device), Xtr_f, ytr, Xte_f, yte)))
    results.append((emb_tag, *_score(_xgb(cfg.xgb_device), Xtr_p, ytr, Xte_p, yte)))
    results.append(("combined (XGB)", *_score(_xgb(cfg.xgb_device), Xtr_c, ytr, Xte_c, yte)))
    sc = StandardScaler().fit(Xtr_e)
    lr = LogisticRegression(max_iter=1000, class_weight="balanced")
    results.append(("FM embeddings (linear probe)",
                    *_score(lr, sc.transform(Xtr_e), ytr, sc.transform(Xte_e), yte)))

    base_roc, base_pr = results[0][1], results[0][2]
    print("\n=== Downstream: default within "
          f"{horizon}mo of {cutoff} (held-out loans) ===")
    print("  (AP/PR-AUC first — at ~0.1% base rate ROC saturates; AP is the operational metric)")
    print(f"  {'model':<34}{'AP':>9}{'dAP':>9}{'ROC':>9}{'dROC':>9}")
    for name, roc, pr in results:
        print(f"  {name:<34}{pr:>9.4f}{pr-base_pr:>+9.4f}{roc:>9.4f}{roc-base_roc:>+9.4f}")

    if cfg.get_path("report"):
        rep = Path(cfg.report)
        rep.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Mortgage Performance — FM Downstream Eval (loan-holdout probe)", "",
            f"Observed at **{cutoff}**, label = default within **{horizon} months** "
            f"after. {len(df):,} performing loans; loan-disjoint "
            f"{int(cfg.split.test_frac*100)}% held out. "
            "Features = tokenizer profile+event fields as-of cutoff (same info the FM saw).", "",
            "| model | AP (PR-AUC) | dAP | ROC-AUC | dROC |", "|---|--:|--:|--:|--:|",
            *[f"| {n} | {pr:.4f} | {pr-base_pr:+.4f} | {roc:.4f} | {roc-base_roc:+.4f} |"
              for n, roc, pr in results],
            "", "## Read",
            "- **AP (PR-AUC) first**: at ~0.1% positives ROC-AUC saturates and hides the "
            "differences that matter operationally (a fixed-capacity review team's catch rate). "
            "This is the NVIDIA blueprint's own framing.",
            "- If **FM embeddings** or **combined** beats **features**, the FM adds signal beyond the "
            "raw as-of-cutoff features — the thesis, on real loans.",
            "- This is a loan-holdout probe on one window, not the calendar-OOT vs 0.757/0.784 "
            "(which needs a multi-year panel + train-years/test-years split).",
        ]
        rep.write_text("\n".join(lines))
        print(f"\nWrote {rep}")


if __name__ == "__main__":
    main()
