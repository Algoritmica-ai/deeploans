# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generic XGBoost baseline for credit default — the Gate-G1 anchor the FM must beat.

All asset-specific knobs (id/time/label columns, observation date + horizon, the
"performing" gate, non-feature and leakage columns, the eval-only segment latent) live in a
YAML config, so this script is schema-agnostic. Reads the loan-stratified temporal splits
from ``scripts/prepare_data.py`` and predicts the configured event within ``horizon_months``.

Four configs separate signal from leakage (full/clean x gate); the honest bar is config (4).
With ``--book`` (loan_book + segment latent) it also runs the hidden-segment ceiling validation.

    python scripts/train_baseline.py --config configs/dutch_mortgages/baseline.yaml \
        --data-dir data/processed --book data/raw/loan_book.parquet --report reports/baseline_report.md
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import yaml
from sklearn.metrics import (accuracy_score, average_precision_score, recall_score, roc_auc_score)

from credit_fm.utils import storage


def forward_cutoffs(path: str, time_col: str, obs: str, horizon: int) -> list[str]:
    """The next `horizon` distinct cutoffs strictly after `obs` (ISO dates sort chronologically)."""
    cutoffs = sorted(pd.read_parquet(path, columns=[time_col])[time_col].astype(str).unique())
    return [c for c in cutoffs if c > obs][:horizon]


def load_obs(path: str, cfg: dict, fwd: list[str]) -> pd.DataFrame:
    tc, idc, lc, lv = cfg["time_col"], cfg["id_col"], cfg["label_col"], str(cfg["label_value"])
    obs = pd.read_parquet(path, filters=[(tc, "=", cfg["obs_date"])])
    fut = pd.read_parquet(path, columns=[idc, lc], filters=[(tc, "in", fwd)])
    bad = set(fut.loc[fut[lc].astype(str) == lv, idc])
    obs["y"] = obs[idc].isin(bad).astype(int)
    return obs


def encode(df, cat_cols, num_cols, cats_map=None):
    df = df.copy()
    fit = cats_map is None
    cats_map = cats_map or {}
    for c in cat_cols:
        if fit:
            cats_map[c] = {v: i for i, v in enumerate(pd.Series(df[c].dropna().unique()))}
        df[c] = df[c].map(cats_map[c]).astype("float")
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[cat_cols + num_cols], cats_map


def _xgb(**kw):
    return xgb.XGBClassifier(n_estimators=300, max_depth=5, learning_rate=0.05, subsample=0.8,
                             colsample_bytree=0.8, eval_metric="auc", n_jobs=-1,
                             tree_method="hist", early_stopping_rounds=20, **kw)


def run_xgb(name, parts):
    Xtr, ytr, Xva, yva, Xte, yte = parts
    t = time.time()
    m = _xgb()
    m.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    rows = []
    for lab, X, y in [("train", Xtr, ytr), ("val", Xva, yva), ("test", Xte, yte)]:
        if y.sum() < 5:
            rows.append((lab, len(y), int(y.sum()), float("nan"), float("nan")))
            continue
        p = m.predict_proba(X)[:, 1]
        rows.append((lab, len(y), int(y.sum()), roc_auc_score(y, p), average_precision_score(y, p)))
    print(f"\n=== {name}  ({time.time()-t:.0f}s, {Xtr.shape[1]} feats) ===")
    print(f"  {'split':<6}{'n':>9}{'pos':>7}{'pos%':>8}{'ROC-AUC':>10}{'PR-AUC':>10}")
    for lab, n, pos, roc, pr in rows:
        rs = f"{roc:.4f}" if not np.isnan(roc) else " n/a"
        ps = f"{pr:.4f}" if not np.isnan(pr) else " n/a"
        print(f"  {lab:<6}{n:>9,}{pos:>7,}{pos/max(n,1)*100:>7.2f}%{rs:>10}{ps:>10}")
    return rows


def segment_validation(obs, cat_clean, num_clean, cfg):
    """Ceiling validation on the Gate-G1 (gated) cohort. Returns report lines."""
    seg = cfg["segment_col"]
    gated = {s: o[o[cfg["gate_col"]].isin(cfg["gate_values"])].dropna(subset=[seg])
             for s, o in obs.items()}
    tr, va, te = gated["train"], gated["val"], gated["test"]
    names = cfg.get("segment_names") or {}

    def lbl(s):
        return f"{int(s)} ({names[int(s)]})" if int(s) in names else str(int(s))

    g = te.groupby(seg)["y"].agg(["count", "mean"])
    spread = g["mean"].max() / max(g["mean"].min(), 1e-9)

    def fit_eval(extra):
        Xtr, cm = encode(tr, cat_clean, num_clean + extra)
        Xva, _ = encode(va, cat_clean, num_clean + extra, cm)
        Xte, _ = encode(te, cat_clean, num_clean + extra, cm)
        m = _xgb()
        m.fit(Xtr, tr.y, eval_set=[(Xva, va.y)], verbose=False)
        p = m.predict_proba(Xte)[:, 1]
        return roc_auc_score(te.y, p), average_precision_score(te.y, p)

    roc0, pr0 = fit_eval([])
    roc1, pr1 = fit_eval([seg])

    # (C) can a tabular model recover the segment from observables?
    Xtr, cm = encode(tr, cat_clean, num_clean)
    Xte, _ = encode(te, cat_clean, num_clean, cm)
    mc = xgb.XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, n_jobs=-1,
                           tree_method="hist")
    mc.fit(Xtr, tr[seg].astype(int))
    yte = te[seg].astype(int)
    pred = mc.predict(Xte)
    acc = accuracy_score(yte, pred)
    maj = yte.value_counts(normalize=True).max()
    segs = sorted(yte.unique())
    rec = recall_score(yte, pred, labels=segs, average=None, zero_division=0)
    sup = yte.value_counts()

    print()
    print("=" * 64)
    print(f"CEILING VALIDATION (hidden {seg}, gated cohort)")
    print("=" * 64)
    print("(A) segment-conditional default rate (test):")
    for s, r in g.iterrows():
        print(f"    {lbl(s)}: {int(r['count']):>6,} loans  {r['mean']*100:5.2f}%")
    print(f"    spread: {spread:.0f}x")
    print(f"(B) oracle-{seg} lift: ROC {roc0:.3f}->{roc1:.3f} (+{roc1-roc0:.3f}); "
          f"PR-AUC {pr0:.3f}->{pr1:.3f} (+{(pr1-pr0)/max(pr0,1e-9)*100:.0f}%)")
    print(f"(C) recover {seg}: acc {acc*100:.1f}% vs majority {maj*100:.1f}% | per-segment recall:")
    for s, r in zip(segs, rec):
        print(f"    {lbl(s)}: recall {r*100:.1f}% ({int(sup[s]):,} loans)")

    return [
        "", f"## Architectural validation — the hidden `{seg}` ceiling", "",
        f"The generator assigns each loan a hidden fragility latent `{seg}` (in `loan_book`, not "
        "the panel -- evaluation-only, never a feature). It drives default but is largely "
        "inaccessible to tabular models. On the Gate-G1 cohort:", "",
        "**(A)** the hidden segment is a large source of default risk:", "",
        "| Segment | loans (test) | default rate |", "|---|--:|--:|",
        *[f"| {lbl(s)} | {int(r['count']):,} | {r['mean']*100:.2f}% |" for s, r in g.iterrows()],
        f"| **spread** | | **{spread:.0f}x** |", "",
        "**(B)** if a model could *see* the segment, accuracy jumps (oracle -- diagnostic only):", "",
        "| | ROC-AUC | PR-AUC |", "|---|--:|--:|",
        f"| Gate G1 (observables only) | {roc0:.3f} | {pr0:.3f} |",
        f"| + oracle `{seg}` | {roc1:.3f} | {pr1:.3f} |",
        f"| **headroom** | **+{roc1-roc0:.3f}** | **+{(pr1-pr0)/max(pr0,1e-9)*100:.0f}%** |", "",
        f"**(C)** but a tabular model *can't* recover the segment -- overall {acc*100:.0f}% "
        f"accuracy vs {maj*100:.0f}% from always guessing the majority. Per-segment recall shows "
        "it leans on the majority class and misses the rest:", "",
        "| Segment | loans (test) | recall |", "|---|--:|--:|",
        *[f"| {lbl(s)} | {int(sup[s]):,} | {r*100:.1f}% |" for s, r in zip(segs, rec)],
        "",
        "**Conclusion.** The segment is a real, large source of default risk (A) that tabular "
        "models can only weakly recover (C); a model that could fully see it would nearly double "
        "PR-AUC (B). The foundation model reads each loan's behavioural *sequence* to recover that "
        "latent -- that headroom above the baseline is the project's thesis, now quantified.",
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="configs/dutch_mortgages/baseline.yaml")
    ap.add_argument("--data-dir", default="data/processed",
                    help="splits location; local path or gs:///s3:// URL")
    ap.add_argument("--key", default=storage.GCS_DEFAULT_KEY, help="GCS service-account JSON")
    ap.add_argument("--book", default="data/raw/loan_book.parquet")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))
    d = args.data_dir.rstrip("/")                 # pluggable: local path or gs:///s3:// URL
    storage.ensure_auth(d, args.key)
    idc, tc, lc = cfg["id_col"], cfg["time_col"], cfg["label_col"]

    t0 = time.time()
    fwd = forward_cutoffs(storage.join(d, "train.parquet"), tc,
                          cfg["obs_date"], int(cfg["horizon_months"]))
    print(f"obs {cfg['obs_date']} -> forward {fwd[0]}..{fwd[-1]} ({len(fwd)} cutoffs)")
    obs = {s: load_obs(storage.join(d, f"{s}.parquet"), cfg, fwd) for s in ("train", "val", "test")}
    cohort = {}
    for s, o in obs.items():
        og = o[o[cfg["gate_col"]].isin(cfg["gate_values"])]
        cohort[s] = (len(o), len(og), o.y.mean() * 100, og.y.mean() * 100)
        print(f"  {s}: {len(o):,} loans (gated {len(og):,}), "
              f"default {cohort[s][2]:.2f}% / gated {cohort[s][3]:.2f}%")

    non_feat = {idc, tc, "y"} | set(cfg["exclude"])
    leak = set(cfg["leakage"])
    tr = obs["train"]
    cat_full = [c for c in tr.columns if c not in non_feat and tr[c].dtype == object]
    num_full = [c for c in tr.columns if c not in non_feat and tr[c].dtype != object]
    cat_clean = [c for c in cat_full if c not in leak]
    num_clean = [c for c in num_full if c not in leak]
    print(f"  features: full={len(cat_full)+len(num_full)}  clean={len(cat_clean)+len(num_clean)}")

    def prep(cat, num, gated):
        def slc(o):
            return o[o[cfg["gate_col"]].isin(cfg["gate_values"])] if gated else o
        Xtr, cm = encode(slc(obs["train"]), cat, num)
        Xva, _ = encode(slc(obs["val"]), cat, num, cm)
        Xte, _ = encode(slc(obs["test"]), cat, num, cm)
        return (Xtr, slc(obs["train"]).y.to_numpy(), Xva, slc(obs["val"]).y.to_numpy(),
                Xte, slc(obs["test"]).y.to_numpy())

    cfgs = [("(1) full features, no gate", cat_full, num_full, False),
            ("(2) full features + gate", cat_full, num_full, True),
            ("(3) no-leakage features, no gate", cat_clean, num_clean, False),
            ("(4) no-leakage + gate (Gate G1)", cat_clean, num_clean, True)]
    results = {name: run_xgb(name, prep(c, n, g)) for name, c, n, g in cfgs}

    print("\n" + "=" * 64 + "\nSUMMARY (test split)\n" + "=" * 64)
    summ = []
    for name in results:
        _, n, pos, roc, pr = next(r for r in results[name] if r[0] == "test")
        print(f"  {name:<40}{roc:>8.4f}{pr:>8.4f}{pos/max(n,1)*100:>7.2f}%")
        summ.append((name, n, pos, roc, pr))

    seg_lines = []
    book = Path(args.book)
    if book.exists() and cfg.get("segment_col"):
        bk = pd.read_parquet(book, columns=[idc, cfg["segment_col"]])
        obs = {s: o.merge(bk, on=idc, how="left") for s, o in obs.items()}
        seg_lines = segment_validation(obs, cat_clean, num_clean, cfg)
    else:
        print("\n(loan_book/segment not available — ceiling validation skipped)")

    if args.report:
        rep = Path(args.report)
        rep.parent.mkdir(parents=True, exist_ok=True)
        n_full = len(cat_full) + len(num_full)
        n_clean = len(cat_clean) + len(num_clean)
        gate_vals = ", ".join(map(str, cfg["gate_values"]))
        lines = [
            "# Baseline Report — XGBoost (Gate G1)", "",
            f"Config `{args.config}` · split `{args.data_dir}` (loan-stratified temporal, DL-007).",
            "", "## Setup", "",
            f"- **Task:** observe each loan at `{cfg['obs_date']}`; predict whether "
            f"`{lc}`=={cfg['label_value']} within the next **{cfg['horizon_months']} cutoffs** "
            f"(`{fwd[0]}` ... `{fwd[-1]}`).",
            "- **Population:** loans present at the observation date; loan-stratified temporal "
            "split (train trains, test scores).",
            f"- **Gate** keeps only currently-performing loans (`{cfg['gate_col']}` in "
            f"{{{gate_vals}}}) -> predict *new* events.",
            f"- **Feature sets:** *full* = {n_full}; *clean* = {n_clean} (drops the "
            f"{len(cfg['leakage'])} leakage columns below).",
            "", "**Population** (loans observed at the date):", "",
            "| Split | observed | gated (Gate G1) | default % | gated default % |",
            "|---|--:|--:|--:|--:|",
            *[f"| {s} | {cohort[s][0]:,} | {cohort[s][1]:,} | {cohort[s][2]:.2f}% | "
              f"{cohort[s][3]:.2f}% |" for s in ("train", "val", "test")],
            f"| **total** | **{sum(cohort[s][0] for s in cohort):,}** | "
            f"**{sum(cohort[s][1] for s in cohort):,}** | | |",
            "", "## Results (test split)", "",
            "| Config | ROC-AUC | PR-AUC | pos% |", "|---|--:|--:|--:|",
        ]
        for name, n, pos, roc, pr in summ:
            lines.append(f"| {name} | {roc:.4f} | {pr:.4f} | {pos/max(n,1)*100:.2f}% |")
        lines += [
            "", "## Reading it",
            f"- **Gate G1 = config (4)**: ROC-AUC {summ[3][3]:.3f}, PR-AUC {summ[3][4]:.3f} "
            "-- the honest bar the foundation model must beat.",
            "- Removing the leakage columns (current-distress state, almost the answer) is the "
            "(1)->(3) drop.",
            "- The gate (predict *new* defaults among performing loans) is the realistic task.",
            "",
            f"**{len(cfg['leakage'])} leakage columns** (dropped in clean): "
            + ", ".join(f"`{c}`" for c in cfg["leakage"]),
            "",
            f"**{len(cfg['exclude'])} excluded** (ids / deal metadata / constants -- never features): "
            + ", ".join(f"`{c}`" for c in cfg["exclude"]),
            "", "## Caveat",
            f"- {cfg.get('caveat', 'Synthetic data is rule-based, so the clean baseline runs higher than a real portfolio.')}",
        ]
        lines += seg_lines
        lines += ["", f"Reproduce: `python scripts/train_baseline.py --config {args.config} "
                  f"--data-dir {args.data_dir} --book {args.book} --report {args.report}`", ""]
        rep.write_text("\n".join(lines))
        print(f"\nWrote {rep}")
    print(f"Total: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()