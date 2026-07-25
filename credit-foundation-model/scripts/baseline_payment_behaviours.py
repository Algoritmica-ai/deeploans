# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Honest rolling-statistics baseline for the payment-behaviours late-payment task.

The FM fine-tune answers "will a currently-current customer exceed 30 dpd within the next 3
invoices?" from the raw days-past-due sequence. The obvious question a jury asks: *does the
foundation model beat a trivial model that just looks at how late you have been recently?*
This script IS that trivial model — the bar the FM must clear to justify itself.

It is a **fair** bar by construction: it reuses the framework's own
:func:`~credit_fm.inference.scoring.observe_panel` and
:func:`~credit_fm.data.labels.forward_event_entities`, and replicates ``finetune.py``'s
deterministic loan-disjoint hash split, so the observations, labels, and **test set are
identical** to the fine-tune's. Only the model differs: instead of the encoder + head, each
observation becomes a handful of hand-rolled rolling-DPD features fed to XGBoost.

    python scripts/baseline_payment_behaviours.py -c configs/payment_behaviours/finetune_late30.yaml
    python scripts/baseline_payment_behaviours.py -c configs/payment_behaviours/finetune_late30.yaml \
        --fm-roc 0.6871 --fm-ap 0.0594     # print the head-to-head against the FM result
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score

from credit_fm.data.dataset_config import load_dataset_config
from credit_fm.data.labels import forward_event_entities, resolve_label_spec
from credit_fm.inference.scoring import observe_panel
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli

VAL_COL = "dpd"
SEQ_COL = "seq_index"


def rolling_features(obs: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """One feature row per loan from its truncated (<= cutoff) dpd history.

    All features are vectorized groupby aggregations — no per-loan Python loop — so this scales
    to the ~0.5M loans observed at each cutoff.
    """
    g = obs.sort_values([id_col, SEQ_COL]).copy()
    g["is_late"] = (g[VAL_COL] > 0).astype("int8")
    g["gt30"] = (g[VAL_COL] > 30).astype("int8")
    g["gt90"] = (g[VAL_COL] > 90).astype("int8")
    g["rn"] = g.groupby(id_col, sort=False).cumcount()

    grp = g.groupby(id_col, sort=False)
    feat = grp.agg(
        n=(VAL_COL, "size"),
        dpd_mean=(VAL_COL, "mean"),
        dpd_max=(VAL_COL, "max"),
        dpd_std=(VAL_COL, "std"),
        dpd_last=(VAL_COL, "last"),
        frac_late=("is_late", "mean"),
        cnt_gt30=("gt30", "sum"),
        cnt_gt90=("gt90", "sum"),
    )
    feat["dpd_std"] = feat["dpd_std"].fillna(0.0)

    # recent window (last 3 invoices)
    last3 = g.groupby(id_col, sort=False).tail(3).groupby(id_col, sort=False)[VAL_COL]
    feat["dpd_mean3"] = last3.mean()
    feat["dpd_max3"] = last3.max()

    # trailing on-time streak: invoices since the last late one (= n if never late)
    last_late = g.loc[g["is_late"] == 1].groupby(id_col, sort=False)["rn"].max()
    feat = feat.join(last_late.rename("last_late_rn"))
    feat["ontime_streak"] = np.where(
        feat["last_late_rn"].isna(), feat["n"], feat["n"] - 1 - feat["last_late_rn"])

    # simple trend: most-recent dpd vs the mean of everything before it
    feat["trend"] = feat["dpd_last"] - (feat["dpd_mean"] * feat["n"] - feat["dpd_last"]) / \
        (feat["n"] - 1).clip(lower=1)
    feat = feat.drop(columns=["last_late_rn"])
    return feat.reset_index()


FEATURES = ["n", "dpd_mean", "dpd_max", "dpd_std", "dpd_last", "frac_late", "cnt_gt30",
            "cnt_gt90", "dpd_mean3", "dpd_max3", "ontime_streak", "trend"]


def era_frame(panel, spec, id_col, time_col, cutoffs, era, log) -> pd.DataFrame:
    """Stack per-cutoff observations for one era into a feature frame with labels + loan ids."""
    parts = []
    for co in cutoffs:
        pos = forward_event_entities(panel, spec, id_col=id_col, time_col=time_col, cutoff=co)
        obs = observe_panel(panel, id_col, time_col, co, spec.gate_col, spec.gate_values)
        feat = rolling_features(obs, id_col)
        feat["y"] = feat[id_col].isin(pos).astype("int8")
        feat["cutoff"] = str(pd.to_datetime(co).date())     # for joining FM scores (stratify)
        parts.append(feat)
        log(f"  {era} cutoff {co}: {len(feat):,} obs, {feat['y'].mean()*100:.2f}% positive")
    return pd.concat(parts, ignore_index=True)


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
    print(f"task: {spec.name} — {spec.event_col} within {spec.horizon_months}mo, "
          f"gate {spec.gate_col}", flush=True)

    tr = era_frame(panel, spec, id_col, time_col, train_cutoffs, "train", print)
    te = era_frame(panel, spec, id_col, time_col, test_cutoffs, "test", print)

    # loan-disjoint hash split — IDENTICAL to finetune.py (deterministic hash, test set matches)
    overlap = set(pd.unique(tr[id_col])) & set(pd.unique(te[id_col]))
    if overlap:
        ov = pd.Series(sorted(overlap))
        to_test = set(ov[pd.util.hash_pandas_object(ov, index=False).to_numpy() % 2 == 0])
        to_train = overlap - to_test
        tr = tr[~tr[id_col].isin(to_test)]
        te = te[~te[id_col].isin(to_train)]
        print(f"  loan-disjoint: {len(overlap):,} loans span both eras (hash-split)", flush=True)
    print(f"obs train {len(tr):,} ({tr['y'].mean()*100:.2f}%) | "
          f"test {len(te):,} ({te['y'].mean()*100:.2f}%)", flush=True)

    x_tr, y_tr = tr[FEATURES].to_numpy(np.float32), tr["y"].to_numpy()
    x_te, y_te = te[FEATURES].to_numpy(np.float32), te["y"].to_numpy()
    n_pos = max(int(y_tr.sum()), 1)
    spw = (len(y_tr) - n_pos) / n_pos
    clf = xgb.XGBClassifier(
        n_estimators=400, max_depth=5, learning_rate=0.05, subsample=0.8,
        colsample_bytree=0.8, min_child_weight=5, scale_pos_weight=spw,
        eval_metric="aucpr", n_jobs=0, tree_method="hist")
    t0 = time.time()
    clf.fit(x_tr, y_tr)
    prob = clf.predict_proba(x_te)[:, 1]
    roc, ap = roc_auc_score(y_te, prob), average_precision_score(y_te, prob)
    print(f"  trained XGBoost on {len(y_tr):,} obs ({n_pos:,} pos, "
          f"scale_pos_weight={spw:.1f}) in {time.time()-t0:.0f}s", flush=True)

    imp = sorted(zip(FEATURES, clf.feature_importances_), key=lambda t: -t[1])
    when = f"{train_cutoffs[0]}..{train_cutoffs[-1]} -> {', '.join(test_cutoffs)}"
    lines = [
        f"# Payment-behaviours rolling-stats baseline — {spec.name}",
        "",
        "Honest XGBoost bar on identical observations/labels/split as the FM fine-tune.",
        f"Observation windows: {when}", "",
        f"- **test: {len(y_te):,} loans, {y_te.mean()*100:.2f}% positive**",
        f"- **ROC-AUC {roc:.4f} | PR-AUC {ap:.4f}**",
        f"- lift over base rate: {ap / max(y_te.mean(), 1e-9):.2f}x", "",
        "Top features by gain:",
        *[f"  {i+1}. {f} ({w:.3f})" for i, (f, w) in enumerate(imp[:8])],
    ]
    fm_roc, fm_ap = cfg.get_path("fm_roc"), cfg.get_path("fm_ap")
    if fm_roc is not None:
        verdict = "FM ahead" if float(fm_roc) > roc else "baseline matches/leads FM"
        lines += ["", "## Head-to-head vs foundation model",
                  f"- FM:       ROC {float(fm_roc):.4f} | PR-AUC {float(fm_ap):.4f}",
                  f"- baseline: ROC {roc:.4f} | PR-AUC {ap:.4f}",
                  f"- **verdict: {verdict}** (ROC Δ {float(fm_roc)-roc:+.4f}, "
                  f"AP Δ {float(fm_ap)-ap:+.4f})"]
    report = cfg.get_path("report_baseline", "reports/pb_late30_baseline.md")
    storage.write_text("\n".join(lines) + "\n", report)

    print(f"\n=== Rolling-stats baseline — {spec.name} ({when}) ===")
    print(f"  test: {len(y_te):,} loans, {y_te.mean()*100:.2f}% positive")
    print(f"  ROC-AUC {roc:.4f} | PR-AUC {ap:.4f}")
    if fm_roc is not None:
        print(f"  vs FM: ROC {float(fm_roc):.4f} / AP {float(fm_ap):.4f}  ->  "
              f"{'FM ahead' if float(fm_roc) > roc else 'baseline matches/leads FM'}")
    print(f"wrote {report}")


if __name__ == "__main__":
    main()
