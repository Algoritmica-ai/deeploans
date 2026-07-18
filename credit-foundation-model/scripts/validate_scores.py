# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Validate a scored-portfolio artifact against the scoring contract — read-only audit.

Two layers, mirroring the pipeline convention:

* **Structural** (always) — the *produced scores* file (from ``scripts/score_portfolio.py``) is
  well-formed:
    A) schema — has the id / score / n_events / cutoff columns;
    B) score range — every score in [0, 1], no NaN;
    C) one row per loan — no duplicate ids;
    D) each scored loan has history — n_events >= 1;
    E) single cutoff — one cutoff value, matching the manifest;
    F) manifest agreement — row count and score summary match ``<scores>_manifest.json``.
* **Quality** (``--labeled-panel``) — first *reconcile the population* (scored loans exist in the
  panel, dup/coverage counts — a plausible ROC on the wrong snapshot is a trap), then score against
  ground truth: join the forward ``default_event`` label (default within ``--horizon`` months of the
  cutoff) and report ROC-AUC / PR-AUC **plus a recall@K / lift table** ("review the top K% riskiest,
  catch what share of defaults?" — the operational metric). Check G = scored ⊆ panel; H (with
  ``--min-roc``) gates the ROC. Only meaningful for a **past** cutoff whose outcomes exist.
* **Calibration** (check I, v1.1 G6.1) — when the file carries a calibrated ``pd`` column
  (``score_portfolio --calibrator``): structurally, ``pd`` must be in [0,1]; with a labeled
  panel, the **Brier score** and reliability table are reported and calibration-in-the-large is
  gated — mean predicted PD within ``--cal-factor``× (default 2) of the realized rate. Raw
  rebalanced-model scores fail this by an order of magnitude; that is the point.

    python scripts/validate_scores.py --scores gs://.../portfolio_scores.parquet
    # + quality: reproduce the model's ~0.82 on a labeled past cutoff
    python scripts/validate_scores.py --scores gs://.../scores_2022.parquet \
        --labeled-panel gs://.../panel_2000_2024.parquet --horizon 12 --min-roc 0.7
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _read_parquet(path: str, columns=None) -> pd.DataFrame:
    if path.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(path[len("gs://"):]) as f:
            return pd.read_parquet(f, columns=columns)
    return pd.read_parquet(path, columns=columns)


def _read_text(path: str) -> str:
    if path.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(path[len("gs://"):], "r") as f:
            return f.read()
    return Path(path).read_text()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scores", required=True, help="scored parquet (local or gs://)")
    ap.add_argument("--id-col", default="loan_id")
    ap.add_argument("--key", default="/workspace/.gcloud/credit-fm-sa.json")
    ap.add_argument("--labeled-panel", help="panel with the forward label — enables quality eval "
                    "(ROC/AP vs ground truth). Use only for a PAST cutoff whose outcomes exist.")
    ap.add_argument("--horizon", type=int, default=12, help="forward-label window (months)")
    ap.add_argument("--label-col", default="default_event")
    ap.add_argument("--time-col", default="reporting_date")
    ap.add_argument("--min-roc", type=float, help="if set, PASS/FAIL on ROC-AUC >= this")
    ap.add_argument("--top-k", type=float, nargs="+", default=[0.01, 0.05, 0.10, 0.20],
                    help="review-budget fractions for the recall@K / lift table (e.g. 0.01 0.05)")
    ap.add_argument("--cal-factor", type=float, default=2.0,
                    help="check I tolerance: mean pd within this factor of the realized rate")
    args = ap.parse_args()
    if args.scores.startswith("gs://") and Path(args.key).exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.key)

    df = _read_parquet(args.scores)
    manifest_path = args.scores.rsplit(".", 1)[0] + "_manifest.json"
    try:
        manifest = json.loads(_read_text(manifest_path))
    except (FileNotFoundError, OSError):
        manifest = None

    results: list[tuple[bool, str, str]] = []

    def chk(name, cond, detail=""):
        results.append((bool(cond), name, detail))

    idc = args.id_col
    # A) schema
    need = {idc, "score", "n_events", "cutoff"}
    have = set(df.columns)
    chk("A: required columns present", need <= have,
        f"missing {sorted(need - have) or 'none'}; {len(df):,} rows")

    if need <= have and len(df):
        # B) score range
        s = pd.to_numeric(df["score"], errors="coerce")
        chk("B: every score in [0,1], no NaN",
            bool(s.notna().all() and (s >= 0).all() and (s <= 1).all()),
            f"range {s.min():.4f}..{s.max():.4f}, {int(s.isna().sum())} NaN")
        # C) one row per loan
        dups = int(df[idc].duplicated().sum())
        chk("C: no duplicate loan ids", dups == 0, f"{dups} duplicates")
        # D) each scored loan has history
        ne = pd.to_numeric(df["n_events"], errors="coerce")
        chk("D: every scored loan has n_events >= 1", bool((ne >= 1).all()),
            f"min n_events = {int(ne.min())}")
        # E) single cutoff
        cuts = df["cutoff"].astype(str).unique()
        chk("E: single cutoff value", len(cuts) == 1, f"cutoffs = {list(cuts)[:3]}")
        # I-structural) calibrated pd column, when present, must be a probability
        if "pd" in df.columns:
            p = pd.to_numeric(df["pd"], errors="coerce")
            chk("I: calibrated pd in [0,1], no NaN",
                bool(p.notna().all() and (p >= 0).all() and (p <= 1).all()),
                f"pd range {p.min():.5f}..{p.max():.5f}, mean {p.mean():.5f}")

    # F) manifest agreement
    if manifest is not None:
        agree = manifest.get("n_scored") == len(df)
        if need <= have and len(df):
            agree = agree and str(manifest.get("cutoff", ""))[:10] in {str(c)[:10] for c in df["cutoff"].astype(str).unique()}
        chk("F: manifest n_scored + cutoff match the file", agree,
            f"manifest n_scored={manifest.get('n_scored')} vs {len(df)}")
    else:
        chk("F: manifest present", False, f"no manifest at {manifest_path}")

    # Quality (optional) — reconcile the scored population against the labeled panel, then score the
    # scores against the forward default label. (A plausible ROC on the WRONG population is a trap:
    # if the scored file is a different snapshot, or dropped/duplicated loans, the metric can still
    # look fine — so verify the population first, then the metric.)
    if args.labeled_panel and need <= have and len(df) and len(df["cutoff"].astype(str).unique()) == 1:
        from sklearn.metrics import average_precision_score, roc_auc_score
        cutoff = pd.to_datetime(df["cutoff"].astype(str).iloc[0])
        hi = cutoff + pd.DateOffset(months=args.horizon)
        panel = _read_parquet(args.labeled_panel, columns=[idc, args.time_col, args.label_col])
        dt = pd.to_datetime(panel[args.time_col], errors="coerce")

        scored_ids = set(df[idc].astype(str))
        panel_ids = set(panel[idc].astype(str))
        window = panel[(dt > cutoff) & (dt <= hi) & panel[args.label_col].fillna(False).astype(bool)]
        defaulted = set(window[idc].astype(str))               # loans that default in the window
        matched = defaulted & scored_ids                       # ...that are actually in the scored set
        y = df[idc].astype(str).isin(defaulted).astype(int).to_numpy()

        # population reconciliation — the reviewer's point
        print(f"\n  population : scored={len(df):,}  unique={df[idc].nunique():,}  "
              f"dup={int(df[idc].duplicated().sum())}  in-panel={len(scored_ids & panel_ids):,}/"
              f"{len(scored_ids):,}")
        print(f"  labels     : window_defaults={len(defaulted):,}  matched_in_scored={len(matched):,} "
              f"({len(matched)/max(len(defaulted),1)*100:.1f}% — rest were gated out as non-performing "
              f"at the cutoff, expected)  positive_rate={y.mean()*100:.3f}%")
        # G) every scored loan must exist in the labeled panel (else it's the wrong snapshot/panel)
        chk("G: scored loans all exist in the labeled panel", scored_ids <= panel_ids,
            f"{len(scored_ids - panel_ids):,} scored ids absent from the panel")

        both = 0 < y.sum() < len(y)
        if both:
            sc = df["score"].to_numpy()
            roc = roc_auc_score(y, sc)
            ap = average_precision_score(y, sc)
            print(f"  forward-label eval (default within {args.horizon}mo of {cutoff.date()}): "
                  f"n={len(y):,}  ROC={roc:.4f}  AP={ap:.4f}")
            # recall@K / lift — "review the top K% riskiest, catch what share of defaults?"
            # (the operational read: precision is low at a rare base rate; lift is the value.)
            P, base = int(y.sum()), y.mean()
            caught_cum = y[np.argsort(-sc)].cumsum()               # defaults caught, ranked by score
            print(f"  recall @ top-K (rank by score; {P} defaults, base rate {base*100:.3f}%):")
            for k in args.top_k:
                n = max(int(len(y) * k), 1)
                caught = int(caught_cum[n - 1])
                print(f"    top {k*100:>4.1f}% ({n:>8,} loans): caught {caught:>4}/{P} = "
                      f"{caught/P*100:4.1f}% recall | precision {caught/n*100:5.2f}% | "
                      f"lift {(caught/n)/base:4.1f}x")
            if args.min_roc is not None:
                chk(f"H: ROC-AUC >= {args.min_roc}", roc >= args.min_roc, f"ROC={roc:.4f}")
            # I-quality) calibration: Brier + reliability + calibration-in-the-large (G6.1)
            if "pd" in df.columns:
                from credit_fm.inference.calibration import brier, reliability_table
                p = df["pd"].to_numpy()
                print(f"  calibration: Brier={brier(y, p):.6f}  (raw-score Brier={brier(y, sc):.6f})"
                      f"  mean_pd={p.mean():.5f}  realized={y.mean():.5f}")
                for row in reliability_table(y, p):
                    print(f"    {row['bin']}  n={row['n']:>8,}  mean_pd={row['mean_pd']:.4f}  "
                          f"realized={row['realized']:.4f}")
                f = args.cal_factor
                ratio = p.mean() / max(y.mean(), 1e-12)
                chk(f"I: calibration-in-the-large (mean pd within {f}x of realized rate)",
                    (1 / f) <= ratio <= f, f"mean_pd/realized = {ratio:.2f}")
        else:
            chk("H: forward-label eval has both classes", both,
                f"{int(y.sum())} defaults in {len(y):,} — need a labeled PAST cutoff")

    ok = all(r[0] for r in results)
    for passed, name, detail in results:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
