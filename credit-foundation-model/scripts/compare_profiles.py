# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Compare the delinquency distribution of two dataset profiles — e.g. the 4% panel vs the 100% book.

Confirms the deterministic 4% loan-hash sample is a faithful stand-in for the whole loan book: it
lines up the ``delinquency_by_reporting_year`` (and vintage) tables from two
``profile_mortgage_dataset.py`` JSON artifacts, reports the per-year gap in percentage points and in
relative terms, and prints a representativeness verdict.

    # A = the 4% panel, B = the whole raw book (delinquency-only is enough and fast)
    python scripts/profile_mortgage_dataset.py --panel gs://.../panel_2000_2024.parquet \
        --out reports/profile_4pct.json --delinquency-only --no-loan-count
    python scripts/profile_mortgage_dataset.py --raw-root gs://.../raw_by_reporting \
        --out reports/profile_100pct.json --delinquency-only --no-vintage --no-loan-count
    python scripts/compare_profiles.py --a reports/profile_4pct.json --b reports/profile_100pct.json \
        --label-a "4% sample" --label-b "100% book" --out reports/delinquency_4pct_vs_100pct.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

_METRICS = ["dpd30_plus_pct", "d180_plus_pct", "default_event_pct", "performing_pct"]


def _rel(a: float, b: float) -> float | None:
    """Relative gap of a vs b (the reference), in %."""
    return round(100 * (a - b) / b, 3) if b else None


def _year_table(a: dict, b: dict, la: str, lb: str) -> pd.DataFrame:
    da = pd.DataFrame(a["delinquency_by_reporting_year"]).set_index("year")
    db = pd.DataFrame(b["delinquency_by_reporting_year"]).set_index("year")
    rows = []
    for y in sorted(set(da.index) | set(db.index)):
        row = {"year": y}
        for m in _METRICS:
            va = float(da.loc[y, m]) if y in da.index else None
            vb = float(db.loc[y, m]) if y in db.index else None
            row[f"{m}__{la}"] = va
            row[f"{m}__{lb}"] = vb
            row[f"{m}__diff_pp"] = round(va - vb, 4) if (va is not None and vb is not None) else None
            row[f"{m}__diff_rel%"] = _rel(va, vb) if (va is not None and vb is not None) else None
        rows.append(row)
    return pd.DataFrame(rows).set_index("year")


def _pooled(profile: dict) -> dict:
    """Book-wide pooled rates (weight every loan-month equally, not the per-year average)."""
    df = pd.DataFrame(profile["delinquency_by_reporting_year"])
    known = df["known_status"].sum() or 1
    rows = df["loan_months"].sum() or 1
    return {
        "loan_months": int(df["loan_months"].sum()),
        "dpd30_plus_pct": round(100 * df["dpd30_plus"].sum() / known, 4),
        "d180_plus_pct": round(100 * df["d180_plus"].sum() / known, 4),
        "default_event_pct": round(100 * df["default_event"].sum() / rows, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", required=True, help="profile JSON A (e.g. the 4% sample)")
    ap.add_argument("--b", required=True, help="profile JSON B (e.g. the 100% book) — the reference")
    ap.add_argument("--label-a", default="A")
    ap.add_argument("--label-b", default="B")
    ap.add_argument("--out", default="reports/delinquency_comparison.json")
    ap.add_argument("--tol-rel", type=float, default=5.0,
                    help="pass if the POOLED relative gap in default_event_pct is <= this %% "
                         "(pooled = loan-month-weighted; robust to thin-year sampling noise)")
    args = ap.parse_args()

    a = json.loads(Path(args.a).read_text())
    b = json.loads(Path(args.b).read_text())
    la, lb = args.label_a, args.label_b

    yt = _year_table(a, b, la, lb)
    pa, pb = _pooled(a), _pooled(b)

    # representativeness: the pooled (loan-month-weighted) default rate is the robust headline —
    # unweighted per-year gaps are dominated by thin years with few sampled loan-months.
    pooled_rel = _rel(pa["default_event_pct"], pb["default_event_pct"])
    dcol = yt["default_event_pct__diff_rel%"].dropna().abs()
    ppcol = yt["default_event_pct__diff_pp"].dropna().abs()
    mean_rel = round(float(dcol.mean()), 3) if len(dcol) else None
    max_rel = round(float(dcol.max()), 3) if len(dcol) else None
    max_pp = round(float(ppcol.max()), 4) if len(ppcol) else None
    verdict = ("REPRESENTATIVE" if (pooled_rel is not None and abs(pooled_rel) <= args.tol_rel)
               else "REVIEW")

    scale = (b["n_rows"] / a["n_rows"]) if a["n_rows"] else None
    summary = {
        "label_a": la, "label_b": lb,
        "a": {"source": a["source"], "n_rows": a["n_rows"], "n_loans": a["n_loans"], "pooled": pa},
        "b": {"source": b["source"], "n_rows": b["n_rows"], "n_loans": b["n_loans"], "pooled": pb},
        "b_to_a_row_scale": round(scale, 2) if scale else None,
        "default_event_pct": {
            "pooled_a": pa["default_event_pct"], "pooled_b": pb["default_event_pct"],
            "pooled_diff_pp": round(pa["default_event_pct"] - pb["default_event_pct"], 4),
            "pooled_diff_rel_%": pooled_rel,
            "yearly_mean_abs_rel_%": mean_rel, "yearly_max_abs_rel_%": max_rel,
            "yearly_max_abs_pp": max_pp,
        },
        "verdict": verdict, "tol_rel_%": args.tol_rel,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"summary": summary, "by_year": yt.reset_index().to_dict("records")},
                              indent=2, default=str))

    # console report
    print(f"\n{la}: {a['n_rows']:,} rows / {a['n_loans']} loans   "
          f"{lb}: {b['n_rows']:,} rows / {b['n_loans']} loans   (x{summary['b_to_a_row_scale']})\n")
    show = yt[[f"default_event_pct__{la}", f"default_event_pct__{lb}",
               "default_event_pct__diff_pp", "default_event_pct__diff_rel%"]]
    show.columns = [f"deft% {la}", f"deft% {lb}", "Δ pp", "Δ rel%"]
    with pd.option_context("display.max_rows", None):
        print(show)
    print(f"\npooled default rate — {la}: {pa['default_event_pct']}%   "
          f"{lb}: {pb['default_event_pct']}%   "
          f"(Δ {summary['default_event_pct']['pooled_diff_pp']} pp, {pooled_rel}% rel)")
    print(f"per-year gap (context) — mean |rel| {mean_rel}%  max |rel| {max_rel}%  max |pp| {max_pp} "
          f"(thin years are noisy)")
    print(f"\nVERDICT: {verdict}  (threshold: pooled |rel| <= {args.tol_rel}%)")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
