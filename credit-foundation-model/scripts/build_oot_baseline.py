# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Out-of-time (calendar-split) Gate-G1 baseline for Fannie Mae.

Unlike ``train_baseline.py`` (single observation cutoff, vintage split), this builds a
**multi-cutoff, out-of-time** baseline that uses the full history:

  * Observe every loan at **Dec 31 of each year** in the requested range. At each cutoff keep
    only *performing* loans (the gate), take features as-of that date, and label = whether the
    loan **defaults within the next ``--horizon-months``** (D180 or a Zero-Balance credit event).
  * One loan contributes one observation per year it is alive and performing → a pooled panel
    tagged with ``obs_year``.
  * **Split by calendar year**: train = ``--train-years``, test = ``--test-years`` (true
    out-of-time); val = a loan-disjoint 10% of the train years (for XGBoost early stopping).

Two leakage guards:
  * **loan-disjoint** (``--loan-split disjoint``): a loan is wholly in train OR test, never both.
  * **embargo**: train years whose forward label window reaches the test period are dropped, so
    no macro signal from the test era bleeds into training.

Reads the raw acquisition-cohort source files (``gs://<bucket>/parquet/<acqQ>.parquet``) — each
holds a loan's whole life, so cutoffs + forward label compute within one file. Columns are
renamed/cast via ``configs/fannie_mae/raw_schema.yaml``; the leakage/exclude lists come from
``configs/fannie_mae/baseline.yaml`` (single source of truth). Loan-level sampling (``--sample-pct``,
hash on loan id) keeps it tractable.

Examples
--------
Crisis stress test (train pre-crisis, test the crash):
    python scripts/build_oot_baseline.py --train-years 2000-2006 --test-years 2008-2010 \
        --sample-pct 20 --report reports/fannie_oot_crisis.md

Recent out-of-time:
    python scripts/build_oot_baseline.py --train-years 2000-2022 --test-years 2023-2025 \
        --sample-pct 20 --report reports/fannie_oot_recent.md
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import yaml
from sklearn.metrics import average_precision_score, roc_auc_score

from credit_fm.utils import storage

DEFAULT_KEY = "/workspace/.gcloud/credit-fm-sa.json"
PROJECT = "vertical-backup-493310-b4"
BUCKET = "sriram-credit-fm-data"
SRC_PREFIX = "parquet/"

_CAST = {"VARCHAR": "NULLIF({c}, '')::VARCHAR AS {n}",
         "INTEGER": "TRY_CAST(NULLIF({c}, '') AS INTEGER) AS {n}",
         "DOUBLE": "TRY_CAST(NULLIF({c}, '') AS DOUBLE) AS {n}",
         "BIGINT": "TRY_CAST(NULLIF({c}, '') AS BIGINT) AS {n}"}
_CFG: dict = {}


def _years(spec: str) -> list[int]:
    a, _, b = spec.partition("-")
    return list(range(int(a), int(b) + 1)) if b else [int(a)]


def build_obs_sql(file_uri: str) -> str:
    """SQL that turns one raw cohort file into performing Dec-cutoff observations (+ forward label)."""
    raw = yaml.safe_load(open(_CFG["raw_schema"]))["columns"]
    name_by_idx = {c["index"]: c for c in raw}
    clean = _CFG["clean_features"]
    # clean feature cast expressions (raw column00x -> snake_case), only the kept features
    feat_exprs = [_CAST[name_by_idx_type(name_by_idx, n)].format(c=f"column{idx_of(raw, n):03d}", n=n)
                  for n in clean]
    feat_list = ", ".join(clean)
    years = ", ".join(str(y) for y in _CFG["all_years"])
    h = _CFG["horizon_months"]
    pct = _CFG["sample_pct"]
    return f"""
    WITH base AS (
        SELECT
            column001 AS loan_id,
            strptime(column002, '%m%Y')::DATE AS rdate,
            TRY_CAST(NULLIF(column039, '') AS INTEGER) AS _dlq,
            NULLIF(column043, '') AS _zbc,
            {", ".join(feat_exprs)}
        FROM read_parquet('{file_uri}')
        WHERE column002 IS NOT NULL AND LENGTH(column002) = 6
          AND SUBSTR(column002, 1, 2) BETWEEN '01' AND '12'
          AND (hash(column001) % 100) < {pct}
    ),
    ev AS (
        SELECT *,
            (_dlq >= 6 OR _zbc IN ('02','03','09','15')) AS _default,
            (_dlq = 0 AND _zbc IS NULL) AS _perf
        FROM base
    ),
    obs AS (
        SELECT *, year(rdate) AS obs_year, row_number() OVER () AS _oid
        FROM ev
        WHERE month(rdate) = 12 AND _perf AND year(rdate) IN ({years})
    ),
    dflt AS (SELECT loan_id, rdate AS ddate FROM ev WHERE _default),
    lab AS (
        SELECT o._oid,
            COALESCE(MAX(CASE WHEN d.ddate > o.rdate
                              AND d.ddate <= o.rdate + INTERVAL {h} MONTH THEN 1 ELSE 0 END), 0) AS y
        FROM obs o LEFT JOIN dflt d ON d.loan_id = o.loan_id
        GROUP BY o._oid
    )
    SELECT o.loan_id, o.obs_year, lab.y, {feat_list}
    FROM obs o JOIN lab USING (_oid)
    """


def name_by_idx_type(name_by_idx: dict, n: str) -> str:
    for c in name_by_idx.values():
        if c["name"] == n:
            return c["type"]
    raise KeyError(n)


def idx_of(raw: list, n: str) -> int:
    for c in raw:
        if c["name"] == n:
            return c["index"]
    raise KeyError(n)


def _process_file(blob_name: str) -> str:
    """Download one cohort file, build its observations, write to staging. Returns the path."""
    import duckdb
    from google.cloud import storage as gcs
    stem = Path(blob_name).name.replace(".parquet", "")
    staging = Path(_CFG["staging"])
    local_src = staging / f"src_{stem}.parquet"
    out = staging / f"obs_{stem}.parquet"
    bucket = gcs.Client(project=PROJECT).bucket(BUCKET)
    bucket.blob(blob_name).download_to_filename(str(local_src))
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={_CFG['duckdb_threads']};")
    con.execute(f"PRAGMA memory_limit='{_CFG['duckdb_memory']}';")
    con.execute(f"COPY ({build_obs_sql(str(local_src))}) TO '{out}' (FORMAT PARQUET);")
    con.close()
    local_src.unlink(missing_ok=True)
    return str(out)


def _xgb(device="cuda"):
    return xgb.XGBClassifier(n_estimators=400, max_depth=6, learning_rate=0.05, subsample=0.8,
                             colsample_bytree=0.8, eval_metric="aucpr", n_jobs=-1,
                             tree_method="hist", device=device, early_stopping_rounds=30)


def encode(df, cats, nums, cmap=None):
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--train-years", required=True, help="e.g. 2000-2006")
    ap.add_argument("--test-years", required=True, help="e.g. 2008-2010")
    ap.add_argument("--sample-pct", type=int, default=20, help="loan %% kept (hash on loan id)")
    ap.add_argument("--horizon-months", type=int, default=12)
    ap.add_argument("--loan-split", choices=["disjoint", "overlap"], default="disjoint",
                    help="disjoint: a loan is wholly in train OR test (no leakage of identity)")
    ap.add_argument("--embargo-years", type=int, default=0,
                    help="extra gap (years) between train label windows and the test period")
    ap.add_argument("--device", default="cuda", help="xgboost device: cuda (GPU) or cpu")
    ap.add_argument("--neg-per-pos", type=int, default=0,
                    help="downsample TRAIN negatives to N x positives (0 = keep all); test untouched")
    ap.add_argument("--config", default="configs/fannie_mae/baseline.yaml")
    ap.add_argument("--raw-schema", default="configs/fannie_mae/raw_schema.yaml")
    ap.add_argument("--staging", default="/workspace/staging_oot")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--duckdb-threads", type=int, default=4)
    ap.add_argument("--duckdb-memory", default="48GB")
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--limit", type=int, default=None, help="first N cohort files (smoke test)")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    raw = yaml.safe_load(open(args.raw_schema))["columns"]
    drop = {"loan_identifier", "loan_id"} | set(cfg["exclude"]) | set(cfg["leakage"])
    clean_features = [c["name"] for c in raw if c["name"] not in drop]
    train_years, test_years = _years(args.train_years), _years(args.test_years)

    _CFG.update(raw_schema=args.raw_schema, clean_features=clean_features,
                all_years=sorted(set(train_years) | set(test_years)),
                horizon_months=args.horizon_months, sample_pct=args.sample_pct,
                staging=args.staging, duckdb_threads=args.duckdb_threads,
                duckdb_memory=args.duckdb_memory)
    st = Path(args.staging)
    shutil.rmtree(st, ignore_errors=True)
    st.mkdir(parents=True, exist_ok=True)
    storage.ensure_auth(f"gs://{BUCKET}", args.key)
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.key)

    from google.cloud import storage as gcs
    bucket = gcs.Client(project=PROJECT).bucket(BUCKET)
    sources = sorted(b.name for b in bucket.list_blobs(prefix=SRC_PREFIX)
                     if b.name.endswith(".parquet"))
    if args.limit:
        sources = sources[:args.limit]
    print(f"Cohort files: {len(sources)}  | sample={args.sample_pct}%  "
          f"train={args.train_years} test={args.test_years} horizon={args.horizon_months}mo")
    print(f"clean features: {len(clean_features)}")

    t0 = time.time()
    out_paths = []
    with ProcessPoolExecutor(max_workers=args.workers, initializer=_init, initargs=(_CFG,)) as ex:
        futs = {ex.submit(_process_file, s): s for s in sources}
        for i, f in enumerate(as_completed(futs), 1):
            out_paths.append(f.result())
            print(f"  [{i}/{len(sources)}] {Path(futs[f]).name} done", flush=True)
    print(f"observation build: {time.time()-t0:.0f}s")

    obs = pd.concat([pd.read_parquet(p) for p in out_paths], ignore_index=True)

    # --- embargo: drop train years whose forward label window reaches the test period ---
    horizon_years = math.ceil(args.horizon_months / 12)
    test_start = min(test_years)
    eff_train_years = [y for y in train_years
                       if y + horizon_years + args.embargo_years < test_start]
    dropped = sorted(set(train_years) - set(eff_train_years))
    if dropped:
        print(f"embargo: dropped train years {dropped} "
              f"(label window within {horizon_years + args.embargo_years}y of test start {test_start})")
    if not eff_train_years:
        raise SystemExit("No train years survive the embargo — widen the train/test gap.")
    obs = obs[obs.obs_year.isin(eff_train_years + test_years)].copy()

    # --- loan assignment to train vs test ---
    obs["_tr"] = obs.obs_year.isin(eff_train_years)
    obs["_te"] = obs.obs_year.isin(test_years)
    if args.loan_split == "disjoint":
        flags = obs.groupby("loan_id")[["_tr", "_te"]].max()          # any train / any test obs
        overlap = flags["_tr"] & flags["_te"]
        to_test = pd.util.hash_pandas_object(flags.index.to_series(), index=False) % 2 == 0
        train_loans = flags.index[(flags["_tr"] & ~overlap) | (overlap & ~to_test)]
        test_loans = flags.index[(flags["_te"] & ~overlap) | (overlap & to_test)]
        print(f"loan-disjoint: {int(overlap.sum()):,} loans span both periods (split by hash); "
              f"train loans {len(train_loans):,}, test loans {len(test_loans):,}")
        train_obs = obs[obs.loan_id.isin(train_loans) & obs["_tr"]]
        test_obs = obs[obs.loan_id.isin(test_loans) & obs["_te"]]
    else:
        train_obs, test_obs = obs[obs["_tr"]], obs[obs["_te"]]

    # loan-disjoint 10% of train loans -> val (early stopping)
    is_val = pd.util.hash_pandas_object(train_obs.loan_id, index=False) % 10 == 0
    part = {"train": train_obs[~is_val], "val": train_obs[is_val], "test": test_obs}

    # optional: downsample TRAIN negatives for rare-event speed/memory (test stays full -> honest)
    if args.neg_per_pos > 0:
        tr = part["train"]
        pos, neg = tr[tr.y == 1], tr[tr.y == 0]
        keep = min(len(neg), len(pos) * args.neg_per_pos)
        part["train"] = pd.concat([pos, neg.sample(n=keep, random_state=42)])
        print(f"neg-downsample: train {len(tr):,} -> {len(part['train']):,} "
              f"({len(pos):,} pos + {keep:,} neg)")

    cats = [c for c in clean_features if obs[c].dtype == object]
    nums = [c for c in clean_features if obs[c].dtype != object]
    Xtr, cm = encode(part["train"], cats, nums)
    Xva, _ = encode(part["val"], cats, nums, cm)
    Xte, _ = encode(part["test"], cats, nums, cm)
    print(f"training xgboost on {len(Xtr):,} rows x {Xtr.shape[1]} feats (device={args.device}) ...")
    m = _xgb(args.device)
    m.fit(Xtr, part["train"].y, eval_set=[(Xva, part["val"].y)], verbose=False)

    rows = []
    for s in ("train", "val", "test"):
        y = part[s].y.to_numpy()
        X = encode(part[s], cats, nums, cm)[0]
        p = m.predict_proba(X)[:, 1]
        rows.append((s, len(y), int(y.sum()), y.mean(),
                     roc_auc_score(y, p) if y.sum() > 5 else np.nan,
                     average_precision_score(y, p) if y.sum() > 5 else np.nan))

    print("\n=== OOT Gate-G1 (performing obs, no-leakage features) ===")
    print(f"  {'split':<6}{'n':>11}{'pos':>9}{'rate':>8}{'ROC':>8}{'PR':>8}")
    for s, n, pos, rate, roc, pr in rows:
        print(f"  {s:<6}{n:>11,}{pos:>9,}{rate*100:>7.2f}%{roc:>8.4f}{pr:>8.4f}")

    if args.report:
        rep = Path(args.report)
        rep.parent.mkdir(parents=True, exist_ok=True)
        te = rows[-1]
        per_year = (part["test"].groupby("obs_year").y.agg(["size", "mean"]))
        lines = [
            "# Fannie Mae — Out-of-Time Gate-G1 Baseline", "",
            f"Calendar split — **train {eff_train_years[0]}–{eff_train_years[-1]}**, "
            f"**test {args.test_years}** (val = 10% of train loans). {args.sample_pct}% loan sample; "
            f"{args.horizon_months}-month default horizon; {len(clean_features)} no-leakage features. "
            f"Loan split: **{args.loan_split}**"
            + (f"; embargo dropped train years {dropped}." if dropped else "; clean train/test gap."),
            "", "Each loan observed every Dec it is performing; label = default (D180 / Zero-Balance "
            "credit event) within the horizon. True out-of-time: the test years are never trained on.",
            "", "## Population", "",
            "| Split | observations | defaults | default rate |", "|---|--:|--:|--:|",
            *[f"| {s} | {n:,} | {pos:,} | {rate*100:.2f}% |" for s, n, pos, rate, _, _ in rows],
            "", "## Result (out-of-time test)", "",
            "| Metric | value |", "|---|--:|",
            f"| ROC-AUC | {te[4]:.4f} |", f"| PR-AUC | {te[5]:.4f} |",
            f"| test default rate | {te[3]*100:.2f}% |",
            "", "## Test default rate by year", "",
            "| obs_year | observations | default rate |", "|---|--:|--:|",
            *[f"| {y} | {int(r['size']):,} | {r['mean']*100:.2f}% |" for y, r in per_year.iterrows()],
            "", "## Notes",
            f"- {cfg.get('caveat', 'Real-world data.')}",
            "- Out-of-time by calendar year is the honest generalization test (train on the past, "
            "score the future). The foundation model must beat this ROC/PR-AUC.",
            f"- **Guards:** loan-disjoint = `{args.loan_split}` (no loan appears in both train and "
            "test); embargo = train label windows end before the test period (no macro bleed).",
        ]
        rep.write_text("\n".join(lines))
        print(f"\nWrote {rep}")


def _init(cfg: dict) -> None:
    _CFG.update(cfg)


if __name__ == "__main__":
    main()
