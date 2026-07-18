# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Profile the whole Fannie Mae dataset — per-column statistics + delinquency rate by year.

Streams a parquet dataset (a single file, a directory, or the Hive-partitioned raw source) in
memory-bounded batches and writes a JSON profile consumed by ``notebooks/06_fannie_data_bible.ipynb``.

It computes, in one pass:
  * **per-column statistics** — dtype, non-null count, null %, distinct count (exact up to a cap),
    numeric summary (min/mean/std/quantiles) or top categorical values;
  * **delinquency rate by reporting (calendar) year** — of all loan-months observed in year Y, the
    share 30+ days past due, 180+ days past due (D180), in a default_event, and performing;
  * **vintage default rate by origination year** — of all loans originated in year Y, the share
    that EVER hit a default_event (loan-level; skip with ``--no-vintage`` on the full raw book).

Works on either input:
  * the ingested panel (has the derived label columns already) — the default, fast, representative;
  * the raw Hive source (``--raw-root``) — derives labels per batch via the ingest logic.

    # profile the ingested 4% panel (representative of the whole book)
    python scripts/profile_fannie_dataset.py \
        --panel gs://sriram-credit-fm-data/output/raw/fannie_mae/panel_2000_2024.parquet \
        --out reports/fannie_dataset_profile.json

    # profile the TRUE whole loan book (all loans) straight from the raw source
    python scripts/profile_fannie_dataset.py \
        --raw-root gs://sriram-credit-fm-data/fannie_by_reporting \
        --out reports/fannie_dataset_profile_full.json --no-vintage
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


def _load_ingest():
    """Import the ingest module lazily (only the raw-source path needs its _derive)."""
    spec = importlib.util.spec_from_file_location(
        "fannie_adapter", Path(__file__).resolve().parent.parent
        / "reference_implementations" / "fannie_mae" / "adapter.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_UNIQUE_CAP = 200_000      # stop exact distinct-counting a column past this many keys
_TOPK_CAP = 4_000          # stop tracking new categorical keys past this many (top-k stays valid)
_RESERVOIR = 200_000       # per-numeric-column reservoir for quantiles
_DERIVED = ("reporting_date", "dlq_num", "default_event", "prepay_event", "is_performing")


class _Col:
    """Streaming accumulator for one column."""

    def __init__(self, name: str, is_numeric: bool):
        self.name = name
        self.is_numeric = is_numeric
        self.n = 0            # non-null count
        self.nulls = 0
        # numeric
        self.vmin = None
        self.vmax = None
        self.vsum = 0.0
        self.vsumsq = 0.0
        self._res = np.empty(_RESERVOIR, dtype="float64")
        self._res_n = 0       # values currently in reservoir
        self._seen = 0        # numeric values seen (for reservoir replacement)
        self._rng = np.random.default_rng(0)
        # categorical / distinct
        self.counts: dict[str, int] = {}
        self.uniques: set = set()
        self.unique_overflow = False
        self.topk_full = False
        self.smin = None      # lexicographic min/max (dates, strings)
        self.smax = None

    def update(self, s: pd.Series) -> None:
        nn = int(s.isna().sum())
        self.nulls += nn
        v = s.dropna()
        self.n += len(v)
        if len(v) == 0:
            return
        if not self.unique_overflow:                     # capped exact distinct count, all kinds
            self.uniques.update(pd.unique(v).tolist())
            if len(self.uniques) > _UNIQUE_CAP:
                self.unique_overflow = True
                self.uniques = set()
        if self.is_numeric:
            x = pd.to_numeric(v, errors="coerce").to_numpy(dtype="float64")
            x = x[~np.isnan(x)]
            if x.size:
                self.vmin = x.min() if self.vmin is None else min(self.vmin, x.min())
                self.vmax = x.max() if self.vmax is None else max(self.vmax, x.max())
                self.vsum += float(x.sum())
                self.vsumsq += float(np.square(x).sum())
                self._reservoir(x)
        else:
            sv = v.astype(str)
            lo, hi = sv.min(), sv.max()
            self.smin = lo if self.smin is None else min(self.smin, lo)
            self.smax = hi if self.smax is None else max(self.smax, hi)
            vc = sv.value_counts()
            for key, c in vc.items():
                if key in self.counts:
                    self.counts[key] += int(c)
                elif not self.topk_full:
                    self.counts[key] = int(c)
                    if len(self.counts) >= _TOPK_CAP:
                        self.topk_full = True

    def _reservoir(self, x: np.ndarray) -> None:
        for val in x:                       # Algorithm R
            self._seen += 1
            if self._res_n < _RESERVOIR:
                self._res[self._res_n] = val
                self._res_n += 1
            else:
                j = self._rng.integers(0, self._seen)
                if j < _RESERVOIR:
                    self._res[j] = val

    def result(self) -> dict:
        total = self.n + self.nulls
        out: dict = {
            "n": self.n,
            "nulls": self.nulls,
            "null_pct": round(100 * self.nulls / total, 4) if total else None,
            "n_unique": (f">{_UNIQUE_CAP:,}" if self.unique_overflow else len(self.uniques)),
            "kind": "numeric" if self.is_numeric else "categorical",
        }
        if self.is_numeric and self.n:
            mean = self.vsum / self.n
            var = max(self.vsumsq / self.n - mean * mean, 0.0)
            res = self._res[: self._res_n]
            q = np.percentile(res, [1, 25, 50, 75, 99]) if res.size else [None] * 5
            out["numeric"] = {
                "min": _num(self.vmin), "max": _num(self.vmax),
                "mean": _num(mean), "std": _num(var ** 0.5),
                "p1": _num(q[0]), "p25": _num(q[1]), "median": _num(q[2]),
                "p75": _num(q[3]), "p99": _num(q[4]),
            }
        elif not self.is_numeric:
            top = sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)[:20]
            out["top_values"] = [[k, c, round(100 * c / self.n, 3) if self.n else None] for k, c in top]
            out["topk_truncated"] = self.topk_full
            out["min"], out["max"] = self.smin, self.smax
        return out


def _num(x):
    if x is None:
        return None
    x = float(x)
    return round(x, 6) if not (np.isnan(x) or np.isinf(x)) else None


def _year(iso: pd.Series) -> pd.Series:
    """ISO 'YYYY-MM-DD' string -> integer year (nullable)."""
    return pd.to_numeric(iso.astype("string").str[:4], errors="coerce").astype("Int64")


def _open_dataset(path: str, key: str, hive: bool):
    """Return (dataset, display_path). Handles local paths and gs:// via gcsfs."""
    fmt = "parquet"
    part = "hive" if hive else None
    if path.startswith("gs://"):
        if key and Path(key).exists():
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", key)
        import gcsfs
        from pyarrow.fs import FSSpecHandler, PyFileSystem
        fs = PyFileSystem(FSSpecHandler(gcsfs.GCSFileSystem()))
        return ds.dataset(path[len("gs://"):], filesystem=fs, format=fmt, partitioning=part), path
    return ds.dataset(path, format=fmt, partitioning=part), path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--panel", help="ingested panel parquet (file or dir; has derived columns)")
    src.add_argument("--raw-root", help="raw Hive-partitioned source root (labels derived per batch)")
    ap.add_argument("--out", default="reports/fannie_dataset_profile.json")
    ap.add_argument("--batch-rows", type=int, default=1_000_000)
    ap.add_argument("--limit-rows", type=int, default=0, help="stop after N rows (0 = all; smoke test)")
    ap.add_argument("--no-vintage", action="store_true", help="skip loan-level vintage default rates")
    ap.add_argument("--no-loan-count", action="store_true", help="skip exact unique-loan tracking")
    ap.add_argument("--delinquency-only", action="store_true",
                    help="only compute delinquency-by-year (streams ~5 columns) — fast on the full "
                         "raw book; skips the per-column profile")
    ap.add_argument("--key", default="/workspace/.gcloud/credit-fm-sa.json")
    args = ap.parse_args()

    path = args.panel or args.raw_root
    is_raw = args.raw_root is not None
    ing = _load_ingest() if is_raw else None
    dataset, disp = _open_dataset(path, args.key, hive=is_raw)
    schema_names = [f.name for f in dataset.schema]
    has_derived = "default_event" in schema_names
    if args.delinquency_only:
        # stream only what the delinquency/vintage math needs (cheap on the full raw book)
        if is_raw and not has_derived:
            needed = ["loan_identifier", "monthly_reporting_period", "origination_date",
                      "current_loan_delinquency_status", "zero_balance_code"]
        else:
            needed = ["loan_id", "reporting_date", "origination_date", "dlq_num",
                      "default_event", "is_performing"]
        needed = [c for c in needed if c in schema_names]
    elif is_raw and not has_derived:                     # raw source: we will derive
        needed = list(dict.fromkeys(schema_names))       # read everything, derive in-loop
    else:
        needed = schema_names
    print(f"source: {disp}\n{dataset.count_rows():,} rows announced, {len(schema_names)} columns"
          + ("  [delinquency-only]" if args.delinquency_only else ""), flush=True)

    cols: dict[str, _Col] = {}
    dlq_year: dict[int, dict[str, int]] = {}             # reporting-year delinquency accumulator
    loan_ids: set = set()
    vint_default: dict[str, bool] = {}                   # loan_id -> ever defaulted
    vint_year: dict[str, int] = {}                       # loan_id -> origination year
    rng_rep = [None, None]                               # reporting-date [min, max] (always tracked)
    rng_orig = [None, None]                              # origination-date [min, max]
    n_rows = 0
    t0 = time.time()

    for batch in dataset.to_batches(columns=needed, batch_size=args.batch_rows):
        df = batch.to_pandas()
        if is_raw and not has_derived:
            df = ing._derive(df)                          # adds loan_id + the 5 derived columns

        # per-column accumulators (built lazily so we know each column's kind) — skipped in
        # delinquency-only mode
        if not args.delinquency_only:
            for name in df.columns:
                s = df[name]
                if name not in cols:
                    cols[name] = _Col(name, is_numeric=pd.api.types.is_numeric_dtype(s.dtype)
                                      and not pd.api.types.is_bool_dtype(s.dtype))
                cols[name].update(s)

        # date ranges (cheap; always available even in delinquency-only mode)
        for col, rng in (("reporting_date", rng_rep), ("origination_date", rng_orig)):
            v = df[col].dropna().astype(str)
            if len(v):
                lo, hi = v.min(), v.max()
                rng[0] = lo if rng[0] is None else min(rng[0], lo)
                rng[1] = hi if rng[1] is None else max(rng[1], hi)

        # delinquency by reporting year
        yr = _year(df["reporting_date"])
        dlq = pd.to_numeric(df["dlq_num"], errors="coerce")
        deft = df["default_event"].fillna(False).astype(bool)
        perf = df["is_performing"].fillna(False).astype(bool)
        g = pd.DataFrame({"year": yr, "dpd30": (dlq >= 1), "d180": (dlq >= 6),
                          "deft": deft, "perf": perf, "known": dlq.notna()}).dropna(subset=["year"])
        for y, grp in g.groupby("year"):
            a = dlq_year.setdefault(int(y), dict(rows=0, known=0, dpd30=0, d180=0, deft=0, perf=0))
            a["rows"] += len(grp)
            a["known"] += int(grp["known"].sum())
            a["dpd30"] += int((grp["dpd30"] & grp["known"]).sum())
            a["d180"] += int((grp["d180"] & grp["known"]).sum())
            a["deft"] += int(grp["deft"].sum())
            a["perf"] += int(grp["perf"].sum())

        # loan-level tracking
        lid = df["loan_id"].astype(str)
        if not args.no_loan_count:
            loan_ids.update(lid.unique().tolist())
        if not args.no_vintage:
            oy = _year(df["origination_date"])
            vt = pd.DataFrame({"lid": lid, "oy": oy, "deft": deft}).dropna(subset=["oy"])
            gd = vt.groupby("lid").agg(oy=("oy", "first"), deft=("deft", "max"))
            for loan, row in gd.iterrows():
                vint_year.setdefault(loan, int(row["oy"]))
                if row["deft"]:
                    vint_default[loan] = True
                else:
                    vint_default.setdefault(loan, False)

        n_rows += len(df)
        print(f"  ...{n_rows:,} rows  ({n_rows / max(time.time() - t0, 1e-9):,.0f}/s)", flush=True)
        if args.limit_rows and n_rows >= args.limit_rows:
            break

    # assemble delinquency-by-year table
    dlq_table = []
    for y in sorted(dlq_year):
        a = dlq_year[y]
        known = a["known"] or 1
        dlq_table.append({
            "year": y, "loan_months": a["rows"], "known_status": a["known"],
            "dpd30_plus": a["dpd30"], "dpd30_plus_pct": round(100 * a["dpd30"] / known, 4),
            "d180_plus": a["d180"], "d180_plus_pct": round(100 * a["d180"] / known, 4),
            "default_event": a["deft"], "default_event_pct": round(100 * a["deft"] / a["rows"], 4),
            "performing_pct": round(100 * a["perf"] / a["rows"], 4),
        })

    # assemble vintage table
    vint_table = []
    if not args.no_vintage and vint_year:
        vt = pd.DataFrame({"oy": pd.Series(vint_year), "deft": pd.Series(vint_default)})
        for y, grp in vt.groupby("oy"):
            n = len(grp)
            d = int(grp["deft"].sum())
            vint_table.append({"origination_year": int(y), "n_loans": n, "n_ever_default": d,
                               "lifetime_default_pct": round(100 * d / n, 4)})
        vint_table.sort(key=lambda r: r["origination_year"])

    profile = {
        "source": disp,
        "source_kind": "raw" if is_raw else "panel",
        "delinquency_only": bool(args.delinquency_only),
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_rows": n_rows,
        "n_loans": (f">{_UNIQUE_CAP:,}" if args.no_loan_count else len(loan_ids)),
        "n_columns": len(cols),
        "reporting_range": rng_rep if rng_rep[0] else None,
        "origination_range": rng_orig if rng_orig[0] else None,
        "columns": {name: c.result() for name, c in cols.items()},
        "delinquency_by_reporting_year": dlq_table,
        "vintage_default_by_origination_year": vint_table,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(profile, indent=2, default=str))
    print(f"\nWrote {out}: {n_rows:,} rows, {profile['n_loans']} loans, "
          f"reporting {profile['reporting_range']}")
    print(f"delinquency-by-year: {len(dlq_table)} years; vintage: {len(vint_table)} years")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
