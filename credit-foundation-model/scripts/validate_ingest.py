# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Validate an ingested Fannie panel against the ingest invariants — read-only audit.

Proves the *produced artifact* is correct (not just the code): the derived label columns
(``default_event``/``is_performing``/``prepay_event``/``dlq_num``/``reporting_date``) exactly
re-derive from the retained raw columns, the dates are month-end ISO strings, the loan sample obeys
the hash bound, and the performing/default/prepay flags are mutually exclusive.

By default it validates the **first parquet row group** (fast). ``--full`` reads the whole panel
(slow: reads ~10 columns of ~125M rows) and additionally checks global row/loan counts and the
reporting-date range.

    python scripts/validate_ingest.py --panel gs://.../panel_2000_2024.parquet --sample-pct 4
    python scripts/validate_ingest.py --panel gs://.../panel_2000_2024.parquet --full
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

_spec = importlib.util.spec_from_file_location(
    "fannie_adapter", Path(__file__).resolve().parent.parent
    / "reference_implementations" / "fannie_mae" / "adapter.py")
ing = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ing)

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RAW = ["loan_id", "monthly_reporting_period", "origination_date",
        "current_loan_delinquency_status", "zero_balance_code"]
_DERIVED = ["reporting_date", "dlq_num", "default_event", "prepay_event", "is_performing"]


def _open(path: str):
    if path.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        return pq.ParquetFile(fs.open(path[len("gs://"):]))
    return pq.ParquetFile(path)


def _bool(s: pd.Series):
    return s.fillna(False).astype(bool).to_numpy()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--panel", required=True)
    ap.add_argument("--sample-pct", type=int, default=4)
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--key", default="/workspace/.gcloud/credit-fm-sa.json")
    args = ap.parse_args()
    if args.panel.startswith("gs://") and Path(args.key).exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.key)

    pf = _open(args.panel)
    meta = pf.metadata
    results: list[tuple[bool, str, str]] = []

    def chk(name, cond, detail=""):
        results.append((bool(cond), name, detail))

    chk("panel is non-empty", meta.num_rows > 0, f"{meta.num_rows:,} rows, {meta.num_columns} cols")

 
    cols = _RAW + _DERIVED

    if args.full:
        if args.panel.startswith("gs://"):
            import gcsfs

            fs = gcsfs.GCSFileSystem()

            with fs.open(args.panel[len("gs://"):], "rb") as f:
                df = pd.read_parquet(f, columns=cols)
        else:
            df = pd.read_parquet(args.panel, columns=cols)
    else:
        df = pf.read_row_group(0, columns=cols).to_pandas()
    scope = "FULL panel" if args.full else "first row group"
    print(f"validating {len(df):,} rows ({scope})\n", flush=True)

    # 1) re-derive labels from the retained raw columns and compare to the stored derived columns
    raw = pd.DataFrame({
        "loan_identifier": df["loan_id"],
        "monthly_reporting_period": df["monthly_reporting_period"],
        "origination_date": df["origination_date"],     # already ISO; only its label logic is re-checked
        "current_loan_delinquency_status": df["current_loan_delinquency_status"],
        "zero_balance_code": df["zero_balance_code"],
    })
    red = ing._derive(raw)
    for c in ("default_event", "is_performing", "prepay_event"):
        chk(f"re-derived {c} == stored", (_bool(red[c]) == _bool(df[c])).all())
    chk("re-derived dlq_num == stored",
        (red["dlq_num"].fillna(-1).astype("int64").to_numpy()
         == df["dlq_num"].fillna(-1).astype("int64").to_numpy()).all())
    chk("re-derived reporting_date == stored",
        (red["reporting_date"].fillna("").to_numpy() == df["reporting_date"].fillna("").to_numpy()).all())

    # 2) dates are month-end ISO strings
    for c in ("reporting_date", "origination_date"):
        v = df[c].dropna().astype(str)
        fmt_ok = v.map(lambda x: bool(_ISO.match(x))).all()
        me_ok = pd.to_datetime(v, errors="coerce").dt.is_month_end.all()
        chk(f"{c} is month-end ISO", fmt_ok and me_ok)

    # 3) mutual exclusivity — STRUCTURAL guarantees from the is_performing definition
    #    (a performing loan has dlq==0 and no termination, so it can be neither defaulted nor prepaid).
    #    NOTE: defaulted-AND-prepaid is NOT structural (independent code paths) so it is not asserted.
    perf, deft, prep = _bool(df.is_performing), _bool(df.default_event), _bool(df.prepay_event)
    chk("no loan is performing AND defaulted", not (perf & deft).any())
    chk("no loan is performing AND prepaid", not (perf & prep).any())

    # 4) any NA flag is only from unknown delinquency
    unknown = df.dlq_num.isna().to_numpy()
    chk("flag NA only on unknown-delinquency rows",
        bool((df.default_event.isna().to_numpy() <= unknown).all()))

    # 5) sampling bound: every kept loan hashes into the requested percentile
    if args.sample_pct < 100:
        ids = pd.Series(df["loan_id"].unique())
        maxb = int((pd.util.hash_pandas_object(ids, index=False) % 100).max())
        chk(f"loan sample <= {args.sample_pct}%", maxb < args.sample_pct, f"max hash bucket = {maxb}")

    # 6) global checks (only meaningful on --full)
    if args.full:
        chk("reporting range starts <= 2000", df.reporting_date.min() <= "2000-12-31",
            f"{df.reporting_date.min()} .. {df.reporting_date.max()}")
        chk("loan count > 1M (4% of ~50M)", df.loan_id.nunique() > 1_000_000,
            f"{df.loan_id.nunique():,} loans")

    ok = all(r[0] for r in results)
    for passed, name, detail in results:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
