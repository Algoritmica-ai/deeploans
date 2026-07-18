# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Fannie Mae dataset adapter — column derivations + parallel source reading (v1.1 G1.4).

Everything Fannie-specific about ingest lives HERE (moved out of ``scripts/ingest_fannie_mae.py``
so the core package and stock scripts stay asset-blind). The published schema is the 113-field
Fannie layout with snake_case names; sources are Hive-partitioned by reporting period::

    fannie_by_reporting/reporting_year=<YYYY>/reporting_quarter=<Q#>/from_<acqQ>_*.parquet

Derived columns (the contract in ``configs/fannie_mae/dataset.yaml``):
  * ``loan_id``           renamed from ``loan_identifier`` (kept as str)
  * ``reporting_date``    ISO 'YYYY-MM-DD' month-end string from ``monthly_reporting_period``
  * ``origination_date``  the MMYYYY string parsed in place to an ISO date string (the split key)
  * ``dlq_num``           ``current_loan_delinquency_status`` as int (``XX``/blank -> NaN)
  * ``default_event``     True if dlq_num >= 6 (D180) OR zero_balance_code is a credit event
  * ``prepay_event``      True if zero_balance_code == '01' (prepaid/matured)
  * ``is_performing``     True if current (dlq_num == 0) and not yet terminated

Used via the stock driver (``python scripts/ingest.py -c configs/fannie_mae/ingest_2000_2024.yaml``)
or directly. ``scripts/validate_ingest.py`` re-derives these columns from the retained raw ones.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from credit_fm.data.adapter import register_adapter
from credit_fm.data.dataset_config import DatasetConfig
from credit_fm.utils import storage

# Zero Balance Code -> outcome. Credit events count as default; 01 is a clean prepay.
ZBC_CREDIT_EVENT = {"02", "03", "09", "15"}  # third-party sale, short sale, REO/DIL, note sale
ZBC_PREPAY = {"01"}                          # prepaid or matured
D180 = 6                                     # months delinquent that defines a default

# Real published column names the derivations depend on.
COL_ID = "loan_identifier"
COL_REPORTING = "monthly_reporting_period"
COL_ORIG = "origination_date"
COL_DLQ = "current_loan_delinquency_status"
COL_ZBC = "zero_balance_code"
REQUIRED = [COL_ID, COL_REPORTING, COL_ORIG, COL_DLQ, COL_ZBC]


def _iso_month_end(s: pd.Series) -> pd.Series:
    """Fannie dates are MMYYYY strings (e.g. '012016'); return ISO 'YYYY-MM-DD' month-end strings.

    The pipeline convention is an ISO-date *string* time column (chronologically sortable, and
    what ``train_baseline`` / ``prepare_data`` compare against) — not a timestamp.
    """
    txt = s.astype("string").str.strip()
    dt = pd.to_datetime(txt, format="%m%Y", errors="coerce")
    if dt.notna().any() and dt.isna().mean() > 0.5:   # permissive fallback for odd mirrors
        dt = pd.to_datetime(txt, errors="coerce")
    dt = dt + pd.offsets.MonthEnd(0)
    return dt.dt.strftime("%Y-%m-%d").where(dt.notna(), pd.NA)


def _derive(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise SystemExit(
            f"Missing expected Fannie columns {missing}. Got {len(df.columns)} cols; "
            f"first 30: {sorted(df.columns)[:30]}")

    df = df.rename(columns={COL_ID: "loan_id"})
    df["reporting_date"] = _iso_month_end(df[COL_REPORTING])       # ISO 'YYYY-MM-DD' string
    df["origination_date"] = _iso_month_end(df[COL_ORIG])          # ISO string, in place

    dlq = df[COL_DLQ].astype("string").str.strip()
    df["dlq_num"] = pd.to_numeric(dlq, errors="coerce")           # 'XX'/'' -> NaN
    zbc = df[COL_ZBC].astype("string").str.strip().str.zfill(2)

    df["default_event"] = (df["dlq_num"] >= D180) | zbc.isin(ZBC_CREDIT_EVENT)
    df["prepay_event"] = zbc.isin(ZBC_PREPAY)
    df["is_performing"] = (df["dlq_num"] == 0) & (~zbc.isin(ZBC_CREDIT_EVENT | ZBC_PREPAY))
    return df


def _hive_path(root: str, reporting: str) -> str:
    """'2016Q1' -> '<root>/reporting_year=2016/reporting_quarter=Q1'."""
    year, _, q = reporting.partition("Q")
    if not (year.isdigit() and q.isdigit()):
        raise SystemExit(f"--reporting expects YYYYQn (e.g. 2016Q1), got '{reporting}'")
    return f"{root.rstrip('/')}/reporting_year={year}/reporting_quarter=Q{q}"


@register_adapter("fannie_mae")
class FannieMaeAdapter:
    """DatasetAdapter for the Fannie source (see module docstring).

    ``stage`` is the ingest stage config (a mapping) carrying ``sources.files`` OR
    ``sources.root`` + ``sources.reporting``, plus ``sample_pct`` / ``workers`` / ``key``.
    """

    def __init__(self, config: DatasetConfig, *, stage):
        self.config = config
        self.stage = stage
        self.sample_pct = int(self._get("sample_pct", 100))
        self.workers = int(self._get("workers", 8))
        self.key = self._get("key")

    def _get(self, dotted: str, default=None):
        """Dotted lookup on the stage config (works for Config and plain dicts alike)."""
        cur = self.stage
        for part in dotted.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    # ------------------------------------------------------------------ sources
    def sources(self) -> list[str]:
        files = self._get("sources.files")
        if files:
            return list(files)
        root, reporting = self._get("sources.root"), self._get("sources.reporting")
        if not (root and reporting):
            raise SystemExit("ingest config needs sources.files OR sources.root + "
                             "sources.reporting (e.g. [2016Q1, 2016Q2]).")
        return [_hive_path(root, r) for r in reporting]

    def source_tag(self, source: str) -> str:
        """Shard tag for the sharded ingest driver (G3.1): the reporting quarter when the source
        is a hive quarter dir ('…/reporting_year=2016/reporting_quarter=Q1' -> '2016Q1'), else a
        sanitized basename."""
        m = re.search(r"reporting_year=(\d{4})/reporting_quarter=Q(\d)", str(source))
        if m:
            return f"{m.group(1)}Q{m.group(2)}"
        base = str(source).rstrip("/").rsplit("/", 1)[-1].removesuffix(".parquet")
        return re.sub(r"[^A-Za-z0-9_=-]+", "-", base)

    # ------------------------------------------------------------------ load
    def load_source(self, s: str) -> pd.DataFrame:
        """Read ONE source (file or hive quarter dir), derive contract columns, apply the
        loan-hash sample. Public: the sharded ingest driver (G3.1) calls this per source."""
        df = _derive(storage.read_parquet(s))         # fsspec read: file or hive dir, local/gs://
        if self.sample_pct < 100:                     # deterministic loan-hash sample
            keep = pd.util.hash_pandas_object(df["loan_id"], index=False) % 100 < self.sample_pct
            df = df[keep]
        print(f"  done {s}: {len(df):>10,} rows  {df['loan_id'].nunique():>8,} loans  "
              f"reporting {df['reporting_date'].min()}..{df['reporting_date'].max()}", flush=True)
        return df

    def load_panel(self) -> pd.DataFrame:
        srcs = self.sources()
        print(f"Reading {len(srcs)} source(s) with {self.workers} parallel workers ...", flush=True)
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            frames = list(ex.map(self.load_source, srcs))     # order preserved
        return pd.concat(frames, ignore_index=True)
