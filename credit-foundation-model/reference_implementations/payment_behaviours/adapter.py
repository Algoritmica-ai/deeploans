# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Payment behaviours adapter — B2B invoice days-past-due sequences -> contract panel.

The raw source is one CSV with a row per customer::

    customer_id,payment_sequence
    f61b2f…,0|0|16|13|35|33|…        # i-th value = days past due of the i-th invoice (0 = on time)

This adapter *explodes* each sequence into contract-conforming panel rows (one row per invoice)
so the stock pipeline runs unchanged. The dataset carries **no calendar dates**, so time is
synthesized from sequence position (one pseudo-month per invoice, DL-007 spirit):

  * ``reporting_date``    ISO month-end string: ``base_month + seq_index`` months
  * ``origination_date``  ISO month-end of ``base_month`` — constant for every customer, which
    makes the "temporal" split's positional partition an **entity-disjoint split ordered by the
    (hashed, i.e. effectively random) customer id**. There is no true calendar here; see the
    runbook for what out-of-time does and does not mean on this asset.

Derived columns (the contract in ``configs/payment_behaviours/dataset.yaml``):
  * ``loan_id``       customer_id as str (contract column names are kept framework-standard)
  * ``dpd``           days past due, cleaned: non-numeric -> dropped, negatives -> 0, capped at
                      ``cap_dpd`` (the raw file contains corrupt outliers up to ~665k days)
  * ``late30_event`` / ``late90_event``   dpd > 30 / dpd > 90 (label event columns — leakage)
  * ``under30`` / ``under90``             dpd <= 30 / dpd <= 90 (observation gates — leakage)

Used via the stock driver::

    python scripts/ingest.py -c configs/payment_behaviours/ingest.yaml
"""

from __future__ import annotations

import pandas as pd

from credit_fm.data.adapter import register_adapter
from credit_fm.data.dataset_config import DatasetConfig

RAW_ID = "customer_id"
RAW_SEQ = "payment_sequence"


def explode_sequences(raw: pd.DataFrame, *, base_month: str = "2000-01",
                      cap_dpd: int = 3650) -> pd.DataFrame:
    """Explode ``customer_id,payment_sequence`` rows into the per-invoice contract panel."""
    missing = [c for c in (RAW_ID, RAW_SEQ) if c not in raw.columns]
    if missing:
        raise SystemExit(f"Missing expected columns {missing}. Got: {list(raw.columns)[:10]}")

    panel = raw[[RAW_ID]].copy()
    panel["dpd"] = raw[RAW_SEQ].astype("string").str.split("|")
    panel = panel.explode("dpd", ignore_index=True)
    panel["seq_index"] = panel.groupby(RAW_ID, sort=False).cumcount()
    panel["dpd"] = pd.to_numeric(panel["dpd"], errors="coerce")

    bad = int(panel["dpd"].isna().sum())
    if bad:
        print(f"  dropping {bad:,} non-numeric sequence entries", flush=True)
        panel = panel.dropna(subset=["dpd"]).reset_index(drop=True)
    panel["dpd"] = panel["dpd"].clip(lower=0, upper=cap_dpd).astype("int64")

    # pseudo-calendar: invoice i -> month-end of (base_month + i); origination = base month-end
    horizon = pd.period_range(base_month, periods=int(panel["seq_index"].max()) + 1, freq="M")
    month_end = horizon.to_timestamp(how="end").normalize().strftime("%Y-%m-%d")
    panel["reporting_date"] = month_end[panel["seq_index"].to_numpy()]
    panel["origination_date"] = month_end[0]

    panel["late30_event"] = panel["dpd"] > 30
    panel["late90_event"] = panel["dpd"] > 90
    panel["under30"] = ~panel["late30_event"]
    panel["under90"] = ~panel["late90_event"]
    return panel.rename(columns={RAW_ID: "loan_id"}).astype({"loan_id": str})


@register_adapter("payment_behaviours")
class PaymentBehavioursAdapter:
    """DatasetAdapter for the anonymized invoice payment-behaviour CSV (see module docstring).

    ``stage`` is the ingest stage config (a mapping) carrying ``source_csv`` plus optional
    ``base_month`` / ``cap_dpd`` / ``sample_pct``.
    """

    def __init__(self, config: DatasetConfig, *, stage):
        self.config = config
        self.stage = stage

    def _get(self, key: str, default=None):
        return self.stage[key] if isinstance(self.stage, dict) and key in self.stage else default

    def sources(self) -> list[str]:
        src = self._get("source_csv")
        if not src:
            raise SystemExit("ingest config needs source_csv: path to the payment-behaviours CSV")
        return [str(src)]

    def load_panel(self) -> pd.DataFrame:
        [src] = self.sources()
        raw = pd.read_csv(src)                            # fsspec-capable (local / gs:// / s3://)
        print(f"  read {src}: {len(raw):,} customers", flush=True)
        sample_pct = int(self._get("sample_pct", 100))
        if sample_pct < 100:                              # deterministic customer-hash sample
            keep = pd.util.hash_pandas_object(raw[RAW_ID].astype(str), index=False) % 100
            raw = raw[keep < sample_pct]
            print(f"  sample_pct={sample_pct}: kept {len(raw):,} customers", flush=True)
        panel = explode_sequences(raw, base_month=str(self._get("base_month", "2000-01")),
                                  cap_dpd=int(self._get("cap_dpd", 3650)))
        print(f"  exploded -> {len(panel):,} invoice rows, "
              f"{panel['loan_id'].nunique():,} customers, "
              f"reporting {panel['reporting_date'].min()}..{panel['reporting_date'].max()}",
              flush=True)
        return panel
