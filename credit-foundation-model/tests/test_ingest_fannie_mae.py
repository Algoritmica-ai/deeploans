# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Unit tests for the Fannie ingest derivation logic (scripts/ingest_fannie_mae.py).

These prove the column derivations are correct on hand-crafted rows covering every case:
current/late/default/credit-event/prepay/unknown-delinquency, date parsing, the code-set matching,
and the structural mutual-exclusivity invariant. No network / GCS — pure logic.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

# load the script as a module (scripts/ is not a package)
_spec = importlib.util.spec_from_file_location(
    "fannie_adapter",
    Path(__file__).resolve().parent.parent / "reference_implementations" / "fannie_mae" / "adapter.py")
ing = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ing)


# ------------------------------------------------------------------ date parsing
def test_iso_month_end_parses_mmyyyy_to_month_end():
    s = pd.Series(["012016", "022016", "022015", "122024"])   # incl. leap (2016) vs non-leap (2015)
    out = ing._iso_month_end(s).tolist()
    assert out == ["2016-01-31", "2016-02-29", "2015-02-28", "2024-12-31"]


def test_iso_month_end_blank_and_bad_become_na():
    out = ing._iso_month_end(pd.Series(["", "132016", None]))
    assert out.isna().all()


# ------------------------------------------------------------------ derivation
def _raw():
    """One row per case. Raw Fannie column names; MMYYYY dates; string codes."""
    rows = [
        # (dlq, zbc, note)                                     expected default/perf/prepay
        ("0",  "",   "current performing"),                    # perf
        ("6",  "",   "D180 default"),                          # default
        ("5",  "",   "just under D180"),                       # neither
        ("0",  "9",  "credit event REO, zfill->09"),           # default (zbc)
        ("0",  "1",  "prepay, zfill->01"),                     # prepay
        ("XX", "",   "unknown delinquency"),                   # neither (NaN)
        ("12", "03", "deep default + short sale"),             # default
    ]
    return pd.DataFrame({
        "loan_identifier": [f"L{i}" for i in range(len(rows))],
        "monthly_reporting_period": ["012016"] * len(rows),
        "origination_date": ["032014"] * len(rows),
        "current_loan_delinquency_status": [r[0] for r in rows],
        "zero_balance_code": [r[1] for r in rows],
        "extra_raw_col": range(len(rows)),                     # a non-required col is preserved
    })


def test_derive_columns_and_rename():
    d = ing._derive(_raw())
    for c in ("loan_id", "reporting_date", "origination_date", "dlq_num",
              "default_event", "prepay_event", "is_performing"):
        assert c in d.columns, c
    assert "loan_identifier" not in d.columns          # renamed away
    assert "extra_raw_col" in d.columns                # raw cols preserved (leakage dropped later)
    assert d.reporting_date.iloc[0] == "2016-01-31" and d.origination_date.iloc[0] == "2014-03-31"


def test_derive_labels_match_rules():
    # flags resolve correctly once NA (unknown-delinquency) is treated as False — which is exactly
    # what every downstream consumer does via .fillna(False) (label + gate).
    d = ing._derive(_raw())
    assert d.default_event.fillna(False).tolist() == [False, True, False, True,  False, False, True]
    assert d.is_performing.fillna(False).tolist() == [True,  False, False, False, False, False, False]
    assert d.prepay_event.fillna(False).tolist()  == [False, False, False, False, True,  False, False]
    assert d.dlq_num.iloc[1] == 6 and pd.isna(d.dlq_num.iloc[5])   # 'XX' -> NA (nullable Int64)


def test_flags_na_only_from_unknown_dlq_and_safe_after_fillna():
    """Documented contract (validation finding, 5 Jul): the derived flags are NOT uniformly typed —
    `default_event`/`is_performing` are nullable `boolean` (Int64 arithmetic propagates <NA>), while
    `prepay_event` is plain `bool`. Any <NA> comes ONLY from unknown-delinquency ('XX'/blank) rows,
    and every downstream consumer resolves it with .fillna(False). This test locks that contract:
    if a flag ever goes <NA> for a *known*-delinquency row, or a consumer drops the fillna, it breaks."""
    d = ing._derive(_raw())
    unknown = d.dlq_num.isna()
    for c in ("default_event", "is_performing", "prepay_event"):
        assert (d[c].isna() <= unknown).all(), f"{c} is NA on a known-delinquency row"
        f = d[c].fillna(False).astype(bool)          # the downstream-standard coercion
        assert f.dtype == bool and f.notna().all()
    assert bool(d.default_event.isna().iloc[5])       # the unknown-dlq row is indeed <NA>


def test_mutual_exclusivity_invariant():
    """A loan can never be both performing and (defaulted or prepaid) — the structural guarantee
    (evaluated the way downstream sees the labels: NA -> False)."""
    d = ing._derive(_raw())
    perf = d.is_performing.fillna(False)
    deft = d.default_event.fillna(False)
    prep = d.prepay_event.fillna(False)
    assert not (perf & deft).any()
    assert not (perf & prep).any()
    assert not (deft & prep).any()        # a termination is a default OR a prepay, never both


def test_derive_requires_the_five_columns():
    with pytest.raises(SystemExit, match="Missing expected Fannie columns"):
        ing._derive(pd.DataFrame({"loan_identifier": ["L0"]}))


# ------------------------------------------------------------------ path / source resolution
def test_hive_path():
    assert ing._hive_path("gs://b/root", "2016Q1") == "gs://b/root/reporting_year=2016/reporting_quarter=Q1"


def test_hive_path_rejects_bad_format():
    with pytest.raises(SystemExit, match="YYYYQn"):
        ing._hive_path("gs://b/root", "2016-Q1")


def _adapter(stage: dict):
    """Build a FannieMaeAdapter with a stub DatasetConfig (source resolution needs no contract)."""
    from credit_fm.data.dataset_config import DatasetConfig
    cfg = DatasetConfig(name="fannie_mae", adapter="fannie_mae", id_col="loan_id",
                        time_col="reporting_date", origination_col="origination_date",
                        origination_derived=False)
    return ing.FannieMaeAdapter(cfg, stage=stage)


def test_adapter_sources_files_take_precedence():
    ad = _adapter({"sources": {"files": ["gs://x/a.parquet"], "root": "gs://x/root",
                               "reporting": ["2016Q1"]}})
    assert ad.sources() == ["gs://x/a.parquet"]


def test_adapter_sources_builds_hive_paths():
    ad = _adapter({"sources": {"files": None, "root": "gs://x/root",
                               "reporting": ["2016Q1", "2016Q2"]}})
    assert ad.sources() == [
        "gs://x/root/reporting_year=2016/reporting_quarter=Q1",
        "gs://x/root/reporting_year=2016/reporting_quarter=Q2"]


def test_adapter_sources_errors_without_inputs():
    with pytest.raises(SystemExit, match="sources.files OR"):
        _adapter({"sources": {"files": None, "root": None, "reporting": None}}).sources()


def test_adapter_load_panel_end_to_end_local(tmp_path):
    """FannieMaeAdapter on local parquet sources: derives, samples, concatenates — no GCS."""
    raw = pd.DataFrame({
        "loan_identifier": [f"10{i}" for i in range(20)],
        "monthly_reporting_period": ["012016"] * 20,
        "origination_date": ["062015"] * 20,
        "current_loan_delinquency_status": ["0"] * 19 + ["6"],
        "zero_balance_code": [""] * 20,
    })
    src = tmp_path / "part.parquet"
    raw.to_parquet(src, index=False)
    ad = _adapter({"sources": {"files": [str(src)]}, "sample_pct": 100, "workers": 1})
    panel = ad.load_panel()
    assert len(panel) == 20 and panel["loan_id"].nunique() == 20
    assert panel["reporting_date"].iloc[0] == "2016-01-31"          # MMYYYY -> ISO month-end
    assert panel["default_event"].sum() == 1                        # the one D180 row
    assert bool(panel["is_performing"].iloc[0]) is True
    assert ad.sources() == [str(src)]


# ------------------------------------------------------------------ sampling determinism
def test_sample_hash_is_deterministic_and_bounded():
    ids = pd.Series([f"L{i}" for i in range(10000)])
    k1 = pd.util.hash_pandas_object(ids, index=False) % 100 < 4
    k2 = pd.util.hash_pandas_object(ids, index=False) % 100 < 4
    assert (k1 == k2).all()                       # deterministic
    assert 0.02 < k1.mean() < 0.06                # ~4% of loans kept
