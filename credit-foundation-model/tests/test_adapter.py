# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Adapter tests (v1.1 G1.2) — registry resolution + the generic zero-code onboarding path."""

from __future__ import annotations

import pandas as pd
import pytest

from credit_fm.data.adapter import (
    REGISTRY,
    DatasetAdapter,
    GenericParquetAdapter,
    get_adapter,
    register_adapter,
)
from credit_fm.data.dataset_config import DatasetConfig, LabelSpec


def _cfg(**over) -> DatasetConfig:
    base = dict(
        name="toy", adapter="generic", id_col="loan_id", time_col="reporting_date",
        origination_col="origination_date", origination_derived=False,
        labels={"default_12m": LabelSpec(name="default_12m", type="forward_event",
                                         event_col="default_event", horizon_months=12,
                                         gate_col="is_performing")},
        leakage=frozenset({"default_event", "is_performing"}))
    base.update(over)
    return DatasetConfig(**base)


def _panel() -> pd.DataFrame:
    return pd.DataFrame({
        "loan_id": [101, 101, 202],                      # ints on purpose — must come back str
        "reporting_date": ["2020-01-31", "2020-02-29", "2020-01-31"],
        "origination_date": ["2019-12-31"] * 3,
        "default_event": [False, False, True],
        "is_performing": [True, True, False],
        "original_ltv": [80.0, 80.0, 95.0],
    })


def test_generic_adapter_loads_and_coerces_ids(tmp_path):
    path = str(tmp_path / "panel.parquet")
    _panel().to_parquet(path, index=False)
    ad = get_adapter(_cfg(), path=path)
    assert isinstance(ad, GenericParquetAdapter) and isinstance(ad, DatasetAdapter)
    df = ad.load_panel()
    assert df["loan_id"].dtype == object and df["loan_id"].iloc[0] == "101"   # str, not int
    assert ad.sources() == [path]


def test_generic_adapter_rejects_missing_contract_columns(tmp_path):
    path = str(tmp_path / "panel.parquet")
    _panel().drop(columns=["is_performing"]).to_parquet(path, index=False)
    with pytest.raises(ValueError, match="is_performing"):
        get_adapter(_cfg(), path=path).load_panel()


def test_origination_derived_skips_that_column(tmp_path):
    path = str(tmp_path / "panel.parquet")
    _panel().drop(columns=["origination_date"]).to_parquet(path, index=False)
    df = get_adapter(_cfg(origination_derived=True), path=path).load_panel()   # Dutch-style
    assert "origination_date" not in df.columns          # fine — split derives it (DL-007)


def test_unknown_adapter_error_is_actionable():
    with pytest.raises(KeyError, match="no adapter registered for 'no_such_asset'"):
        get_adapter(_cfg(adapter="no_such_asset"))


def test_register_adapter_and_dispatch(tmp_path):
    @register_adapter("_test_asset")
    class TestAdapter:
        def __init__(self, config, **options):
            self.config = config

        def load_panel(self):
            return _panel()

        def sources(self):
            return ["synthetic"]

    try:
        ad = get_adapter(_cfg(adapter="_test_asset"))
        assert isinstance(ad, TestAdapter) and len(ad.load_panel()) == 3
        assert isinstance(ad, DatasetAdapter)            # protocol satisfied structurally
    finally:
        REGISTRY.pop("_test_asset", None)                # keep the registry clean for other tests
