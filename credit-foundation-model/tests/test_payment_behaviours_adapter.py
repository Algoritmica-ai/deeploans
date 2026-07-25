# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Payment-behaviours adapter: sequence explosion, cleaning, contract + label integration."""

import pandas as pd
import pytest

from credit_fm.data.dataset_config import load_dataset_config
from credit_fm.data.labels import forward_event_entities
from reference_implementations.payment_behaviours.adapter import (
    PaymentBehavioursAdapter,
    explode_sequences,
)

DATASET_YAML = "configs/payment_behaviours/dataset.yaml"


def _raw() -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": ["cust_a", "cust_b", "cust_c"],
        "payment_sequence": [
            "0|0|16|45",          # goes >30 dpd at invoice 3
            "0",                  # single on-time invoice
            "5|999999|-3|x|12",   # outlier to cap, negative to clip, junk to drop
        ],
    })


def test_explode_shapes_and_cleaning():
    panel = explode_sequences(_raw(), base_month="2000-01", cap_dpd=3650)
    assert list(panel["loan_id"].unique()) == ["cust_a", "cust_b", "cust_c"]
    assert len(panel) == 4 + 1 + 4                      # the junk 'x' entry is dropped
    c = panel[panel["loan_id"] == "cust_c"].reset_index(drop=True)
    assert c["dpd"].tolist() == [5, 3650, 0, 12]        # capped, clipped; junk gone
    assert c["seq_index"].tolist() == [0, 1, 2, 4]      # position preserved across the drop


def test_pseudo_dates_and_flags():
    panel = explode_sequences(_raw(), base_month="2000-01")
    a = panel[panel["loan_id"] == "cust_a"]
    assert a["reporting_date"].tolist() == ["2000-01-31", "2000-02-29", "2000-03-31",
                                            "2000-04-30"]
    assert (panel["origination_date"] == "2000-01-31").all()
    assert a["late30_event"].tolist() == [False, False, False, True]
    assert a["under30"].tolist() == [True, True, True, False]
    assert panel["late90_event"].sum() == 1             # only cust_c's capped outlier


def test_adapter_load_panel_and_sampling(tmp_path):
    csv = tmp_path / "pb.csv"
    _raw().to_csv(csv, index=False)
    ds = load_dataset_config(DATASET_YAML)              # also validates the contract rules
    adapter = PaymentBehavioursAdapter(ds, stage={"source_csv": str(csv)})
    panel = adapter.load_panel()
    assert adapter.sources() == [str(csv)]
    assert panel["loan_id"].dtype == object             # ids are ALWAYS strings (contract)
    for spec in ds.labels.values():                     # contract columns actually exist
        assert spec.event_col in panel.columns and spec.gate_col in panel.columns
    sampled = PaymentBehavioursAdapter(
        ds, stage={"source_csv": str(csv), "sample_pct": 0}).load_panel
    with pytest.raises(Exception):                      # 0% sample -> empty explode fails loud
        sampled()


def test_forward_event_label_semantics():
    """late30_3m: cust_a is <=30 dpd at invoice 0 (2000-01-31) and fires within 3 months."""
    panel = explode_sequences(_raw(), base_month="2000-01")
    ds = load_dataset_config(DATASET_YAML)
    spec = ds.labels["late30_3m"]
    fired = forward_event_entities(panel, spec, id_col="loan_id", time_col="reporting_date",
                                   cutoff="2000-01-31")
    assert fired == {"cust_a", "cust_c"}                # cust_c's capped spike is at 2000-02
    fired_feb = forward_event_entities(panel, spec, id_col="loan_id",
                                       time_col="reporting_date", cutoff="2000-02-29")
    assert fired_feb == {"cust_a"}                      # cust_c stays <=30 dpd after February
