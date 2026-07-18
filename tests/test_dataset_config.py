# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Dataset-contract tests (v1.1 G1.1) — parsing, validation rules, and the drift guard.

The drift guard pins ``baseline.yaml``'s legacy leakage/exclude lists to the canonical
``dataset.yaml`` until every consumer reads the contract — the two may never disagree.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from credit_fm.data.dataset_config import (
    DatasetConfig,
    load_dataset_config,
    resolve_leakage_exclude,
)

REPO = Path(__file__).resolve().parent.parent

MINIMAL = """
dataset:
  name: toy
  adapter: generic
  id_col: loan_id
  time_col: reporting_date
  origination_col: origination_date
labels:
  default_12m:
    type: forward_event
    event_col: default_event
    horizon_months: 12
    gate_col: is_performing
leakage: [default_event, is_performing, current_dlq]
exclude: [deal_name]
"""


def _write(tmp_path, text) -> str:
    p = tmp_path / "dataset.yaml"
    p.write_text(text)
    return str(p)


def test_minimal_config_parses(tmp_path):
    ds = load_dataset_config(_write(tmp_path, MINIMAL))
    assert ds.name == "toy" and ds.adapter == "generic"
    assert ds.id_col == "loan_id" and ds.origination_col == "origination_date"
    spec = ds.labels["default_12m"]
    assert spec.event_col == "default_event" and spec.horizon_months == 12
    assert spec.event_value is True and spec.gate_values == (True,)
    assert "current_dlq" in ds.leakage and "deal_name" in ds.exclude
    assert ds.banned == ds.leakage | ds.exclude


@pytest.mark.parametrize("break_it, needle", [
    (lambda t: t.replace("  id_col: loan_id\n", ""), "dataset.id_col"),
    (lambda t: t.replace("type: forward_event", "type: nonsense"), "labels.default_12m.type"),
    (lambda t: t.replace("horizon_months: 12", "horizon_months: 0"), "horizon_months"),
    # event_col not listed under leakage -> the "label is the answer" rule
    (lambda t: t.replace("leakage: [default_event, is_performing, current_dlq]",
                         "leakage: [is_performing, current_dlq]"), "must also be listed under"),
    # gate_col not listed under leakage
    (lambda t: t.replace("leakage: [default_event, is_performing, current_dlq]",
                         "leakage: [default_event, current_dlq]"), "gate_col"),
    # a column in both lists -> ambiguous drop reason
    (lambda t: t.replace("exclude: [deal_name]", "exclude: [deal_name, current_dlq]"), "BOTH"),
])
def test_invalid_configs_fail_with_actionable_message(tmp_path, break_it, needle):
    with pytest.raises(ValueError, match=needle):
        load_dataset_config(_write(tmp_path, break_it(MINIMAL)))


def test_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_dataset_config(tmp_path / "nope.yaml")


def test_committed_fannie_contract_is_valid():
    ds = load_dataset_config(REPO / "configs/fannie_mae/dataset.yaml")
    assert ds.adapter == "fannie_mae" and not ds.origination_derived
    assert {"default_12m", "prepay_12m"} == set(ds.labels)
    assert ds.labels["default_12m"].gate_col == "is_performing"
    assert "current_loan_delinquency_status" in ds.leakage
    assert len(ds.leakage) >= 40 and len(ds.exclude) >= 14


def test_committed_dutch_contract_is_valid():
    ds = load_dataset_config(REPO / "configs/dutch_mortgages/dataset.yaml")
    assert ds.adapter == "generic" and ds.origination_derived        # DL-007
    spec = ds.labels["default_6m"]
    assert spec.event_value == "Y" and "Performing" in spec.gate_values


@pytest.mark.parametrize("asset", ["fannie_mae", "dutch_mortgages"])
def test_drift_guard_baseline_lists_match_dataset_contract(asset):
    """baseline.yaml's legacy lists must equal dataset.yaml's until consumers migrate (G2.1)."""
    ds = load_dataset_config(REPO / f"configs/{asset}/dataset.yaml")
    base = yaml.safe_load((REPO / f"configs/{asset}/baseline.yaml").read_text())
    assert set(base["leakage"]) == set(ds.leakage), "baseline.yaml drifted from dataset.yaml"
    assert set(base["exclude"]) == set(ds.exclude), "baseline.yaml drifted from dataset.yaml"


def test_resolve_leakage_exclude_old_and_new_paths(tmp_path):
    # old-style: inline lists win (deprecated but honored)
    exc, leak = resolve_leakage_exclude({"exclude": ["a"], "leakage": ["b"]})
    assert exc == ["a"] and leak == ["b"]
    # new-style: dataset pointer
    path = _write(tmp_path, MINIMAL)
    exc, leak = resolve_leakage_exclude({"dataset": path})
    assert "deal_name" in exc and "default_event" in leak
    # neither -> actionable error
    with pytest.raises(ValueError, match="dataset"):
        resolve_leakage_exclude({}, "some.yaml")


def test_frozen_dataclass_is_hashable_config():
    ds = DatasetConfig(name="x", adapter="generic", id_col="i", time_col="t",
                       origination_col="o", origination_derived=False)
    with pytest.raises(Exception):
        ds.name = "y"                                    # frozen — contract objects are immutable
