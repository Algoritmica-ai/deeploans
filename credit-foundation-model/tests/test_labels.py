# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Label-layer tests (v1.1 G2.1) — golden equivalence with the legacy logic + the spec paths.

The legacy ``forward_default_loans`` (verbatim copy below, from pre-G2 ``finetune.py``) is the
reference: the spec-driven builder must reproduce it EXACTLY on boolean labels. Additional cases
pin the horizon boundary, the non-boolean (Dutch-style) event value, gate-value lists, and the
config resolution paths (contract vs legacy-with-deprecation).
"""

from __future__ import annotations

import pandas as pd
import pytest

from credit_fm.data.dataset_config import LabelSpec
from credit_fm.data.labels import forward_event_entities, resolve_label_spec
from credit_fm.inference.scoring import observe_panel


def _legacy_forward_default_loans(panel, id_col, time_col, label_col, cutoff, horizon_months):
    """VERBATIM pre-G2 implementation (finetune.py) — the golden reference."""
    lo = pd.to_datetime(cutoff)
    hi = lo + pd.DateOffset(months=horizon_months)
    dt = pd.to_datetime(panel[time_col], errors="coerce")
    hit = panel[(dt > lo) & (dt <= hi) & panel[label_col].fillna(False).astype(bool)]
    return set(hit[id_col])


def _panel() -> pd.DataFrame:
    """Loans covering every labelling case around a 2020-12-31 cutoff, 12mo horizon."""
    rows = [
        # in-window default
        ("L_hit",      "2021-06-30", True,  False),
        # default exactly AT the horizon end (dt <= hi -> counts)
        ("L_boundary", "2021-12-31", True,  False),
        # default AFTER the horizon (excluded)
        ("L_late",     "2022-01-31", True,  False),
        # default AT the cutoff itself (dt > lo -> excluded; it's not a FORWARD event)
        ("L_at_cut",   "2020-12-31", True,  False),
        # never defaults
        ("L_clean",    "2021-06-30", False, True),
        # NA label in window (nullable boolean -> treated as no event)
        ("L_na",       "2021-06-30", None,  True),
    ]
    df = pd.DataFrame(rows, columns=["loan_id", "reporting_date", "default_event", "is_performing"])
    df["default_event"] = df["default_event"].astype("boolean")     # pandas nullable boolean
    return df


SPEC = LabelSpec(name="default_12m", type="forward_event", event_col="default_event",
                 horizon_months=12, gate_col="is_performing")


def test_golden_equivalence_with_legacy_forward_default_loans():
    panel = _panel()
    legacy = _legacy_forward_default_loans(panel, "loan_id", "reporting_date",
                                           "default_event", "2020-12-31", 12)
    new = forward_event_entities(panel, SPEC, id_col="loan_id", time_col="reporting_date",
                                 cutoff="2020-12-31")
    assert new == legacy == {"L_hit", "L_boundary"}      # boundary in, late/at-cutoff/NA out


def test_string_event_value_dutch_style():
    panel = pd.DataFrame({
        "loan_id": ["D1", "D2", "D3"],
        "reporting_date": ["2025-03-31"] * 3,
        "default_crr_flag": ["Y", "N", None],
    })
    spec = LabelSpec(name="default_6m", type="forward_event", event_col="default_crr_flag",
                     horizon_months=6, event_value="Y")
    out = forward_event_entities(panel, spec, id_col="loan_id", time_col="reporting_date",
                                 cutoff="2024-12-31")
    assert out == {"D1"}                                 # 'N' and NA are not events


def test_observe_panel_gate_values_default_matches_legacy_boolean():
    panel = _panel()
    old_style = observe_panel(panel, "loan_id", "reporting_date", "2021-06-30", "is_performing")
    new_style = observe_panel(panel, "loan_id", "reporting_date", "2021-06-30", "is_performing",
                              gate_values=(True,))
    assert set(old_style["loan_id"]) == set(new_style["loan_id"])


def test_observe_panel_gate_value_list():
    panel = pd.DataFrame({
        "loan_id": ["A", "B", "C"],
        "reporting_date": ["2024-12-31"] * 3,
        "arrears_bucket": ["Performing", "90+ DPD", "1-29 DPD"],
    })
    obs = observe_panel(panel, "loan_id", "reporting_date", "2024-12-31", "arrears_bucket",
                        gate_values=("Performing", "1-29 DPD"))
    assert set(obs["loan_id"]) == {"A", "C"}             # the 90+ DPD loan is gated out


def test_resolve_label_spec_from_contract(tmp_path):
    ds = tmp_path / "dataset.yaml"
    ds.write_text("""
dataset: {name: toy, adapter: generic, id_col: loan_id, time_col: reporting_date,
          origination_col: origination_date}
labels:
  default_12m: {type: forward_event, event_col: default_event, horizon_months: 12,
                gate_col: is_performing}
leakage: [default_event, is_performing]
""")
    spec = resolve_label_spec({"dataset": str(ds), "task": {"label": "default_12m"}})
    assert spec.event_col == "default_event" and spec.horizon_months == 12
    # unknown name -> actionable error listing what exists
    with pytest.raises(ValueError, match="available.*default_12m"):
        resolve_label_spec({"dataset": str(ds), "task": {"label": "nope"}})


def test_resolve_label_spec_legacy_keys_with_deprecation(capsys):
    spec = resolve_label_spec({"task": {"label_col": "default_event", "horizon_months": 12,
                                        "gate_col": "is_performing"}})
    assert spec.event_col == "default_event" and spec.gate_values == (True,)
    assert "deprecated" in capsys.readouterr().out


def test_resolve_label_spec_needs_one_of_the_two_paths():
    with pytest.raises(ValueError, match="label"):
        resolve_label_spec({"task": {"cutoff": "2020-12-31"}})
    with pytest.raises(ValueError, match="horizon_months"):
        resolve_label_spec({"task": {"label_col": "default_event"}})


def test_committed_finetune_configs_resolve_from_the_contract():
    """The migrated Fannie finetune recipes must resolve default_12m without legacy keys."""
    from credit_fm.utils.config import load_config
    for recipe in ("configs/fannie_mae/finetune.yaml", "configs/fannie_mae/finetune_oot.yaml",
                   "configs/fannie_mae/finetune_crisis.yaml"):
        cfg = load_config(recipe)
        spec = resolve_label_spec(cfg)
        assert spec.name == "default_12m" and spec.horizon_months == 12
        assert spec.gate_col == "is_performing"
        assert "label_col" not in cfg.get("task", {})     # legacy keys fully removed


def test_prepay_task_is_pure_config():
    """★ G2.2 — the second task is one yaml line: label: prepay_12m (zero code)."""
    from credit_fm.utils.config import load_config
    cfg = load_config("configs/fannie_mae/finetune_prepay_oot.yaml")
    spec = resolve_label_spec(cfg)
    assert spec.name == "prepay_12m" and spec.event_col == "prepay_event"
    assert spec.horizon_months == 12 and spec.gate_col == "is_performing"
    assert cfg["train"]["neg_per_pos"] == 0               # prepay isn't rare — natural balance
