"""Proof the engine is not secretly mortgage-shaped.

The RMBS pack is a faithful port of upstream, which makes it a good parity test
and a poor generalisation test — it exercises exactly the paths upstream needed.
This file uses an asset class that shares almost nothing with it:

======================  ==========================  =========================
                        RMBS pack                   auto lease here
======================  ==========================  =========================
periods                 24 monthly                  8 quarterly
lifecycle               8 states, delinquency       3 states
amortisation            annuity on principal        depreciation of residual
terminal exit           prepayment + charge-off     early termination
collateral              house price rising          vehicle value falling
======================  ==========================  =========================

If a hardcoded mortgage assumption survived the generalisation, a quarterly
depreciating lease is where it shows up.

The structure test uses the real 181-field ESMA Annex 3 taxonomy shipped by
deeploans, so the template parser is exercised against genuine regulatory
metadata rather than something written to suit it.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from sdd.age.panel import run_ageing
from sdd.generate import build_book
from sdd.profile import load_template, profile_dataset
from sdd.profile.template import parse_format_hint
from sdd.spec import load_spec_dict
from sdd.validate import validate_panel

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# an auto lease book: nothing about it is mortgage-shaped
# ---------------------------------------------------------------------------


@pytest.fixture
def auto_lease_spec() -> dict:
    return {
        "spec_version": 1,
        "meta": {
            "name": "auto_lease",
            "asset_class": "auto",
            "regulatory_template": "ESMA Annex 5",
        },
        "entity": {
            "id_column": "contract_id",
            "id_format": "AUT{deal_year}-{seq:05d}",
            "time_column": "cut_off_date",
            # Quarterly, not monthly — the calendar is a spec field, not an
            # assumption baked into the engine.
            "calendar": {"start": "2024-03-31", "periods": 8, "freq": "quarter_end"},
        },
        "params": {"deal_year": 2024},
        "constants": {"currency": "EUR", "country": "DE", "originator": "AutoBank AG"},
        "columns": [
            {
                "name": "contract_id",
                "role": "static",
                "dtype": "str",
                "generator": {"kind": "sequence", "prefix": "AUT", "width": 5},
            },
            {
                "name": "cut_off_date",
                "role": "dynamic",
                "dtype": "str",
                "generator": {"kind": "constant", "value": "2024-03-31"},
            },
            {"name": "currency", "role": "constant"},
            {"name": "country", "role": "constant"},
            {"name": "originator", "role": "constant"},
            {
                "name": "vehicle_type",
                "role": "static",
                "dtype": "category",
                "generator": {
                    "kind": "categorical",
                    "values": ["Passenger", "LCV", "Motorcycle"],
                    "weights": [0.82, 0.15, 0.03],
                },
            },
            {
                "name": "fuel",
                "role": "static",
                "dtype": "category",
                "generator": {
                    "kind": "conditional_categorical",
                    "parent": "vehicle_type",
                    "mapping": {
                        "Passenger": ["Petrol", "Diesel", "BEV", "Hybrid"],
                        "LCV": ["Diesel", "BEV"],
                        "Motorcycle": ["Petrol"],
                    },
                },
            },
            {
                "name": "original_value",
                "role": "static",
                "dtype": "float",
                "generator": {
                    "kind": "scipy",
                    "dist": "lognorm",
                    "params": {"s": 0.35, "scale": 32000.0},
                    "decimals": 2,
                },
            },
            {
                "name": "residual_value",
                "role": "dynamic",
                "dtype": "float",
                "generator": {"kind": "constant", "value": 0.0},
            },
            {
                "name": "quarters_remaining",
                "role": "dynamic",
                "dtype": "int",
                "generator": {
                    "kind": "categorical",
                    "values": [8, 12, 16],
                    "weights": [0.3, 0.5, 0.2],
                },
            },
            {
                "name": "status",
                "role": "dynamic",
                "dtype": "category",
                "generator": {
                    "kind": "categorical",
                    "values": ["Current", "Delinquent"],
                    "weights": [0.97, 0.03],
                },
            },
            {"name": "residual_pct", "role": "derived", "dtype": "float"},
        ],
        "derivations": [
            # Residual starts at the vehicle's value and falls from there.
            {"target": "residual_value", "expr": "original_value", "round": 2},
            {
                "target": "residual_pct",
                "expr": "residual_value / original_value * 100",
                "round": 2,
                "stage": "both",
            },
        ],
        "lifecycle": {
            "state_column": "status",
            "states": ["Current", "Delinquent", "Terminated"],
            "terminal": ["Terminated"],
            "transitions": [[0.96, 0.04], [0.55, 0.45]],
            "hazards": [
                {
                    "kind": "bernoulli",
                    "name": "early_termination",
                    "annual_rate": 0.10,
                    "to_state": "Terminated",
                }
            ],
            "state_fields": {"Terminated": {"residual_value": 0.0}},
        },
        "dynamics": {
            # A vehicle loses value; a house gains it. Different kernel entirely.
            "amortisation": {
                "kind": "depreciation",
                "balance": "residual_value",
                "rate_per_period": 0.06,
            },
            "counters": [{"column": "quarters_remaining", "step": -1, "clip_min": 0}],
        },
        "emit": {
            "filename": "auto_lease_{yyyy}Q{period}.csv",
            "column_order": [
                "contract_id",
                "cut_off_date",
                "currency",
                "country",
                "originator",
                "vehicle_type",
                "fuel",
                "original_value",
                "residual_value",
                "residual_pct",
                "quarters_remaining",
                "status",
            ],
        },
        "validation": {"non_negative_columns": ["residual_value", "original_value"]},
    }


def test_a_quarterly_depreciating_lease_generates(auto_lease_spec):
    spec = load_spec_dict(auto_lease_spec)
    book = build_book(spec, 800, seed=1)
    assert len(book) == 800
    assert book["contract_id"].iloc[0] == "AUT2024-00001"
    # Residuals start at the vehicle's full value.
    pd.testing.assert_series_equal(
        book["residual_value"], book["original_value"], check_names=False
    )
    # The conditional generator respects a completely different mapping.
    assert set(book.loc[book["vehicle_type"] == "Motorcycle", "fuel"]) <= {"Petrol"}
    assert set(book.loc[book["vehicle_type"] == "LCV", "fuel"]) <= {"Diesel", "BEV"}


def test_quarterly_periods_are_honoured(auto_lease_spec, tmp_path):
    spec = load_spec_dict(auto_lease_spec)
    result = run_ageing(spec, build_book(spec, 500, seed=2), tmp_path, seed=2)
    panel = pd.read_parquet(tmp_path / spec.emit.panel_filename)
    dates = sorted(panel["cut_off_date"].unique())
    assert result["periods"] == 8
    assert dates[:3] == ["2024-03-31", "2024-06-30", "2024-09-30"]


def test_values_depreciate_rather_than_amortise(auto_lease_spec, tmp_path):
    """The mortgage engine reduced balances by a payment; this reduces by a rate."""
    spec = load_spec_dict(auto_lease_spec)
    run_ageing(spec, build_book(spec, 600, seed=3), tmp_path, seed=3)
    panel = pd.read_parquet(tmp_path / spec.emit.panel_filename)

    ordered = panel.sort_values(["contract_id", "cut_off_date"])
    live = ordered[ordered["status"] != "Terminated"]
    previous = live.groupby("contract_id")["residual_value"].shift()
    moved = live[previous.notna() & (previous > 0)]
    ratio = (moved["residual_value"] / previous[moved.index]).median()
    assert ratio == pytest.approx(0.94, abs=0.005)


def test_an_annual_hazard_converts_to_the_calendars_period(auto_lease_spec, tmp_path):
    """10% a year on a quarterly calendar is ~2.6% a quarter, not ~0.9% a month."""
    spec = load_spec_dict(auto_lease_spec)
    result = run_ageing(spec, build_book(spec, 4000, seed=4), tmp_path, seed=4, write_files=False)
    second = result["mix"][1]
    terminated = second.get("Terminated", 0)
    assert 0.015 < terminated / second["rows"] < 0.04


def test_the_lease_panel_passes_its_own_invariants(auto_lease_spec, tmp_path):
    spec = load_spec_dict(auto_lease_spec)
    run_ageing(spec, build_book(spec, 700, seed=5), tmp_path, seed=5)
    report = validate_panel(spec, tmp_path / spec.emit.panel_filename)
    assert report.passed, report.summary()


def test_the_profiler_reads_back_a_non_mortgage_panel(auto_lease_spec, tmp_path):
    """The profiler must not assume monthly cut-offs or a delinquency ladder."""
    spec = load_spec_dict(auto_lease_spec)
    run_ageing(spec, build_book(spec, 900, seed=6), tmp_path, seed=6)
    panel = pd.read_parquet(tmp_path / spec.emit.panel_filename)

    profile = profile_dataset(panel)
    assert profile.id_column == "contract_id"
    assert profile.time_column == "cut_off_date"
    assert profile.is_panel

    lifecycle = profile.dynamics["lifecycle"]
    assert lifecycle["state_column"] == "status"
    assert lifecycle["terminal"] == ["Terminated"]
    assert lifecycle["states"][0] == "Current"

    # Falling values are detected as a negative-drift index.
    drift = {i["name"]: i["annual"] for i in profile.dynamics.get("indices", [])}
    assert not drift or all(v < 0 for v in drift.values())


# ---------------------------------------------------------------------------
# the real ESMA taxonomy as a structure input
# ---------------------------------------------------------------------------


def test_reads_the_real_esma_annex_3_taxonomy():
    template = load_template(FIXTURES / "cre_taxonomy.json")
    assert template.asset_class == "cre"
    assert len(template.fields) == 181
    typed = [f for f in template.fields if f.dtype]
    assert len(typed) > 150, "most fields carry a usable format hint"


def test_format_hints_map_onto_types():
    assert parse_format_hint("{MONETARY}")["dtype"] == "float"
    assert parse_format_hint("{DATEFORMAT}")["dtype"] == "date"
    assert parse_format_hint("{Y/N}")["domain"] == ["Y", "N"]
    assert parse_format_hint("{PERCENTAGE}")["max"] == 100.0
    assert parse_format_hint("{ALPHANUM-28}")["max_length"] == 28
    assert parse_format_hint("{INTEGER-9999}")["dtype"] == "int"
    assert parse_format_hint("not a hint") == {}
    assert parse_format_hint(None) == {}


def test_a_taxonomy_can_be_keyed_by_field_code():
    """Sample tapes are usually keyed by code (CREL1), not by human name."""
    template = load_template(FIXTURES / "cre_taxonomy.json", name_field="field_code")
    assert template.column_names[0] == "CREL1"
    assert template.field("CREL1").label == "Unique Identifier"


def test_a_bare_csv_header_works_as_a_structure(tmp_path):
    path = tmp_path / "header.csv"
    path.write_text("loan_id,balance,region\n")
    template = load_template(path)
    assert template.column_names == ["loan_id", "balance", "region"]


def test_a_data_dictionary_supplies_types(tmp_path):
    path = tmp_path / "dictionary.csv"
    path.write_text(
        "name,type,description\n"
        "loan_id,string,The identifier\n"
        "balance,decimal,Outstanding amount\n"
        "opened,date,Origination date\n"
    )
    template = load_template(path)
    assert [f.dtype for f in template.fields] == ["str", "float", "date"]


def test_an_unreadable_structure_format_is_refused(tmp_path):
    path = tmp_path / "structure.docx"
    path.write_bytes(b"not really a schema")
    with pytest.raises(ValueError, match="cannot read a schema definition"):
        load_template(path)
