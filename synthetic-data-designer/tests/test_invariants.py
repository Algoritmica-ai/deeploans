"""Invariant checks, and proof that they can fail.

Every check gets two tests: it passes on a clean panel, and it fails on a panel
corrupted in exactly the way that check exists to catch. A validator that has
never been seen to fail is not evidence of anything.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd.age.panel import run_ageing
from sdd.generate import build_book
from sdd.spec import load_spec, load_spec_dict
from sdd.validate import validate_panel

N = 800
SEED = 3


@pytest.fixture(scope="module")
def spec():
    from tests.conftest import PACKS

    return load_spec(PACKS / "rmbs_nl_green_lion.yaml")


@pytest.fixture(scope="module")
def clean_panel(spec, tmp_path_factory) -> pd.DataFrame:
    out = tmp_path_factory.mktemp("inv")
    book = build_book(spec, N, seed=SEED)
    run_ageing(spec, book, out, seed=SEED)
    return pd.read_parquet(out / spec.emit.panel_filename)


def family(report, name: str) -> list:
    """Every check in a family. Per-column checks are named ``family::column``."""
    found = [c for c in report.checks if c.name == name or c.name.startswith(f"{name}::")]
    if not found:
        raise AssertionError(f"no check named {name!r}; got {[c.name for c in report.checks]}")
    return found


def check(report, name: str):
    """The one check in a family that matters.

    When a family has a failing member that is the interesting one — asserting
    on an arbitrary passing sibling would make a negative control vacuous.
    """
    found = family(report, name)
    failed = [c for c in found if not c.passed]
    return failed[0] if failed else found[0]


# ---------------------------------------------------------------------------
# clean panel
# ---------------------------------------------------------------------------


def test_clean_panel_passes_everything(spec, clean_panel):
    report = validate_panel(spec, clean_panel)
    assert report.passed, report.summary()
    assert len(report.checks) > 15


def test_report_serialises_for_the_api(spec, clean_panel):
    payload = validate_panel(spec, clean_panel).to_dict()
    assert payload["passed"] is True
    assert payload["failed"] == 0
    assert isinstance(payload["checks"], list)


def test_summary_says_so_when_everything_passes(spec, clean_panel):
    assert "consistent with the spec" in validate_panel(spec, clean_panel).summary()


# ---------------------------------------------------------------------------
# negative controls — one corruption per check
# ---------------------------------------------------------------------------


def test_catches_a_duplicated_row(spec, clean_panel):
    corrupt = pd.concat([clean_panel, clean_panel.head(1)], ignore_index=True)
    report = validate_panel(spec, corrupt)
    assert not report.passed
    assert not check(report, "ids_unique_per_period").passed


def test_catches_a_loan_appearing_mid_panel(spec, clean_panel):
    """A closed pool cannot acquire new loans after the first cut-off."""
    intruder = clean_panel[clean_panel["reporting_date"] == "2024-06-30"].head(1).copy()
    intruder["loan_id"] = "GL2024_999999"
    report = validate_panel(spec, pd.concat([clean_panel, intruder], ignore_index=True))
    assert not check(report, "closed_pool").passed


def test_catches_a_static_column_drifting(spec, clean_panel):
    corrupt = clean_panel.copy()
    target = corrupt.index[corrupt["reporting_date"] == "2024-08-31"][0]
    corrupt.loc[target, "province"] = (
        "Zeeland" if corrupt.loc[target, "province"] != "Zeeland" else "Utrecht"
    )
    report = validate_panel(spec, corrupt)
    assert not report.passed
    assert not check(report, "static_stable").passed


def test_catches_a_loan_surviving_a_terminal_state(spec, clean_panel):
    """A redeemed loan cannot come back."""
    corrupt = clean_panel.copy()
    redeemed = corrupt[corrupt["arrears_bucket"] == "Redeemed"]
    assert len(redeemed) > 0
    row = redeemed.iloc[0].copy()
    row["reporting_date"] = "2025-12-31"
    row["arrears_bucket"] = "Performing"
    report = validate_panel(spec, pd.concat([corrupt, pd.DataFrame([row])], ignore_index=True))
    assert not check(report, "terminal_states_absorb").passed


def test_catches_a_state_field_not_being_applied(spec, clean_panel):
    """A defaulted loan must carry days_past_due 200, per the spec."""
    corrupt = clean_panel.copy()
    idx = corrupt.index[corrupt["arrears_bucket"] == "Defaulted"]
    assert len(idx) > 0
    corrupt.loc[idx[0], "days_past_due"] = 7
    report = validate_panel(spec, corrupt)
    assert not report.passed
    assert not check(report, "state_fields").passed


def test_catches_a_counter_skipping(spec, clean_panel):
    corrupt = clean_panel.copy()
    idx = corrupt.index[corrupt["reporting_date"] == "2024-09-30"][0]
    corrupt.loc[idx, "seasoning_months"] += 5
    report = validate_panel(spec, corrupt)
    assert not check(report, "counter_step").passed


def test_catches_a_value_outside_its_domain(spec, clean_panel):
    corrupt = clean_panel.copy()
    corrupt.loc[corrupt.index[0], "nhg_flag"] = "MAYBE"
    report = validate_panel(spec, corrupt)
    assert not report.passed
    assert not check(report, "domain").passed


def test_catches_a_negative_balance(spec, clean_panel):
    corrupt = clean_panel.copy()
    corrupt.loc[corrupt.index[0], "current_balance"] = -1.0
    report = validate_panel(spec, corrupt)
    assert not check(report, "non_negative_columns").passed


def test_failure_summary_names_the_broken_check(spec, clean_panel):
    corrupt = clean_panel.copy()
    corrupt.loc[corrupt.index[0], "current_balance"] = -1.0
    text = validate_panel(spec, corrupt).summary()
    assert "FAIL" in text and "non_negative_columns" in text


# ---------------------------------------------------------------------------
# reading from disk, custom checks, and toggles
# ---------------------------------------------------------------------------


def test_validates_a_parquet_path_as_well_as_a_frame(spec, tmp_path):
    book = build_book(spec, 300, seed=9)
    run_ageing(spec, book, tmp_path, seed=9)
    report = validate_panel(spec, tmp_path / spec.emit.panel_filename)
    assert report.passed, report.summary()


def test_custom_sql_check_runs(minimal_spec_dict, spec, clean_panel):
    from sdd.spec.schema import CustomInvariant

    tweaked = spec.model_copy(deep=True)
    tweaked.validation.custom = [
        CustomInvariant(
            name="no_giant_loans",
            description="No loan exceeds EUR 5m.",
            sql="SELECT loan_id FROM panel WHERE original_balance > 5000000",
        )
    ]
    assert check(validate_panel(tweaked, clean_panel), "custom").passed


def test_custom_sql_check_can_fail(spec, clean_panel):
    from sdd.spec.schema import CustomInvariant

    tweaked = spec.model_copy(deep=True)
    tweaked.validation.custom = [
        CustomInvariant(
            name="impossible", sql="SELECT loan_id FROM panel WHERE original_balance > 0"
        )
    ]
    assert not validate_panel(tweaked, clean_panel).passed


def test_a_broken_custom_query_is_reported_not_raised(spec, clean_panel):
    from sdd.spec.schema import CustomInvariant

    tweaked = spec.model_copy(deep=True)
    tweaked.validation.custom = [
        CustomInvariant(name="typo", sql="SELECT nonexistent_column FROM panel")
    ]
    result = check(validate_panel(tweaked, clean_panel), "custom")
    assert not result.passed
    assert result.error and "query failed" in result.error


def test_toggling_a_check_off_skips_it(spec, clean_panel):
    tweaked = spec.model_copy(deep=True)
    tweaked.validation.checks.non_negative_balances = False
    corrupt = clean_panel.copy()
    corrupt.loc[corrupt.index[0], "current_balance"] = -1.0
    names = [c.name for c in validate_panel(tweaked, corrupt).checks]
    assert "non_negative_columns" not in names


def test_works_on_a_spec_with_no_lifecycle(minimal_spec_dict, tmp_path):
    """A one-shot table with no ageing should still validate."""
    raw = dict(minimal_spec_dict)
    raw.pop("lifecycle")
    raw["dynamics"] = {}
    spec = load_spec_dict(raw)
    book = build_book(spec, 50, seed=1)
    report = validate_panel(spec, book[spec.output_columns()])
    assert report.passed, report.summary()


def test_column_names_with_awkward_characters_are_quoted(spec, clean_panel):
    """State labels like '90+ DPD' and '1-29 DPD' must not break the SQL."""
    report = validate_panel(spec, clean_panel)
    names = [c.name for c in report.checks]
    assert "state_fields::90+ DPD" in names
    assert "state_fields::1-29 DPD" in names
    assert all(c.error is None for c in report.checks), [
        (c.name, c.error) for c in report.checks if c.error
    ]
