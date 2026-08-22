"""Turning portfolio rates into engine settings.

The translation has to work in both directions and agree with itself: a rate
read out of a matrix, then written back unchanged, must leave the matrix
producing the same rate. Everything else here is the arithmetic that makes the
configure form's three sliders mean what they say.
"""

from __future__ import annotations

import pytest

from sdd.age.calibrate import (
    apply_rates,
    default_states,
    implied_default_rate,
    implied_prepayment_rate,
    prepayment_hazard,
    rates,
    scale_worsening,
    set_default_rate,
    set_prepayment_rate,
    set_recovery_rate,
)
from sdd.api import load
from sdd.spec import load_spec_dict

PACK = "rmbs_nl_green_lion"


@pytest.fixture
def pack():
    return load(PACK)


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------


def test_the_default_rate_read_from_a_pack_is_a_first_year_cumulative(pack):
    """Not the one-period probability, which is roughly twelve times smaller."""
    annual = implied_default_rate(pack)
    one_step = 1 - pack.lifecycle.transitions[0][0]

    assert 0.0 < annual < 0.5
    assert annual > one_step / 2


def test_default_states_are_the_ones_a_loan_cannot_recover_from(pack):
    assert "Defaulted" in default_states(pack)
    assert "Charged-Off" in default_states(pack)
    assert "Redeemed" not in default_states(pack)


def test_the_prepayment_hazard_is_the_one_that_ends_a_loan_healthily(pack):
    hazard = prepayment_hazard(pack)
    assert hazard is not None
    assert hazard.to_state == "Redeemed"
    assert implied_prepayment_rate(pack) == pytest.approx(hazard.annual_rate)


def test_rates_reports_all_three_for_the_form(pack):
    reported = rates(pack)
    assert set(reported) == {"default_rate", "prepayment_rate", "recovery_rate"}
    assert reported["recovery_rate"] is None, "the pack books no recovery yet"


# ---------------------------------------------------------------------------
# writing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("target", [0.01, 0.03, 0.10, 0.25])
def test_a_requested_default_rate_is_actually_achieved(pack, target):
    out, result = set_default_rate(pack, target)
    assert result["achieved"] == pytest.approx(target, abs=0.002)
    assert implied_default_rate(out) == pytest.approx(target, abs=0.002)


def test_setting_a_rate_leaves_the_matrix_a_valid_matrix(pack):
    out, _ = set_default_rate(pack, 0.15)
    for row in out.lifecycle.transitions:
        assert sum(row) == pytest.approx(1.0, abs=1e-9)
        assert all(p >= 0 for p in row)
    # And the loader agrees, which is the check that actually matters.
    load_spec_dict(out.model_dump(mode="json", by_alias=True))


def test_reading_then_writing_the_same_rate_is_a_no_op(pack):
    current = implied_default_rate(pack)
    out, _ = set_default_rate(pack, current)
    assert implied_default_rate(out) == pytest.approx(current, abs=0.002)


def test_scaling_the_worsening_flow_preserves_row_sums():
    matrix = [[0.9, 0.08, 0.02], [0.3, 0.5, 0.2], [0.0, 0.0, 1.0]]
    scaled = scale_worsening(matrix, 3.0)

    assert all(sum(row) == pytest.approx(1.0) for row in scaled)
    # More weight on getting worse, less on staying put.
    assert scaled[0][1] + scaled[0][2] > matrix[0][1] + matrix[0][2]
    assert scaled[0][0] < matrix[0][0]
    # An absorbing row has nothing worse to move to, so it is left alone.
    assert scaled[2] == matrix[2]


def test_a_prepayment_rate_is_written_as_the_hazard_the_engine_reads(pack):
    out, result = set_prepayment_rate(pack, 0.12)
    assert result["achieved"] == pytest.approx(0.12)
    assert prepayment_hazard(out).annual_rate == pytest.approx(0.12)
    assert prepayment_hazard(out).period_rate is None


def test_recovery_creates_the_column_it_needs(pack):
    assert pack.column("recovery_amount") is None

    out, _ = set_recovery_rate(pack, 0.4)
    assert out.dynamics.recovery.rate == 0.4
    assert out.dynamics.recovery.balance == pack.dynamics.amortisation.balance
    assert out.column("recovery_amount") is not None
    assert "recovery_amount" in out.emit.column_order
    # A number nothing records is not a setting: the column must be emitted.
    load_spec_dict(out.model_dump(mode="json", by_alias=True))


def test_recovery_is_booked_only_when_a_loan_writes_off(pack):
    out, _ = set_recovery_rate(pack, 0.4)
    assert set(out.dynamics.recovery.on_states) <= set(pack.lifecycle.terminal)


# ---------------------------------------------------------------------------
# the form's contract
# ---------------------------------------------------------------------------


def test_a_rate_that_cannot_apply_is_reported_not_raised():
    """A spec without a lifecycle should lose one box, not the whole form."""
    spec = load_spec_dict(
        {
            "meta": {"name": "flat"},
            "entity": {
                "id_column": "id",
                "time_column": "asof",
                "calendar": {"start": "2024-01-31", "periods": 1},
            },
            "columns": [
                {
                    "name": "id",
                    "role": "static",
                    "dtype": "str",
                    "generator": {"kind": "sequence"},
                },
                {
                    "name": "asof",
                    "role": "dynamic",
                    "dtype": "str",
                    "generator": {"kind": "constant", "value": "2024-01-31"},
                },
            ],
        }
    )
    out, notes = apply_rates(spec, default_rate=0.05, recovery_rate=0.3)

    assert out is spec or out.lifecycle is None
    assert len(notes) == 2
    assert all("not applied" in n for n in notes)


def test_apply_rates_reports_what_each_setting_achieved(pack):
    _, notes = apply_rates(pack, default_rate=0.06, prepayment_rate=0.09, recovery_rate=0.35)
    assert any("default rate set to 6" in n for n in notes)
    assert any("prepayment rate set to 9" in n for n in notes)
    assert any("recovery rate set to 35" in n for n in notes)
