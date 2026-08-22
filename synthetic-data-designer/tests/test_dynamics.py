"""Amortisation kernels, index overlays, counters, accruals.

Each kernel is checked against the closed-form arithmetic it implements, on
hand-built rows, so a wrong sign or a swapped operand shows up immediately.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.age.dynamics import (
    DynamicsError,
    amortise,
    annuity_payment,
    apply_accruals,
    apply_counters,
    apply_indices,
    index_multiplier,
    seed_accrual_counters,
)
from sdd.spec.schema import Accrual, Amortisation, Counter, Index


@pytest.fixture
def loans() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "balance": [100_000.0, 100_000.0, 100_000.0],
            "rate": [6.0, 6.0, 6.0],  # 0.5% a month
            "payment": [1000.0, 1000.0, 1000.0],
            "term": [120.0, 2.0, 1.0],
            "io": ["N", "Y", "N"],
            "value": [200_000.0, 200_000.0, 200_000.0],
        }
    )


ALL = np.ones(3, dtype=bool)


# ---------------------------------------------------------------------------
# amortisation kernels
# ---------------------------------------------------------------------------


def test_annuity_retires_the_payment_less_interest(loans):
    am = Amortisation(kind="annuity", balance="balance", rate="rate", payment="payment")
    got = amortise(loans, am, pays_mask=ALL, active_mask=ALL)
    # 100000 * 1.005 - 1000 = 99500
    np.testing.assert_allclose(got, [99_500.0] * 3)


def test_interest_only_never_amortises(loans):
    am = Amortisation(kind="interest_only", balance="balance")
    np.testing.assert_allclose(amortise(loans, am, pays_mask=ALL, active_mask=ALL), [100_000.0] * 3)


def test_flat_when_pins_selected_rows(loans):
    """Interest-only loans inside an otherwise amortising pool must not pay down."""
    am = Amortisation(
        kind="annuity", balance="balance", rate="rate", payment="payment", flat_when="io == 'Y'"
    )
    got = amortise(loans, am, pays_mask=ALL, active_mask=ALL)
    np.testing.assert_allclose(got, [99_500.0, 100_000.0, 99_500.0])


def test_non_payers_freeze_rather_than_amortise(loans):
    """The central modelling rule: no payment means no principal reduction."""
    am = Amortisation(kind="annuity", balance="balance", rate="rate", payment="payment")
    pays = np.array([True, False, False])
    got = amortise(loans, am, pays_mask=pays, active_mask=ALL)
    np.testing.assert_allclose(got, [99_500.0, 100_000.0, 100_000.0])


def test_linear_with_a_payment_column(loans):
    am = Amortisation(kind="linear", balance="balance", payment="payment")
    np.testing.assert_allclose(amortise(loans, am, pays_mask=ALL, active_mask=ALL), [99_000.0] * 3)


def test_linear_from_remaining_term(loans):
    am = Amortisation(kind="linear", balance="balance", term="term")
    got = amortise(loans, am, pays_mask=ALL, active_mask=ALL)
    # 1/120, 1/2, 1/1 of the balance retired
    np.testing.assert_allclose(got, [99_166.666667, 50_000.0, 0.0], rtol=1e-6)


def test_bullet_repays_only_at_maturity(loans):
    am = Amortisation(kind="bullet", balance="balance", term="term")
    got = amortise(loans, am, pays_mask=ALL, active_mask=ALL)
    np.testing.assert_allclose(got, [100_000.0, 100_000.0, 0.0])


def test_depreciation_decays_the_value(loans):
    am = Amortisation(kind="depreciation", balance="balance", rate_per_period=0.02)
    np.testing.assert_allclose(amortise(loans, am, pays_mask=ALL, active_mask=ALL), [98_000.0] * 3)


def test_revolving_can_grow_the_balance(loans):
    am = Amortisation(kind="revolving", balance="balance", rate_per_period=0.01)
    np.testing.assert_allclose(amortise(loans, am, pays_mask=ALL, active_mask=ALL), [101_000.0] * 3)


def test_balance_never_goes_below_the_floor():
    df = pd.DataFrame({"balance": [500.0], "rate": [0.0], "payment": [1000.0]})
    am = Amortisation(kind="annuity", balance="balance", rate="rate", payment="payment")
    assert amortise(df, am, pays_mask=np.array([True]), active_mask=np.array([True]))[0] == 0.0


def test_annuity_payment_matches_the_textbook_formula():
    # 200k at 6% over 360 months is about 1199.10 a month.
    got = annuity_payment(np.array([200_000.0]), np.array([6.0]), np.array([360.0]))
    assert abs(got[0] - 1199.10) < 0.5


def test_annuity_payment_handles_a_zero_rate():
    got = annuity_payment(np.array([120_000.0]), np.array([0.0]), np.array([120.0]))
    np.testing.assert_allclose(got, [1000.0])


# ---------------------------------------------------------------------------
# indices
# ---------------------------------------------------------------------------


def test_constant_drift_compounds_to_the_annual_rate():
    idx = Index(name="hpi", applies_to=["value"], kind="constant_drift", annual=0.03)
    m = index_multiplier(idx, 1, 12.0, np.random.default_rng(0))
    assert abs(m**12 - 1.03) < 1e-9


def test_series_index_repeats_its_last_value():
    idx = Index(name="hpi", applies_to=["value"], kind="series", series=[1.01, 1.02])
    rng = np.random.default_rng(0)
    assert index_multiplier(idx, 1, 12.0, rng) == 1.01
    assert index_multiplier(idx, 2, 12.0, rng) == 1.02
    assert index_multiplier(idx, 9, 12.0, rng) == 1.02  # past the end


def test_index_scales_every_named_column(loans):
    idx = Index(name="hpi", applies_to=["value"], kind="constant_drift", annual=0.0)
    out = apply_indices(loans.copy(), [idx], 1, 12.0, np.random.default_rng(0))
    np.testing.assert_allclose(out["value"], [200_000.0] * 3)


def test_scenario_shift_moves_the_drift_negative(loans):
    idx = Index(name="hpi", applies_to=["value"], kind="constant_drift", annual=0.03)
    out = apply_indices(
        loans.copy(), [idx], 1, 12.0, np.random.default_rng(0), annual_shift={"hpi": -0.13}
    )
    assert out["value"].iloc[0] < 200_000.0


def test_index_pointing_at_a_missing_column_is_an_error(loans):
    idx = Index(name="hpi", applies_to=["ghost"], kind="constant_drift", annual=0.03)
    with pytest.raises(DynamicsError, match="missing column 'ghost'"):
        apply_indices(loans.copy(), [idx], 1, 12.0, np.random.default_rng(0))


# ---------------------------------------------------------------------------
# counters
# ---------------------------------------------------------------------------


def test_counter_steps_and_clips(loans):
    out = apply_counters(loans.copy(), [Counter(column="term", step=-1, clip_min=0)])
    assert list(out["term"]) == [119, 1, 0]


def test_counter_expression_recomputes_from_other_columns():
    df = pd.DataFrame({"total": [360, 360], "used": [10, 20], "left": [0, 0]})
    out = apply_counters(df, [Counter(column="left", expr="total - used")])
    assert list(out["left"]) == [350, 340]


def test_counter_on_a_missing_column_is_an_error():
    with pytest.raises(DynamicsError, match="does not exist"):
        apply_counters(pd.DataFrame({"a": [1]}), [Counter(column="ghost", step=1)])


# ---------------------------------------------------------------------------
# accruals
# ---------------------------------------------------------------------------


def _accrual_df() -> pd.DataFrame:
    return pd.DataFrame({"payment": [900.0] * 3, "arrears": [0.0] * 3})


def test_arrears_accumulate_one_payment_per_period():
    acc = [Accrual(column="arrears", add="payment", when="not_performing")]
    df = _accrual_df()
    counters: dict[str, np.ndarray] = {}
    labels = np.array(["Late"] * 3, dtype=object)
    terminal = np.zeros(3, dtype=bool)

    for expected in (900.0, 1800.0, 2700.0):
        df, counters = apply_accruals(
            df,
            acc,
            state_labels=labels,
            terminal_mask=terminal,
            performing_state="Performing",
            counters=counters,
        )
        assert df["arrears"].iloc[0] == expected


def test_curing_resets_the_arrears_counter():
    acc = [Accrual(column="arrears", add="payment", when="not_performing")]
    df = _accrual_df()
    counters: dict[str, np.ndarray] = {}
    terminal = np.zeros(3, dtype=bool)

    df, counters = apply_accruals(
        df,
        acc,
        state_labels=np.array(["Late"] * 3, dtype=object),
        terminal_mask=terminal,
        performing_state="Performing",
        counters=counters,
    )
    df, counters = apply_accruals(
        df,
        acc,
        state_labels=np.array(["Performing"] * 3, dtype=object),
        terminal_mask=terminal,
        performing_state="Performing",
        counters=counters,
    )
    assert (df["arrears"] == 0.0).all()
    assert (counters["arrears"] == 0).all()


def test_terminal_rows_owe_nothing():
    """A redeemed loan owes nothing, whatever state it was in last month."""
    acc = [Accrual(column="arrears", add="payment", when="not_performing")]
    df, _counters = apply_accruals(
        _accrual_df(),
        acc,
        state_labels=np.array(["Redeemed"] * 3, dtype=object),
        terminal_mask=np.ones(3, dtype=bool),
        performing_state="Performing",
        counters={"arrears": np.array([5, 5, 5])},
    )
    assert (df["arrears"] == 0.0).all()


def test_seeding_recovers_missed_payments_from_days_past_due():
    """Without seeding, a loan starting 75 days late would jump from three
    payments owed to one at the first ageing step."""
    df = pd.DataFrame({"arrears": [0.0] * 4, "payment": [900.0] * 4, "dpd": [0, 15, 45, 75]})
    acc = [Accrual(column="arrears", add="payment", when="not_performing")]
    counters = seed_accrual_counters(df, acc, "dpd")
    assert list(counters["arrears"]) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# recovery
# ---------------------------------------------------------------------------


def test_recovery_is_booked_once_on_the_way_into_a_write_off():
    """A defaulted loan is not a total loss, but it is only recovered once.

    Without the "entering" test an absorbing state would book a recovery every
    period an entity sat in it, and a pool would recover several times what it
    lost.
    """
    from sdd.age.panel import apply_recovery

    spec = _recovery_spec(rate=0.4)
    df = pd.DataFrame({"balance": [100_000.0, 200_000.0, 300_000.0]})

    entering = apply_recovery(
        spec,
        df.copy(),
        previous=np.array(["Defaulted", "Defaulted", "Performing"], dtype=object),
        current=np.array(["Charged-Off", "Defaulted", "Performing"], dtype=object),
    )
    assert list(entering["recovery_amount"]) == [40_000.0, 0.0, 0.0]

    # Already there last period: nothing more is recovered.
    staying = apply_recovery(
        spec,
        df.copy(),
        previous=np.array(["Charged-Off"] * 3, dtype=object),
        current=np.array(["Charged-Off"] * 3, dtype=object),
    )
    assert list(staying["recovery_amount"]) == [0.0, 0.0, 0.0]


def _recovery_spec(rate: float):
    from sdd.spec import load_spec_dict

    return load_spec_dict(
        {
            "meta": {"name": "rec"},
            "entity": {
                "id_column": "id",
                "time_column": "asof",
                "calendar": {"start": "2024-01-31", "periods": 3},
            },
            "columns": [
                {"name": "id", "role": "static", "dtype": "str", "generator": {"kind": "sequence"}},
                {
                    "name": "asof",
                    "role": "dynamic",
                    "dtype": "str",
                    "generator": {"kind": "constant", "value": "2024-01-31"},
                },
                {
                    "name": "state",
                    "role": "dynamic",
                    "dtype": "category",
                    "generator": {"kind": "constant", "value": "Performing"},
                },
                {
                    "name": "balance",
                    "role": "dynamic",
                    "dtype": "float",
                    "generator": {"kind": "uniform", "low": 1, "high": 2},
                },
                {
                    "name": "recovery_amount",
                    "role": "dynamic",
                    "dtype": "float",
                    "generator": {"kind": "constant", "value": 0.0},
                },
            ],
            "lifecycle": {
                "state_column": "state",
                "states": ["Performing", "Defaulted", "Charged-Off"],
                "transition_states": ["Performing", "Defaulted"],
                "transitions": [[0.98, 0.02], [0.0, 1.0]],
                "absorbing": ["Defaulted"],
                "terminal": ["Charged-Off"],
                "hazards": [
                    {
                        "kind": "dwell_time",
                        "name": "writeoff",
                        "from_state": "Defaulted",
                        "to_state": "Charged-Off",
                        "periods": 3,
                    }
                ],
            },
            "dynamics": {
                "recovery": {
                    "rate": rate,
                    "balance": "balance",
                    "target": "recovery_amount",
                    "on_states": ["Charged-Off"],
                }
            },
        }
    )
