"""The auto-loan pack, and what makes it different from the mortgage one.

A second pack is only worth shipping if it exercises the engine differently.
These tests check the ways it does: collateral that falls in value rather than
rising, a balloon payment tied to the product type, contracts short enough that
maturity is reached inside the panel, and recovery booked on write-off.

They also pin the arithmetic that a naive pack gets wrong — a contract older
than its own term — because that produced negative balances the first time.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd import api

PACK = "auto_abs_esma_annex5"


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    result = api.run(PACK, 1200, tmp_path_factory.mktemp("auto"), seed=7)
    return api.load(PACK), result, pd.read_parquet(result["panel"])


def _opening(panel: pd.DataFrame) -> pd.DataFrame:
    return panel[panel.data_cut_off_date == panel.data_cut_off_date.min()]


def _closing(panel: pd.DataFrame) -> pd.DataFrame:
    return panel[panel.data_cut_off_date == panel.data_cut_off_date.max()]


# ---------------------------------------------------------------------------
# it loads, runs, and validates
# ---------------------------------------------------------------------------


def test_the_pack_is_a_valid_spec():
    checked = api.check(PACK)
    assert checked["valid"], checked["problems"]
    assert checked["spec"]["asset_class"] == "auto"
    assert checked["spec"]["title"] == "European Auto Loans — ESMA Annex 5"
    assert "Annex 5" in checked["spec"]["regulatory_template"]


def test_the_panel_passes_every_invariant_it_declares(run):
    _, result, _ = run
    failures = [c["name"] for c in result["validation"]["checks"] if not c["passed"]]
    assert result["validation"]["passed"], failures
    assert result["validation"]["total"] >= 20


def test_it_is_named_for_people_and_identified_for_machines():
    """The UI shows the title; `sdd run` takes the file stem."""
    assert PACK in api.list_packs()
    assert api.load(PACK).meta.name == "auto_abs_de"


# ---------------------------------------------------------------------------
# what makes an auto pool an auto pool
# ---------------------------------------------------------------------------


def test_the_collateral_depreciates_rather_than_appreciating(run):
    """The structural difference from a mortgage pool: a car loses value."""
    _, _, panel = run
    opening, closing = _opening(panel), _closing(panel)

    assert closing.current_valuation_amount.mean() < opening.current_valuation_amount.mean() * 0.85


def test_leverage_falls_as_the_contract_amortises(run):
    """Balances fall faster than values do, so an auto pool de-levers."""
    _, _, panel = run
    assert _closing(panel).current_ltv_pct.median() < _opening(panel).current_ltv_pct.median()


def test_a_balloon_belongs_to_the_product_that_has_one(run):
    """Sampling the flag independently would give PCP contracts with no balloon
    and hire purchase agreements with one — neither product exists."""
    _, _, panel = run
    pcp = panel.product_type == "Personal contract purchase"

    assert (panel.loc[pcp, "balloon_flag"] == "Y").all()
    assert (panel.loc[~pcp, "balloon_flag"] == "N").all()
    assert (panel.loc[pcp, "balloon_amount"] > 0).all()
    assert (panel.loc[~pcp, "balloon_amount"] == 0).all()


def test_recovery_is_booked_when_a_vehicle_is_sold(run):
    """A repossessed car is sold within months, so a 24-period panel sees it."""
    _, _, panel = run
    recovered = panel[panel.recovery_amount > 0]

    assert len(recovered) > 0
    assert (recovered.account_status == "Charged-Off").all()
    # Booked once per entity, not every period it sits in the state.
    assert recovered.unique_identifier.nunique() == len(recovered)


def test_contracts_are_short_enough_to_mature_inside_the_panel(run):
    """A 360-month mortgage barely moves over 24 cut-offs; a 48-month car loan
    is most of the way through its life."""
    _, _, panel = run
    opening = _opening(panel)
    assert opening.original_term_months.max() <= 72
    assert opening.remaining_term_months.median() < 60


# ---------------------------------------------------------------------------
# the arithmetic that a naive pack gets wrong
# ---------------------------------------------------------------------------


def test_no_contract_is_older_than_its_own_term(run):
    """Origination date and term are sampled independently, so a draw can make a
    24-month loan written five years ago. Past maturity the closed-form annuity
    balance goes negative, so seasoning is capped — and the origination date is
    recomputed to match, rather than left contradicting it."""
    _, _, panel = run
    opening = _opening(panel)

    assert (opening.seasoning_months < opening.original_term_months).all()
    assert (opening.seasoning_months >= 1).all()

    # The date and the seasoning still agree after the repair.
    implied = (2024 - opening.origination_year) * 12 + (1 - opening.origination_month)
    assert (implied == opening.seasoning_months).all()


def test_no_balance_or_amount_goes_negative(run):
    _, _, panel = run
    for column in api.load(PACK).validation.non_negative_columns:
        assert pd.to_numeric(panel[column], errors="coerce").min() >= 0, column


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stressed(tmp_path_factory):
    out = {}
    for scenario in ("base", "adverse", "severe"):
        result = api.run(
            PACK, 900, tmp_path_factory.mktemp(scenario), seed=7, periods=12, scenario=scenario
        )
        out[scenario] = (result, pd.read_parquet(result["panel"]))
    return out


def test_each_scenario_is_worse_than_the_last(stressed):
    distress = {}
    for scenario, (result, panel) in stressed.items():
        assert result["validation"]["passed"]
        closing = _closing(panel)
        distress[scenario] = closing.account_status.isin(
            ["31-60 DPD", "61-90 DPD", "Defaulted", "Charged-Off"]
        ).mean()

    assert distress["base"] < distress["adverse"] < distress["severe"]
    assert distress["severe"] > 0.10


def test_a_rate_shift_actually_moves_the_rate(stressed):
    """The scenario feature this pack is the first to use: `rate_shift` adds to
    a sampled rate column. It emits a derivation that reads its own target, which
    the loader used to reject outright."""
    base = _closing(stressed["base"][1]).current_interest_rate_pct.mean()
    adverse = _closing(stressed["adverse"][1]).current_interest_rate_pct.mean()
    severe = _closing(stressed["severe"][1]).current_interest_rate_pct.mean()

    assert adverse == pytest.approx(base + 1.0, abs=0.15)
    assert severe == pytest.approx(base + 2.5, abs=0.15)


def test_a_used_car_price_correction_reaches_the_collateral(stressed):
    """What auto ABS is actually sensitive to: recovery depends on resale value."""
    base = _closing(stressed["base"][1]).current_valuation_amount.mean()
    severe = _closing(stressed["severe"][1]).current_valuation_amount.mean()
    assert severe < base * 0.9
