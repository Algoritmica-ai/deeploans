"""Learning how entities leave the pool.

The transition matrix covers the states an entity sits in. Leaving is a hazard,
and the profiler used to emit exactly two whatever the data held: one flat rate
into the first terminal state, and one write-off delay fixed at nine periods
regardless of what the panel showed.

A book with four ways out therefore came back with two of them unreachable, the
loader refused the spec, and `/api/analyse` returned a 500 — so any tape where
loans are sold, mature, or recover from default could not be uploaded at all.
Most commercial lending data looks like that.
"""

from __future__ import annotations

import pathlib
import tempfile

import pandas as pd
import pytest

from sdd import api
from sdd.profile import build_spec, profile_dataset

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="module")
def learned_exits():
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, 400, tmp, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    profile = profile_dataset(
        panel,
        id_column="facility_id",
        time_column="reporting_date",
        state_column="credit_state",
    )
    return {e["to_state"]: e for e in profile.dynamics["lifecycle"]["exits"]}


def test_every_terminal_state_gets_a_rule(learned_exits):
    """The whole point: nothing may be left unreachable."""
    assert set(learned_exits) == {"Prepaid", "Sold", "Recovered", "Matured"}


def test_a_flat_chance_is_recovered_as_a_rate(learned_exits):
    """The pack prepays at 22% a year and trades at 10%."""
    assert learned_exits["Prepaid"]["kind"] == "bernoulli"
    assert learned_exits["Prepaid"]["annual_rate"] == pytest.approx(0.22, abs=0.04)

    assert learned_exits["Sold"]["kind"] == "bernoulli"
    assert learned_exits["Sold"]["annual_rate"] == pytest.approx(0.10, abs=0.04)


def test_a_fixed_delay_is_recognised_as_one(learned_exits):
    """Recovery is a nine-period workout, not a monthly chance.

    Told apart by shape: a delay puts a spike in the dwell distribution because
    it always fires on the same period, while a flat chance spreads thin.
    """
    recovered = learned_exits["Recovered"]
    assert recovered["kind"] == "dwell_time"
    assert recovered["from_state"] == "Defaulted"
    assert recovered["periods"] == 9, "the pack configures a nine-period workout"


def test_the_dwell_period_is_not_off_by_one(learned_exits):
    """The row that moves is the last one *in* the state.

    Its dwell counter reads one short of the period the hazard fires on, so the
    raw modal value would shorten every workout by a month on each round trip.
    """
    assert learned_exits["Recovered"]["periods"] == 9
    assert "9 periods" in learned_exits["Recovered"]["evidence"]


def test_each_rule_carries_its_evidence(learned_exits):
    """A learned number nobody can check is worse than no number."""
    for exit_rule in learned_exits.values():
        assert exit_rule["evidence"], f"{exit_rule['to_state']} has no evidence"
        assert any(ch.isdigit() for ch in exit_rule["evidence"])


@pytest.mark.parametrize("pack", ["clo_eu_leveraged_loans", "rmbs_nl_green_lion"])
def test_a_relearned_spec_is_valid_and_runs(tmp_path, pack):
    """The round trip the spec-driven design rests on.

    Before this the CLO half raised `states ['Recovered', 'Sold'] can never be
    reached`.
    """
    base = api.load(pack)
    result = api.run(pack, 300, tmp_path / pack, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    learned, _ = build_spec(
        panel,
        name=f"re_{pack}",
        id_column=base.entity.id_column,
        time_column=base.entity.time_column,
        state_column=base.lifecycle.state_column,
    )
    dumped = learned.model_dump(mode="json", exclude_none=True, by_alias=True)

    check = api.check(dumped)
    assert check["valid"], check["problems"][:3]

    regenerated = api.run(dumped, 200, tmp_path / f"out_{pack}", seed=5, validate_output=False)
    assert regenerated["total_rows"] > 0


def test_a_condition_hazard_comes_back_as_a_condition(learned_exits):
    """Maturity is driven by a column, and now comes back that way.

    This test used to assert the opposite, and said so: a condition hazard was
    relearned as a flat monthly rate, which made the state reachable and the
    spec runnable but was a different rule from the one that produced the data.
    The limitation was recorded here rather than assumed away, with a note
    asking whoever lifted it to come back and change this.

    Lifted. The rule is recovered expression and all, which matters because the
    two are not interchangeable: a flat rate gives a 96-month facility the same
    chance of maturing in month three as a 60-month one.
    """
    matured = learned_exits["Matured"]
    assert matured["kind"] == "condition"
    assert matured["when"] == "months_to_maturity <= 1"
