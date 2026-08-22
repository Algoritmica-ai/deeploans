"""An open pool: entities joining after the first cut-off.

A closed pool holds every entity at period 0 and only shrinks. That is right for
a static securitisation and wrong for a lender's book, a revolving deal, or any
portfolio observed across a window in which it kept lending. These tests pin the
three things that have to hold once new entities can arrive: they arrive when the
spec says, they cannot collide with entities that already exist, and the
validator stops asserting the pool is closed.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd import api
from sdd.age.calibrate import rates
from sdd.profile import build_spec
from sdd.spec import SpecError, load_spec_dict

PACK = "rmbs_nl_green_lion"


def _spec(**originations) -> dict:
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 6
    if originations:
        spec["originations"] = originations
    return spec


@pytest.fixture(scope="module")
def open_run(tmp_path_factory):
    """One open-pool run, reused by the tests that only read its output."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"] = {"start": "2024-12-31", "periods": 14, "freq": "month_end"}
    spec["originations"] = {"rate": 0.04, "fresh": True}
    result = api.run(spec, 400, tmp_path_factory.mktemp("open"), seed=5)
    return spec, result, pd.read_parquet(result["panel"])


# ---------------------------------------------------------------------------
# the count and the window
# ---------------------------------------------------------------------------


def test_new_entities_appear_at_every_cut_off_after_the_first(open_run):
    _, result, _panel = open_run
    per_period = [m["originated"] for m in result["mix"]]

    assert per_period[0] == 0, "the opening book is not an origination"
    assert all(n == 16 for n in per_period[1:]), per_period
    assert result["originated"] == 16 * 13
    assert result["total_entities"] == 400 + result["originated"]


def test_the_panel_spans_the_calendar_year_the_run_crosses(open_run):
    """A run starting in December must hold loans written in the years after it."""
    _, _, panel = open_run
    first = set(panel[panel.reporting_date == panel.reporting_date.min()].loan_id)
    arrivals = panel[~panel.loan_id.isin(first)]
    years = sorted(arrivals.reporting_date.str[:4].unique())

    assert panel.reporting_date.min().startswith("2024-12")
    assert years == ["2025", "2026"], years


def test_a_rate_scales_with_the_opening_book(tmp_path):
    """A rate means the same thing at any size; a count would not."""
    small = api.run(_spec(rate=0.05), 200, tmp_path / "small", seed=1)
    large = api.run(_spec(rate=0.05), 800, tmp_path / "large", seed=1)

    assert small["mix"][1]["originated"] == 10
    assert large["mix"][1]["originated"] == 40


def test_a_fixed_count_is_honoured_exactly(tmp_path):
    result = api.run(_spec(per_period=7), 300, tmp_path, seed=1)
    assert [m["originated"] for m in result["mix"]][1:] == [7] * 5


def test_the_window_bounds_when_entities_may_join(tmp_path):
    result = api.run(_spec(rate=0.1, start_period=3, end_period=4), 200, tmp_path, seed=1)
    assert [m["originated"] for m in result["mix"]] == [0, 0, 0, 20, 20, 0]


def test_a_pool_can_grow_rather_than_only_shrink(open_run):
    _, result, _ = open_run
    rows = [m["rows"] for m in result["mix"]]
    assert rows[-1] > rows[0], "an open pool taking on 4% a period should outgrow attrition"


# ---------------------------------------------------------------------------
# what the new entities look like
# ---------------------------------------------------------------------------


def _arrivals(panel: pd.DataFrame) -> pd.DataFrame:
    """The first row of every entity that was not there at the first cut-off."""
    first = set(panel[panel.reporting_date == panel.reporting_date.min()].loan_id)
    later = panel[~panel.loan_id.isin(first)]
    return later.sort_values("reporting_date").groupby("loan_id").head(1)


def test_identifiers_never_collide_with_entities_that_already_exist(open_run):
    _, _, panel = open_run
    per_period = panel.groupby("reporting_date")["loan_id"].agg(["nunique", "size"])
    assert (per_period["nunique"] == per_period["size"]).all()
    assert panel.loan_id.nunique() == 400 + 16 * 13


def test_new_entities_enter_performing(open_run):
    _, _, panel = open_run
    assert set(_arrivals(panel).arrears_bucket) == {"Performing"}


def test_a_newly_written_loan_arrives_with_its_balance_intact(tmp_path):
    """A loan written this month has not also spent years paying down.

    Measured against the opening book, which *is* seasoned: the arrivals should
    sit far closer to their original balance than the loans already in the pool.
    """
    configured = api.configure(PACK, periods=10, origination_rate=0.05)
    panel = pd.read_parquet(api.run(configured["spec"], 300, tmp_path, seed=4)["panel"])

    opening = panel[panel.reporting_date == panel.reporting_date.min()]
    arrivals = _arrivals(panel)

    assert (arrivals.current_balance / arrivals.original_balance).min() > 0.99
    assert (opening.current_balance / opening.original_balance).median() < 0.95


def test_fresh_entities_carry_the_origination_date_of_the_period_they_join(tmp_path):
    """Resetting the date a derivation reads is what makes a loan genuinely new.

    Zeroing the seasoning column directly would not work: it is derived, so the
    derivation recomputes it from the origination date on the next line.
    """
    configured = api.configure(PACK, periods=8, origination_rate=0.05)
    assert configured["spec"]["originations"]["reset_expr"] == {"origination_year": "period_year"}

    result = api.run(configured["spec"], 300, tmp_path, seed=2)
    arrivals = _arrivals(pd.read_parquet(result["panel"]))
    assert arrivals.seasoning_months.max() <= 1
    assert set(arrivals.origination_year) <= {2024, 2025}


def test_acquired_entities_keep_the_seasoning_they_were_sampled_with(tmp_path):
    """`fresh: false` is a pool buying seasoned loans, not writing new ones."""
    result = api.run(_spec(rate=0.05, fresh=False), 300, tmp_path, seed=2)
    arrivals = _arrivals(pd.read_parquet(result["panel"]))
    assert arrivals.seasoning_months.median() > 1


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def test_the_closed_pool_check_is_replaced_not_merely_dropped(open_run):
    _, result, _ = open_run
    names = {c["name"] for c in result["validation"]["checks"]}

    assert "closed_pool" not in names
    assert {"entity_spans_contiguous", "origination_window"} <= names
    assert result["validation"]["passed"], [
        c["name"] for c in result["validation"]["checks"] if not c["passed"]
    ]


def test_a_gap_in_an_entitys_history_is_caught(open_run, tmp_path):
    """The open-pool counterpart to closed_pool has to be able to fail."""
    from sdd.validate import validate_panel

    spec, _, panel = open_run
    loaded = api.load(spec)
    victim = panel.loan_id.iloc[0]
    middle = sorted(panel.reporting_date.unique())[3]
    holed = panel[~((panel.loan_id == victim) & (panel.reporting_date == middle))]

    report = validate_panel(loaded, holed)
    failed = {c.name for c in report.failures}
    assert "entity_spans_contiguous" in failed


def test_an_entity_joining_outside_the_window_is_caught(tmp_path):
    from sdd.validate import validate_panel

    spec = _spec(rate=0.05, start_period=4)
    result = api.run(spec, 200, tmp_path, seed=3)
    panel = pd.read_parquet(result["panel"])
    assert result["validation"]["passed"]

    # Move one arrival earlier than the window allows.
    early = _arrivals(panel).loan_id.iloc[0]
    tampered = panel.copy()
    row = tampered[tampered.loan_id == early].iloc[0].copy()
    row["reporting_date"] = sorted(panel.reporting_date.unique())[1]
    tampered = pd.concat([tampered, row.to_frame().T], ignore_index=True)

    report = validate_panel(api.load(spec), tampered)
    assert "origination_window" in {c.name for c in report.failures}


# ---------------------------------------------------------------------------
# the spec contract
# ---------------------------------------------------------------------------


def test_originations_need_a_count_or_a_rate_but_not_both():
    with pytest.raises(SpecError, match="exactly one of"):
        load_spec_dict(_spec(rate=0.05, per_period=10))
    with pytest.raises(SpecError, match="exactly one of"):
        load_spec_dict(_spec(start_period=2))


def test_originations_without_a_lifecycle_are_refused(minimal_spec_dict):
    import copy

    raw = copy.deepcopy(minimal_spec_dict)
    raw.pop("lifecycle", None)
    raw["originations"] = {"rate": 0.1}
    with pytest.raises(SpecError, match="needs a lifecycle"):
        load_spec_dict(raw)


def test_a_reset_naming_an_unknown_column_is_refused():
    with pytest.raises(SpecError, match="unknown columns"):
        load_spec_dict(_spec(rate=0.05, reset={"not_a_column": 1}))


def test_an_impossible_window_is_refused():
    with pytest.raises(SpecError, match="before start_period"):
        load_spec_dict(_spec(rate=0.05, start_period=5, end_period=2))


# ---------------------------------------------------------------------------
# the configure form and the profiler
# ---------------------------------------------------------------------------


def test_the_form_turns_the_pool_open_and_closed_again():
    opened = api.configure(PACK, periods=12, origination_rate=0.02)
    assert opened["spec"]["originations"]["rate"] == 0.02
    assert any("new loans arrive" in n.lower() for n in opened["notes"])

    closed = api.configure(opened["spec"], origination_rate=0)
    assert closed["spec"].get("originations") is None
    assert any("closed" in n for n in closed["notes"])


def test_capabilities_report_the_open_pool_to_the_form():
    caps = api.capabilities(_spec(rate=0.03))
    assert caps["originations"] is True
    assert caps["origination_rate"] == 0.03


def test_the_profiler_recovers_an_open_pool_from_its_output(tmp_path):
    """The round trip that matters: generate an open pool, profile it blind, and
    get a spec that produces one again — at the same rate."""
    spec = _spec(rate=0.04, fresh=True)
    spec["entity"]["calendar"]["periods"] = 10
    result = api.run(spec, 500, tmp_path / "first", seed=8)
    panel = pd.read_parquet(result["panel"])

    relearned, profile = build_spec(panel, name="relearned")
    learned = profile.dynamics["originations"]
    assert learned["rate"] == pytest.approx(0.04, abs=0.005)
    assert relearned.originations is not None
    assert relearned.originations.rate == pytest.approx(0.04, abs=0.005)

    again = api.run(
        relearned.model_dump(mode="json", exclude_none=True, by_alias=True),
        500,
        tmp_path / "second",
        seed=9,
    )
    assert again["originated"] == pytest.approx(result["originated"], rel=0.1)
    assert again["validation"]["passed"]


def test_a_closed_pool_is_not_reported_as_open(tmp_path):
    result = api.run(_spec(), 300, tmp_path, seed=4)
    panel = pd.read_parquet(result["panel"])

    _, profile = build_spec(panel, name="closed")
    assert "originations" not in profile.dynamics


def test_attrition_is_measured_as_departures_not_as_net_change(tmp_path):
    """In an open pool the two differ, and the net one reports nonsense.

    A pool taking on as many loans as it loses has zero net change and a
    perfectly ordinary prepayment rate.
    """
    spec = _spec(rate=0.05)
    spec["entity"]["calendar"]["periods"] = 10
    result = api.run(spec, 400, tmp_path, seed=6)
    panel = pd.read_parquet(result["panel"])

    _, profile = build_spec(panel, name="open")
    measured = profile.dynamics["attrition"]["annual_rate"]
    expected = rates(api.load(spec))["prepayment_rate"]

    assert measured > 0.0, "departures still happen while the pool grows"
    assert measured == pytest.approx(expected, abs=0.05)
