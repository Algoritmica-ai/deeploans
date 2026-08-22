"""What a learned spec carries beyond its columns.

A spec is more than a list of generators. It says how big the book is, which
figures matter, and what gets drawn on the results screen — and until now a spec
that came back from the profiler had none of the three. Regenerating a pack
produced a portfolio whose size was whatever the draws happened to sum to, an
empty metrics table, and no charts.

The three are not on the same footing, and these tests keep them apart:

  * the **target** is measured. The tape states the opening total and the spec
    should say so.
  * the **metrics** and **charts** are proposed. Which figures matter is a
    judgement no tape records, so what is checked here is that the proposal is
    sound and computes — not that it recovered anything.
"""

from __future__ import annotations

import copy
import pathlib
import tempfile

import pandas as pd
import pytest

from sdd import api
from sdd.profile import build_spec
from sdd.spec.schema import DesignSpec
from sdd.validate.charts import configured_charts

PACKS = ["clo_eu_leveraged_loans", "rmbs_nl_green_lion", "auto_abs_esma_annex5"]
ENTITIES = 250


@pytest.fixture(scope="module")
def learned():
    """Every pack, run and then profiled back into a spec."""
    out = {}
    for pack in PACKS:
        tmp = pathlib.Path(tempfile.mkdtemp())
        base = api.load(pack)
        result = api.run(pack, ENTITIES, tmp, seed=3, validate_output=False)
        panel = pd.read_parquet(result["panel"])
        spec, profile = build_spec(
            panel,
            name=f"relearned_{pack}",
            id_column=base.entity.id_column,
            time_column=base.entity.time_column,
            state_column=base.lifecycle.state_column,
        )
        out[pack] = {
            "spec": spec,
            "profile": profile,
            "panel": panel,
            "time_column": base.entity.time_column,
        }
    return out


@pytest.mark.parametrize("pack", PACKS)
def test_target_matches_the_opening_book(learned, pack):
    """The target is the total that was actually on the tape at the first cut-off.

    Not the panel-wide sum, which double-counts every survivor, and not the mean
    times every entity ever seen — a pool that reinvests met 538 facilities in a
    250-facility deal, and multiplying by the larger number states a portfolio
    that never existed at any one moment.
    """
    case = learned[pack]
    targets = case["spec"].entity.targets
    assert targets, f"{pack}: no target learned, so the deal has no stated size"
    target = targets[0]

    panel, time_column = case["panel"], case["time_column"]
    opening = panel[panel[time_column] == panel[time_column].min()]
    observed = float(opening[target.column].sum())

    assert target.total == pytest.approx(observed, rel=1e-6)
    assert target.entities == opening[case["spec"].entity.id_column].nunique()


@pytest.mark.parametrize("pack", PACKS)
def test_learned_target_is_applicable(learned, pack):
    """A target whose generator cannot be scaled is worse than no target.

    ``apply_targets`` raises on a generator with no closed-form mean, so an
    unusable target does not fail the run that wrote it — it fails every run
    afterwards, which is the worst place to put a failure. Emitting one only
    where it works is the guard; this is the check that the guard is not simply
    refusing everything.
    """
    from sdd.generate.targets import _expected_value

    spec = learned[pack]["spec"]
    target = spec.entity.targets[0]
    column = next(c for c in spec.columns if c.name == target.column)
    assert _expected_value(column.generator) is not None


def test_target_actually_moves_the_book(learned):
    """Negative control: change the number and the portfolio must follow.

    A target that agrees with the fit is indistinguishable from a target that is
    ignored, since both regenerate the same book. Asking for three times the
    size separates them.
    """
    case = learned["auto_abs_esma_annex5"]
    spec = case["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    column = spec["entity"]["targets"][0]["column"]
    asked = spec["entity"]["targets"][0]["total"]

    totals = {}
    for factor in (1.0, 3.0):
        variant = copy.deepcopy(spec)
        variant["entity"]["targets"][0]["total"] = asked * factor
        tmp = pathlib.Path(tempfile.mkdtemp())
        result = api.run(variant, ENTITIES, tmp, seed=9, validate_output=False)
        book = pd.read_parquet(result["panel"])
        opening = book[book[case["time_column"]] == book[case["time_column"]].min()]
        totals[factor] = float(opening[column].sum())

    # Sampling error at 250 entities is a few per cent — the target aims, it does
    # not guarantee — so the tolerance is wide enough to survive that and narrow
    # enough that a target doing nothing would fail.
    assert totals[1.0] == pytest.approx(asked, rel=0.10)
    assert totals[3.0] == pytest.approx(asked * 3.0, rel=0.10)
    assert totals[3.0] / totals[1.0] == pytest.approx(3.0, rel=0.10)


@pytest.mark.parametrize("pack", PACKS)
def test_proposed_metrics_compute(learned, pack):
    """The proposed report has to survive a run, not just validation.

    A metric naming a column that is not there, or weighting by one that holds
    text, passes the schema and dies mid-run. So the spec is regenerated and the
    metrics table read back.
    """
    spec = learned[pack]["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    names = [m["name"] for m in spec["metrics"]]
    assert "total_balance" in names
    assert "active_entities" in names

    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(spec, ENTITIES, tmp, seed=11, validate_output=False)
    table = pd.DataFrame(result["metrics"])

    assert len(table) == result["periods"]
    for name in names:
        assert name in table.columns
        assert table[name].notna().all(), f"{name} computed to nothing"

    assert (table["total_balance"] > 0).all()
    assert table["active_entities"].max() <= ENTITIES + result["originated"]
    if "non_performing_pct" in table:
        assert table["non_performing_pct"].between(0.0, 1.0).all()


@pytest.mark.parametrize("pack", PACKS)
def test_proposed_charts_draw(learned, pack):
    """Every proposed chart has to produce a payload with points in it.

    A chart is declared against a metric or a column, and a declaration that
    names neither correctly renders as an empty box on the results screen rather
    than as an error anywhere.
    """
    spec = learned[pack]["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(spec, ENTITIES, tmp, seed=13, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    charts = configured_charts(DesignSpec.model_validate(spec), panel, result["metrics"])
    assert charts, f"{pack}: nothing to draw"

    for chart in charts:
        assert chart["title"]
        assert chart.get("explain"), f"{chart['title']} has no note behind the icon"
        if chart["kind"] == "series":
            assert chart["values"], f"{chart['title']} drew an empty line"
        if chart["kind"] == "stacked_series":
            assert chart["series"], f"{chart['title']} drew no bands"


def test_rate_column_is_chosen_by_its_values(learned):
    """The weighted mean must not land on a column of labels.

    A tape carries both ``interest_rate_type`` holding "Fixed" and
    ``current_interest_rate_pct`` holding 4.2. Matching on the substring alone
    picks the first, and the same trap took out the auto pack's rate detection
    once already.
    """
    for pack in PACKS:
        spec = learned[pack]["spec"]
        rate = next((m for m in spec.metrics if m.name == "wa_rate"), None)
        if rate is None:
            continue
        column = next(c for c in spec.columns if c.name == rate.column)
        assert column.dtype in ("float", "int"), f"{pack}: {rate.column} is not numeric"
        assert "type" not in rate.column.lower()


def test_opening_count_is_not_the_panel_count(learned):
    """The distinction the target depends on, checked on the pack that has it.

    The CLO pool reinvests: it buys collateral as loans repay, so it meets many
    more facilities over two years than it held on day one. If these two numbers
    ever collapse into one, the target silently overstates the deal.
    """
    profile = learned["clo_eu_leveraged_loans"]["profile"]
    assert profile.opening_entities == ENTITIES
    assert profile.entities > profile.opening_entities
