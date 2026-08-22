"""Charts a pack asks for.

The four drawn until now were fixed in the browser and named for a mortgage: a
delinquency curve and a loan-to-value distribution. A CLO run drew both, and
neither means anything for a corporate loan — there is no LTV, and the ladder is
watchlist and distress rather than days past due.

§18 asks for chart definitions selectable by pack, and explicitly not for a
branch on the pack's name in the interface. So a pack declares what it wants and
the fallback stays generic: it sniffs columns, it never asks which pack it is.
"""

from __future__ import annotations

import pathlib
import tempfile

import pandas as pd
import pytest

from sdd import api
from sdd.spec.schema import ChartSpec

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="module")
def built():
    out = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, 400, out, seed=42, validate_output=False)
    charts = api.charts(PACK, result["panel"], metrics=result["metrics"])
    return result, charts, {c["title"]: c for c in charts["configured"]}


def test_the_pack_gets_the_charts_the_specification_asks_for(built):
    _, _, by_title = built
    assert set(by_title) == {
        "Portfolio par",
        "Credit state",
        "CCC share of par",
        # The two that came with the P1 metrics: credit quality as one number,
        # and diversity as a count rather than an agency's index.
        "Effective obligors",
        "Average credit factor",
        "Industry concentration",
    }


def test_none_of_them_failed_to_build(built):
    _, _, by_title = built
    broken = {t: c["unavailable"] for t, c in by_title.items() if c.get("unavailable")}
    assert not broken, broken


def test_a_series_reads_the_report_rather_than_recomputing_it(built):
    """The line on screen is the number the downloaded report carries.

    Re-aggregating the panel would work and would be a second calculation of the
    same figure, which is how a chart and a report come to disagree.
    """
    result, _, by_title = built
    chart = by_title["Portfolio par"]
    report = pd.DataFrame(result["metrics"])
    assert chart["values"] == [round(float(v), 6) for v in report["collateral_par"]]
    assert chart["periods"] == [str(d) for d in report["date"]]


def test_the_stacked_chart_covers_the_states_it_declares(built):
    _, _, by_title = built
    chart = by_title["Credit state"]
    assert [s["label"] for s in chart["series"]] == [
        "Performing",
        "Watchlist",
        "Distressed",
        "Defaulted",
    ]
    # Shares of the surviving pool, so each band sits in [0, 1] and no column of
    # the stack can exceed the whole.
    for series in chart["series"]:
        assert all(0.0 <= v <= 1.0 for v in series["values"])
    totals = [sum(s["values"][i] for s in chart["series"]) for i in range(len(chart["periods"]))]
    assert all(t <= 1.0000001 for t in totals)


def test_the_bar_chart_is_the_book_as_it_stands(built):
    """A concentration figure is about the final cut-off, not summed over every
    month the book stood there."""
    result, _, by_title = built
    chart = by_title["Industry concentration"]
    panel = pd.read_parquet(result["panel"])
    assert chart["as_of"] == str(panel["reporting_date"].max())
    assert chart["shares"] == sorted(chart["shares"], reverse=True)
    assert sum(chart["shares"]) == pytest.approx(1.0, abs=1e-6)


def test_a_chart_that_cannot_be_built_does_not_lose_the_others():
    """One broken definition must not blank the results screen."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["results"]["charts"].append(
        {"kind": "series", "title": "Nonsense", "metric": "no_such_metric"}
    )
    out = pathlib.Path(tempfile.mkdtemp())
    result = api.run(spec, 150, out, seed=3, validate_output=False)
    charts = api.charts(spec, result["panel"], metrics=result["metrics"])

    by_title = {c["title"]: c for c in charts["configured"]}
    assert "no_such_metric" in by_title["Nonsense"]["unavailable"]
    assert not by_title["Portfolio par"].get("unavailable")


def test_a_pack_that_declares_no_charts_keeps_the_generic_ones(tmp_path):
    """Nothing here knows which pack it is. A pack that declares nothing gets
    the column-sniffing fallback, exactly as before.

    This used to run against the two shipped packs that declared no charts. All
    three declare charts now — the genericity claim is worth little if only the
    pack a feature was built for exercises it — so the case is constructed.
    """
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec.pop("results", None)
    spec["metrics"] = []

    result = api.run(spec, 150, tmp_path / "bare", seed=3, validate_output=False)
    charts = api.charts(spec, result["panel"])
    assert charts["configured"] == []
    assert charts["delinquency"] is not None, "the generic charts are gone"


def test_every_shipped_pack_draws_its_own_charts(tmp_path):
    """The genericity claim, checked rather than asserted.

    Three asset classes, one chart vocabulary, and every chart has to produce
    points — a chart declared against a metric that does not exist renders as an
    empty box rather than as an error anywhere.
    """
    for pack in api.list_packs():
        result = api.run(pack, 200, tmp_path / pack, seed=3, validate_output=False)
        charts = api.charts(pack, result["panel"], metrics=result["metrics"])
        configured = charts["configured"]
        assert configured, f"{pack} draws nothing of its own"
        for chart in configured:
            assert not chart.get("unavailable"), f"{pack}: {chart['title']} {chart['unavailable']}"
            assert chart.get("values") or chart.get("series") or chart.get("categories"), (
                f"{pack}: {chart['title']} drew nothing"
            )


def test_a_series_can_read_a_column_when_there_is_no_metric(tmp_path):
    """Not every pack declares metrics, and a chart should still be possible."""
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 6
    spec["results"] = {
        "charts": [
            {"kind": "series", "title": "Balance", "column": "current_balance", "unit": "money"}
        ]
    }
    result = api.run(spec, 200, tmp_path, seed=3, validate_output=False)
    charts = api.charts(spec, result["panel"])
    chart = charts["configured"][0]
    assert len(chart["values"]) == 6
    assert chart["values"][0] > chart["values"][-1], "an amortising pool should run down"


@pytest.mark.parametrize(
    "bad",
    [
        {"kind": "series", "title": "x"},
        {"kind": "stacked_series", "title": "x"},
        {"kind": "category_bar", "title": "x", "group": "g"},
        {"kind": "histogram", "title": "x"},
    ],
)
def test_an_underspecified_chart_is_refused_at_load_time(bad):
    with pytest.raises(ValueError):
        ChartSpec(**bad)


def test_exactly_one_pack_is_featured():
    """ "Start here" pointing at two places tells a reader nothing.

    It happened: adding the benchmark pack left both it and the CLO flagged
    `featured`, so two cards went purple and both claimed to be the entry point.
    The colour is the strongest signal on that screen and it has to mean one
    thing.
    """
    featured = [name for name in api.list_packs() if api.load(name).meta.featured]
    assert len(featured) == 1, f"expected one featured pack, found {featured}"

    # And it must be the one shown first, or the highlight sits below something
    # unhighlighted and reads as decoration rather than direction.
    assert api.list_packs()[0] == featured[0]
