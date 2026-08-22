"""The portfolio, summarised at every cut-off.

The panel says what each loan did. A report says what the *book* did — its size,
its average coupon, how much is in trouble, how concentrated it is. Until now
the engine reported a count of entities per state and nothing else, so the
monthly tapes existed and the monthly report did not.

The tests that matter here are the reconciliation ones: a figure nobody can tie
back to the rows it came from is decoration.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd import api
from sdd import metrics as M
from sdd.spec.schema import Metric

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    out = tmp_path_factory.mktemp("metrics")
    result = api.run(PACK, 500, out, seed=42)
    return (
        result,
        pd.read_parquet(result["panel"]),
        pd.read_parquet(out / "portfolio_metrics.parquet"),
    )


# ---------------------------------------------------------------------------
# the arithmetic
# ---------------------------------------------------------------------------


def _frame():
    return pd.DataFrame(
        {
            "par": [100.0, 200.0, 300.0],
            "spread": [400.0, 300.0, 350.0],
            "flag": ["Y", "N", "N"],
            "obligor": ["A", "A", "B"],
            "flow": [10.0, 0.0, 5.0],
        }
    )


def _value(metric, frame=None, running=None):
    spec = api.load(PACK)
    spec.metrics = [metric]
    if running is None:
        running = {}
    row = M.compute(spec, frame if frame is not None else _frame(), 0, "2026-01-31", running)
    return row[metric.name]


def test_sum_and_count():
    assert _value(Metric(name="v", kind="sum", column="par")) == 600.0
    assert _value(Metric(name="v", kind="count")) == 3.0
    assert _value(Metric(name="v", kind="distinct_count", column="obligor")) == 2.0


def test_a_weighted_mean_is_weighted():
    """Unweighted this is 350; weighted by par it is not, and the difference is
    the whole reason the field is required."""
    weighted = _value(Metric(name="v", kind="weighted_mean", column="spread", weight="par"))
    assert weighted == pytest.approx((100 * 400 + 200 * 300 + 300 * 350) / 600)
    assert weighted != pytest.approx(350.0)


def test_a_share_is_of_the_column_not_the_row_count():
    """One row in three carries the flag, but only a sixth of the par."""
    share = _value(Metric(name="v", kind="share_where", column="par", where="flag == 'Y'"))
    assert share == pytest.approx(100 / 600)


def test_the_largest_group_share():
    share = _value(Metric(name="v", kind="max_group_share", column="par", group="obligor"))
    assert share == pytest.approx(300 / 600)


def test_a_cumulative_metric_accumulates_across_periods():
    running: dict[str, float] = {}
    metric = Metric(name="v", kind="cumulative", column="flow")
    first = _value(metric, running=running)
    second = _value(metric, running=running)
    assert first == 15.0
    assert second == 30.0, "the running total did not carry between cut-offs"


def test_an_empty_book_has_no_average_rather_than_a_zero_one():
    """Zero would read as a real measurement of nothing."""
    import math

    empty = _frame().iloc[:0]
    value = _value(
        Metric(name="v", kind="weighted_mean", column="spread", weight="par"), frame=empty
    )
    assert math.isnan(value)


def test_a_missing_column_is_named_in_the_error():
    with pytest.raises(M.MetricError, match="no_such_column"):
        _value(Metric(name="v", kind="sum", column="no_such_column"))


# ---------------------------------------------------------------------------
# the report as produced
# ---------------------------------------------------------------------------


def test_the_pack_reports_every_figure_the_specification_lists(run):
    _, _, report = run
    # 19 P0 plus the three §17 marks P1, added once the licensing question was
    # answered by computing them generically rather than reproducing an agency's
    # tables. See `docs/clo/GENERIC-CREDIT-MEASURES.md`.
    assert len(api.load(PACK).metrics) == 22
    for column in (
        "collateral_par",
        "active_facilities",
        "obligors",
        "wa_spread_bps",
        "ccc_par_pct",
        "largest_obligor_pct",
        "cumulative_realised_losses",
    ):
        assert column in report.columns


def test_one_row_per_cut_off(run):
    result, panel, report = run
    assert len(report) == result["periods"]
    assert list(report["date"]) == sorted(panel["reporting_date"].unique())


def test_the_report_is_written_beside_the_data(tmp_path):
    api.run(PACK, 150, tmp_path, seed=3, validate_output=False)
    names = {f.name for f in tmp_path.iterdir() if f.is_file()}
    assert {"portfolio_metrics.parquet", "portfolio_metrics.csv"} <= names


# ---------------------------------------------------------------------------
# reconciliation — the tests that make the figures trustworthy
# ---------------------------------------------------------------------------


def test_the_totals_tie_back_to_the_facilities(run):
    """A figure nobody can tie back to the rows it came from is decoration."""
    _, panel, report = run
    from_panel = panel.groupby("reporting_date")["current_par"].sum().round(2)
    from_report = report.set_index("date")["collateral_par"].round(2)
    pd.testing.assert_series_equal(
        from_panel, from_report, check_names=False, check_index_type=False, rtol=1e-9
    )


def test_the_counts_tie_back(run):
    _, panel, report = run
    counted = panel.groupby("reporting_date")["facility_id"].nunique()
    assert list(report["active_facilities"].astype(int)) == list(counted)
    obligors = panel.groupby("reporting_date")["obligor_id"].nunique()
    assert list(report["obligors"].astype(int)) == list(obligors)


def test_every_share_stays_between_zero_and_one(run):
    """A share above 1 means numerator and denominator came from different
    populations, which is the failure a reported percentage hides best.

    Selected by *kind* rather than by name: `wa_coupon_pct` is a weighted mean
    reading 6.8 per cent, and asserting it sits in [0, 1] would test the naming
    convention instead of the metric.
    """
    _, _, report = run
    shares = [
        m.name for m in api.load(PACK).metrics if m.kind in ("share_where", "max_group_share")
    ]
    assert shares, "the pack declares no shares to check"
    for column in shares:
        values = report[column].dropna()
        assert (values >= 0).all() and (values <= 1).all(), column


def test_cumulative_figures_never_fall(run):
    _, _, report = run
    for column in [c for c in report.columns if c.startswith("cumulative_")]:
        assert report[column].is_monotonic_increasing, column


def test_the_opening_ratings_reach_the_opening_report(run):
    """Chains seed their own arrays; the book has to carry the same states.

    Left unwritten, period 0 showed every facility on the column generator's
    constant and the CCC share read 0.00% at the first cut-off and 5% at the
    second — wrong for exactly one row of the report.
    """
    _, _, report = run
    assert report["ccc_par_pct"].iloc[0] > 0, "no CCC facilities in the opening book"


# ---------------------------------------------------------------------------
# genericity
# ---------------------------------------------------------------------------


def test_metrics_work_on_a_pack_that_is_not_the_clo_one(tmp_path):
    """Nothing here knows what a CLO is."""
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 6
    spec["metrics"] = [
        {"name": "pool_balance", "kind": "sum", "column": "current_balance"},
        {"name": "loans", "kind": "count"},
        {
            "name": "wa_coupon",
            "kind": "weighted_mean",
            "column": "current_interest_rate_pct",
            "weight": "current_balance",
            "decimals": 4,
        },
        {
            "name": "arrears_pct",
            "kind": "share_where",
            "column": "current_balance",
            "where": "arrears_bucket != 'Performing'",
            "decimals": 6,
        },
    ]
    result = api.run(spec, 300, tmp_path, seed=3, validate_output=False)
    report = pd.DataFrame(result["metrics"])
    assert len(report) == 6
    assert (report["pool_balance"] > 0).all()
    assert (report["arrears_pct"].between(0, 1)).all()


def test_a_pack_without_metrics_writes_none(tmp_path):
    """A spec that asks for no report gets no report file.

    This used to point at the auto pack, which declared none. All three shipped
    packs now carry a report — the genericity claim is worth little if only the
    pack the feature was built for exercises it — so the case is made explicitly
    instead.
    """
    spec = api.load("auto_abs_esma_annex5").model_dump(
        mode="json", exclude_none=True, by_alias=True
    )
    spec["metrics"] = []
    spec.pop("results", None)

    api.run(spec, 150, tmp_path, seed=3, validate_output=False)
    assert not (tmp_path / "portfolio_metrics.parquet").exists()
    assert not (tmp_path / "portfolio_metrics.csv").exists()
