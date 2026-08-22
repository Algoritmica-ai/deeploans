"""§31 release tests A to F.

The specification names six scenarios to run before a release. Their content
already existed, scattered through `test_clo_pack.py` where nobody could
address them as a group — so "have the release checks been run?" was a question
you answered by remembering which six tests those were.

Two things change here. They are one named suite:

    pytest -m release

and they run against **either** the working tree or a deployed instance:

    pytest -m release --release-target=https://algoritmica-synthetic-data-designer.hf.space

The second is what §31 actually asks for, and it is not a formality. Every other
test in this repo proves the *code* is right. Between the code and what a user
touches sits a Docker build, a copy script, a different Python, a different
filesystem and an HTTP layer, and no other test crosses any of them. Group
detection passed 671 local tests and was dead on the deployed Space for exactly
that reason.

Against a deployment these are slow — minutes, behind a one-worker pool on two
cores — so they are marked and deselected by default. This is a pre-release
step, not something for the normal loop.
"""

from __future__ import annotations

import pandas as pd
import pytest

pytestmark = pytest.mark.release

PACK = "clo_eu_leveraged_loans"

# §31 Test A fixes the population, horizon and seed. B, C and D reuse them
# unchanged — the point of B and C is that only the scenario differs.
FACILITIES = 500
PERIODS = 36
SEED = 42

DISTRESSED = ["Distressed", "Defaulted", "Recovered"]


def _distress_share(panel: pd.DataFrame) -> float:
    return float(panel["credit_state"].isin(DISTRESSED).mean())


def _default_share(panel: pd.DataFrame) -> float:
    return float(panel["credit_state"].isin(["Defaulted", "Recovered"]).mean())


# ---------------------------------------------------------------------------
# Test A — Standard
# ---------------------------------------------------------------------------


def test_a_standard(standard_run):
    """500 facilities, 36 periods, seed 42, base.

    Expected: all invariants pass; moderate portfolio turnover; some
    deterioration and defaults; the portfolio remains diversified.
    """
    run = standard_run
    assert run.entities == FACILITIES
    assert run.periods == PERIODS
    assert run.total_rows > 0

    assert run.invariants_passed, f"invariants failed: {run.failing_checks}"

    # "Moderate turnover" needs a number. The pool buys replacements for two
    # years, so some churn every month is the expected state; none at all would
    # mean reinvestment silently stopped, and a tenth of the book a month would
    # mean it is not a portfolio, it is a trading desk.
    turnover = run.metrics["portfolio_turnover"].dropna()
    assert not turnover.empty, "no turnover was measured at all"
    assert 0.0 < turnover.mean() < 0.10, f"turnover of {turnover.mean():.4f} is not moderate"

    # "Some deterioration" — a base case that never deteriorates is not a base
    # case, and one that collapses is not base either.
    distress = _distress_share(run.panel)
    assert 0.005 < distress < 0.25, f"base distress of {distress:.4f} is not 'some'"
    assert _default_share(run.panel) > 0.0, "no facility ever defaulted"

    # "Remains diversified" — read off the effective obligor count rather than
    # the plain one, since a hundred obligors where two hold half the money is
    # not a diversified book and the plain count cannot tell you so.
    effective = run.metrics["effective_obligors"]
    assert (effective > 20).all(), f"effective obligors fell to {effective.min()}"
    assert float(run.metrics["largest_obligor_pct"].max()) < 0.10


# ---------------------------------------------------------------------------
# Test B — Adverse
# ---------------------------------------------------------------------------


def test_b_adverse_is_worse_than_base(standard_run, adverse_run):
    """Same seed and population. Expected: more distress, more defaults, lower
    market value, lower recoveries than base.

    **"Lower recoveries" is read as the recovery *rate*, not the amount**, and
    the distinction is not pedantry. Adverse produces roughly seven times the
    defaults, so gross recoveries rise even as each default returns materially
    less — measured here, EUR 30.6m of recoveries in base against EUR 155.1m in
    adverse, on rates of 0.554 and 0.401. Read as the amount this expectation
    would be close to unsatisfiable: it would require the rate to collapse
    almost to zero before the total could fall.

    The rate is also what the pack actually declares — `recovery_multiplier`
    0.8 for adverse, 0.55 for severe — so this reading is the one the
    configuration can be checked against.
    """
    assert _distress_share(adverse_run.panel) > _distress_share(standard_run.panel)
    assert _default_share(adverse_run.panel) > _default_share(standard_run.panel)

    assert (
        adverse_run.panel["current_market_price"].mean()
        < standard_run.panel["current_market_price"].mean()
    )

    assert _recovery_rate(adverse_run) < _recovery_rate(standard_run)


def _recovery_rate(run) -> float:
    """Recovered par as a share of defaulted par, over the whole run."""
    defaulted = float(run.metrics["cumulative_defaults"].iloc[-1])
    recovered = float(run.metrics["cumulative_recoveries"].iloc[-1])
    assert defaulted > 0, "nothing defaulted, so there is no recovery rate to speak of"
    return recovered / defaulted


# ---------------------------------------------------------------------------
# Test C — Severe
# ---------------------------------------------------------------------------


def test_c_severe_is_materially_worse_than_adverse(standard_run, adverse_run, severe_run):
    """Expected: materially greater deterioration than adverse; the largest
    realised losses; the lowest average price.

    "Materially" is given a floor rather than left to `>`. Three scenarios that
    ordered correctly by a rounding error would satisfy a bare inequality and
    would tell an investor nothing, so severe has to be at least half again as
    bad as adverse.
    """
    adverse, severe = _distress_share(adverse_run.panel), _distress_share(severe_run.panel)
    assert severe > adverse * 1.5, f"severe {severe:.4f} is not materially worse than {adverse:.4f}"

    losses = [
        float(r.metrics["cumulative_realised_losses"].iloc[-1])
        for r in (standard_run, adverse_run, severe_run)
    ]
    assert losses == sorted(losses), (
        f"realised losses did not order base->adverse->severe: {losses}"
    )

    prices = [
        float(r.panel["current_market_price"].mean())
        for r in (standard_run, adverse_run, severe_run)
    ]
    assert prices == sorted(prices, reverse=True), f"prices did not fall with stress: {prices}"


# ---------------------------------------------------------------------------
# Test D — Reproducibility
# ---------------------------------------------------------------------------


def test_d_reproducibility(target, standard_run):
    """Run Test A twice. Expected: byte-identical or logically identical output.

    Logical identity is what is asserted: the frames must be equal cell for
    cell. Byte identity is not, because parquet embeds writer metadata that has
    nothing to do with the data — and a test that failed on a library upgrade
    would be reporting on pyarrow rather than on this project.
    """
    again = target.run(PACK, entities=FACILITIES, periods=PERIODS, seed=SEED)

    assert again.total_rows == standard_run.total_rows
    assert again.spec_hash == standard_run.spec_hash

    left = standard_run.panel.sort_values(["facility_id", "reporting_date"]).reset_index(drop=True)
    right = again.panel.sort_values(["facility_id", "reporting_date"]).reset_index(drop=True)
    assert left.equals(right), "the same seed and configuration produced different data"

    pd.testing.assert_frame_equal(standard_run.metrics, again.metrics)


# ---------------------------------------------------------------------------
# Test E — Reinvestment
# ---------------------------------------------------------------------------


def test_e_reinvestment_window_is_respected(target):
    """A short reinvestment period. Expected: assets enter before it ends, and
    none afterwards.

    Deliberately shortened rather than run at the pack's own 24, because a
    window that ends at the very edge of the panel cannot distinguish "stopped
    correctly" from "the data ran out".
    """
    window = 6
    run = target.run(
        PACK,
        entities=300,
        periods=24,
        seed=SEED,
        spec_overrides={"originations": {"end_period": window}},
    )

    cutoffs = sorted(run.panel["reporting_date"].unique())
    index = {date: position for position, date in enumerate(cutoffs)}
    arrived = run.panel.groupby("facility_id")["reporting_date"].min().map(index)

    joined_later = arrived[arrived > 0]
    assert not joined_later.empty, "nothing joined at all, so the window proves nothing"
    assert int(joined_later.max()) <= window, (
        f"a facility joined at period {int(joined_later.max())}, after the window closed at {window}"
    )
    assert int(joined_later.min()) <= window


# ---------------------------------------------------------------------------
# Test F — Terminal states
# ---------------------------------------------------------------------------


def test_f_terminal_assets_exit_and_stay_gone(target):
    """Elevated prepayment, default and trading rates. Expected: Prepaid, Sold,
    Matured and Recovered exit correctly, and no terminal asset is resurrected.

    The rates are lifted so all four exits are actually exercised. At the pack's
    own calibration a 36-period run matures a handful of facilities, and a test
    that never sees an exit cannot show that the exit works.
    """
    run = target.run(
        PACK,
        entities=400,
        periods=36,
        seed=SEED,
        scenario="severe",
        spec_overrides={
            "scenarios": {
                "severe": {
                    "name": "severe",
                    "default_multiplier": 5.5,
                    "prepayment_multiplier": 3.0,
                    "recovery_multiplier": 0.55,
                }
            }
        },
    )

    terminal = {"Prepaid", "Sold", "Matured", "Recovered"}
    reached = set(run.panel["credit_state"]) & terminal
    assert len(reached) >= 3, f"only {sorted(reached)} were exercised"

    ordered = run.panel.sort_values(["facility_id", "reporting_date"])
    was_terminal = ordered["credit_state"].isin(terminal)
    resurrected = (
        ordered.assign(_t=was_terminal)
        .groupby("facility_id")["_t"]
        .apply(lambda flags: bool((flags.cummax() & ~flags).any()))
    )
    assert not resurrected.any(), (
        f"{int(resurrected.sum())} facilities came back from a terminal state"
    )

    # A terminal facility stops reporting: its terminal row is its last.
    last = ordered.groupby("facility_id").tail(1)
    final_terminal = last[last["credit_state"].isin(terminal)]
    assert not final_terminal.empty
    lengths = ordered[ordered["credit_state"].isin(terminal)].groupby("facility_id").size()
    assert int(lengths.max()) == 1, "a terminal facility was reported more than once"
