"""The European CLO pack.

A trading portfolio of corporate loans, which differs from the two consumer
packs in four ways that these tests pin: the balance is bullet rather than
amortising, the pool reinvests and then stops, the ladder is a credit ladder
rather than a delinquency one, and facilities reach the end of their own term.

What this pack does *not* model is as important as what it does. There is one
obligor per facility, so nothing here measures obligor concentration, and the
rating is derived from the credit state rather than migrated by its own matrix.
Both are recorded in `test_the_pack_does_not_overclaim`.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd import api

PACK = "clo_eu_leveraged_loans"
TERMINAL = {"Recovered", "Prepaid", "Sold", "Matured"}


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    result = api.run(PACK, 500, tmp_path_factory.mktemp("clo"), seed=42)
    return result, pd.read_parquet(result["panel"])


def test_the_default_run_completes_and_validates(run):
    result, _ = run
    assert result["validation"]["passed"], [
        c["name"] for c in result["validation"]["checks"] if not c["passed"]
    ]
    assert result["validation"]["total"] >= 35


def test_the_shape_matches_the_specification(run):
    result, _ = run
    spec = api.load(PACK)
    # Output columns, not declared ones: obligor attributes live on the group now.
    assert 55 <= len(spec.output_columns()) <= 70, "the spec asks for roughly 55-70 columns"
    assert result["entities"] == 500
    assert result["periods"] == 36
    assert spec.entity.id_column == "facility_id"
    assert spec.entity.time_column == "reporting_date"


def test_every_credit_state_is_reachable(run):
    """Eight states, four of them terminal and each reached a different way."""
    _, panel = run
    seen = set(panel["credit_state"])
    for state in ("Performing", "Watchlist", "Distressed", "Defaulted"):
        assert state in seen, f"{state} never occurred"
    for state in TERMINAL:
        assert state in seen, f"terminal state {state} was never reached"


def test_the_balance_is_bullet_not_amortising(run):
    """A leveraged loan repays at maturity, so the balance holds flat."""
    _, panel = run
    live = panel[panel["credit_state"].isin(["Performing", "Watchlist", "Distressed"])]
    spread = live.groupby("facility_id")["current_balance"].nunique()
    # A facility's balance takes one value while it is live, plus zero in the
    # final month when the bullet falls due.
    assert spread.max() <= 2, "a bullet balance amortised"


def test_the_portfolio_reinvests_and_then_stops(run):
    """New collateral joins until the reinvestment period closes, then never."""
    result, panel = run
    assert result["originated"] > 0, "nothing was acquired"

    cutoffs = sorted(panel["reporting_date"].unique())
    first_seen = panel.groupby("facility_id")["reporting_date"].min()
    joined_at = first_seen.map({d: i for i, d in enumerate(cutoffs)})

    assert (joined_at > 0).any(), "no facility joined after the opening portfolio"
    assert joined_at.max() <= 24, "a facility joined after the reinvestment period ended"


def test_facilities_reach_the_end_of_their_own_term(run):
    """Maturity is a real exit route here, not an edge case."""
    _, panel = run
    matured = panel[panel["credit_state"] == "Matured"]
    assert len(matured) > 0
    # Maturity is a fact about the facility, so every matured row satisfies it.
    assert (matured["months_to_maturity"] <= 1).all()


def test_terminal_facilities_stop_reporting(run):
    _, panel = run
    last = panel.groupby("facility_id")["reporting_date"].max()
    terminal_rows = panel[panel["credit_state"].isin(TERMINAL)]
    for fid, when in terminal_rows.set_index("facility_id")["reporting_date"].items():
        assert last[fid] == when, f"{fid} kept reporting after leaving the portfolio"


def test_a_default_is_followed_by_a_workout(run):
    """Defaulted facilities stay in the portfolio while the recovery runs."""
    _, panel = run
    defaulted = set(panel.loc[panel["credit_state"] == "Defaulted", "facility_id"])
    assert defaulted

    # The workout is nine periods, and a facility that serves it in full reports
    # `Defaulted` for eight of them: the ninth is the period it resolves in, and
    # that row already reads `Recovered`.
    resolved_ids = set(panel.loc[panel["credit_state"] == "Recovered", "facility_id"])
    served = (
        panel[panel["facility_id"].isin(resolved_ids) & (panel["credit_state"] == "Defaulted")]
        .groupby("facility_id")
        .size()
    )
    assert not served.empty, "no facility completed a workout"
    assert served.max() == 8, (
        f"a resolved facility reported Defaulted for {served.max()} periods; "
        "the nine-period workout should show eight"
    )

    resolved = panel[panel["credit_state"] == "Recovered"]
    assert len(resolved) > 0
    assert (resolved["recovery_amount"] > 0).all(), "a workout returned nothing"


def test_a_loss_is_booked_against_the_par_that_defaulted(run):
    """A resolved facility carries no par, so the loss needs the earlier figure.

    Measured against `current_par` the loss was always zero, because the workout
    has already closed the facility out by the time it is booked.
    """
    _, panel = run
    resolved = panel[panel["credit_state"] == "Recovered"]
    assert (resolved["realised_loss"] > 0).any(), "every realised loss was zero"
    assert (resolved["recovery_amount"] <= resolved["par_at_default"] + 0.01).all()


def test_event_dates_are_stamped_once(run):
    """A date rewritten every period would lose the event it records."""
    _, panel = run
    for column in ("default_date", "recovery_date", "sale_date"):
        stamped = panel[panel[column].notna()]
        if stamped.empty:
            continue
        assert stamped.groupby("facility_id")[column].nunique().max() == 1, (
            f"{column} changed after it was first written"
        )


@pytest.mark.parametrize(
    ("metric", "ascending"),
    [("distress", True), ("price", False), ("loss", True)],
)
def test_scenarios_order_correctly(tmp_path_factory, metric, ascending):
    """§15: the same seed and population, progressively more stress."""
    base = tmp_path_factory.mktemp("scen")
    values = []
    for scenario in ("base", "adverse", "severe"):
        result = api.run(
            PACK, 300, base / scenario, seed=42, scenario=scenario, validate_output=False
        )
        panel = pd.read_parquet(result["panel"])
        values.append(
            {
                "distress": panel["credit_state"]
                .isin(["Distressed", "Defaulted", "Recovered"])
                .mean(),
                "price": panel["current_market_price"].mean(),
                "loss": panel["realised_loss"].sum(),
            }[metric]
        )

    ordered = values == sorted(values) if ascending else values == sorted(values, reverse=True)
    assert ordered, f"{metric} did not order base->adverse->severe: {values}"


def test_the_run_is_reproducible(tmp_path):
    a = api.run(PACK, 300, tmp_path / "a", seed=7, validate_output=False)
    b = api.run(PACK, 300, tmp_path / "b", seed=7, validate_output=False)
    assert pd.read_parquet(a["panel"]).equals(pd.read_parquet(b["panel"]))
    assert a["spec_hash"] == b["spec_hash"]


def test_the_pack_does_not_overclaim():
    """Limitations that must not be quietly forgotten.

    Each assertion here is written to fail the moment its limitation is lifted,
    so it has to be updated rather than deleted — that is the point.

    Grouping and rating migration have both landed and their assertions are now
    inverted. The rating one had to be rewritten twice over: it originally
    looked for a hazard named "rating" on the *primary* lifecycle, which meant a
    rating chain could arrive as a `secondary_chain` and the test would sail
    past it, still asserting a limitation that no longer existed.
    """
    spec = api.load(PACK)

    assert spec.groups, "obligor grouping disappeared"
    assert spec.groups[0].columns, "the obligor carries no attributes of its own"

    chains = {c.name: c for c in spec.secondary_chains}
    assert "rating" in chains, "the rating is no longer a chain of its own"

    rating = chains["rating"]
    assert rating.lifecycle.transitions, "the rating chain has no matrix to migrate on"
    assert len(rating.lifecycle.states) >= 9, "fewer grades than the specification asks for"
    assert rating.coupling.forced_by, "nothing ties the rating to the credit state"
    assert rating.coupling.stress, "the rating does not feed back into distress"

    # And it must no longer be computed from the credit state.
    derived = [d for d in spec.derivations if d.target == rating.lifecycle.state_column]
    assert not derived, (
        "the rating is both migrated and derived; the derivation would overwrite "
        "the chain every period"
    )


def test_no_real_company_or_manager_names():
    """Everything identifying must read as synthetic."""
    spec = api.load(PACK)
    for key in ("transaction_name", "manager_name", "clo_id"):
        value = str(spec.constants[key])
        assert "Synthetic" in value or "SYNCLO" in value, f"{key} is {value!r}"
