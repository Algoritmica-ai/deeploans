"""The three figures §17 marks P1, and the sector overlay §16 asks for.

§17 warns off reproducing a rating agency's calculations. That warning is the
design constraint here, not an afterthought, and it is what these tests check as
much as the arithmetic:

  * **Average credit factor** stands in for a weighted average rating. The
    factors are not any agency's published table — each is the probability that
    *this pack's own* rating chain reaches default within five years from that
    grade, times 10,000. Reproducible from the pack file and nothing else.
  * **Effective obligors** stands in for a diversity score. It is the inverse
    Herfindahl — one over the sum of squared shares — which is ordinary
    statistics rather than anyone's model, and reads as a count so it can be
    compared directly against the plain obligor number.
  * **Portfolio turnover** has no agency equivalent and is simply how much of
    the book left since the last cut-off.

The sector overlay is the other half: `default_multiplier` moves the whole book
at once, which is not how a downturn arrives, and leaves every concentration
figure identical however severe it is set.
"""

from __future__ import annotations

import copy
import pathlib
import tempfile

import numpy as np
import pandas as pd
import pytest

from sdd import api

PACK = "clo_eu_leveraged_loans"
ENTITIES = 400


@pytest.fixture(scope="module")
def run():
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, ENTITIES, tmp, seed=3, validate_output=True)
    return {
        "spec": api.load(PACK),
        "panel": pd.read_parquet(result["panel"]),
        "report": pd.DataFrame(result["metrics"]),
        "validation": result["validation"],
    }


# ---------------------------------------------------------------------------
# the credit factor
# ---------------------------------------------------------------------------


def test_the_factors_are_this_pack_s_own_model(run):
    """The licensing claim, checked rather than asserted.

    Every factor is recomputed here from the pack's own rating transition
    matrix: raise it to the 60th power, read the column for D, multiply by
    10,000. If someone quietly pastes an agency's table in, this fails — which
    is the point, since the whole defence of shipping these numbers is that they
    are outputs of a model published in the same file.
    """
    spec = run["spec"]
    chain = spec.secondary_chains[0].lifecycle
    matrix = np.array(chain.transitions, dtype=float)
    default_column = chain.states.index("D")
    five_years = np.linalg.matrix_power(matrix, 60)

    derivation = next(d for d in spec.derivations if d.target == "credit_factor")
    declared = {}
    for rule in derivation.rules:
        grade = rule.if_.split("== ")[1].strip("'\" ")
        declared[grade] = rule.then
    declared["D"] = derivation.else_

    for index, grade in enumerate(chain.states):
        expected = round(float(five_years[index, default_column]) * 10_000)
        assert declared[grade] == pytest.approx(expected, abs=1), (
            f"{grade}: pack says {declared[grade]}, the pack's own chain implies {expected}"
        )


def test_the_factor_is_monotone_in_the_grade(run):
    """A worse grade never carries a lower factor.

    This is what makes the weighted average mean anything. If the ordering
    broke, a downgrade could improve the reported credit quality — and the
    number would keep looking entirely plausible while it did.
    """
    spec = run["spec"]
    grades = spec.secondary_chains[0].lifecycle.states
    panel = run["panel"]

    factors = panel.groupby("rating_at_cutoff")["credit_factor"].first()
    ordered = [float(factors[g]) for g in grades if g in factors.index]
    assert ordered == sorted(ordered)


def test_the_average_reads_between_the_grades_it_averages(run):
    """A sanity check that catches a factor column wired to the wrong thing."""
    report, panel = run["report"], run["panel"]
    lowest = float(panel["credit_factor"].min())
    highest = float(panel["credit_factor"].max())

    average = report["wa_credit_factor"].dropna()
    assert (average >= lowest).all()
    assert (average <= highest).all()


def test_the_average_worsens_as_the_book_does(run):
    """Direction, on a pack whose rating chain drifts downward by construction."""
    average = run["report"]["wa_credit_factor"].dropna()
    assert average.iloc[-1] > average.iloc[0]


# ---------------------------------------------------------------------------
# effective count
# ---------------------------------------------------------------------------


def test_effective_obligors_matches_the_inverse_herfindahl(run):
    """Recomputed by hand from the panel, on the opening cut-off."""
    panel, report = run["panel"], run["report"]
    opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]

    by_obligor = opening.groupby("obligor_id")["current_par"].sum()
    shares = by_obligor / by_obligor.sum()
    expected = 1.0 / float((shares**2).sum())

    # The metric is declared with two decimals, so it is compared at the
    # precision it is reported at rather than at the precision it is computed to.
    assert report.loc[0, "effective_obligors"] == pytest.approx(round(expected, 2), abs=1e-9)


def test_it_never_exceeds_the_plain_count(run):
    """The property that makes it readable as a count.

    Equal shares give exactly the count; anything less even gives less. A value
    above the count would mean the arithmetic is not a Herfindahl at all.
    """
    panel, report = run["panel"], run["report"]
    counts = panel.groupby("reporting_date")["obligor_id"].nunique().to_numpy()
    effective = report["effective_obligors"].to_numpy()
    assert (effective <= counts + 1e-6).all()


def test_it_responds_to_concentration():
    """Negative control, on frames constructed to have a known answer.

    A metric that merely counted would pass every test above. These two frames
    hold the same hundred groups and the same total; only the spread differs.
    """
    from sdd.metrics import _effective_count
    from sdd.spec.schema import Metric

    metric = Metric(name="e", kind="effective_count", column="par", group="obligor")

    even = pd.DataFrame({"obligor": [f"O{i}" for i in range(100)], "par": [1_000.0] * 100})
    lopsided = even.copy()
    lopsided.loc[:1, "par"] = 25_000.0  # two obligors take half the money

    spread = _effective_count(even, metric, None, even["par"])
    concentrated = _effective_count(lopsided, metric, None, lopsided["par"])

    assert spread == pytest.approx(100.0)
    assert concentrated < 20.0


# ---------------------------------------------------------------------------
# turnover
# ---------------------------------------------------------------------------


def test_turnover_reports_nothing_for_the_first_cutoff(run):
    """A book cannot have turned over before it existed.

    Zero would be the easy answer and the wrong one: it would drag every average
    down and read as a quiet period rather than as no observation.
    """
    assert pd.isna(run["report"].loc[0, "portfolio_turnover"])
    assert run["report"]["portfolio_turnover"].iloc[1:].notna().all()


def test_turnover_values_departures_before_the_balance_is_zeroed(tmp_path):
    """The bug this metric shipped with, pinned.

    Most packs zero the balance as an entity enters its terminal state — that is
    what `state_fields` is for — so the row on which a loan disappears reads
    zero. Valued there, a book that lost a quarter of its loans reported exactly
    zero turnover, on every cut-off, for the whole panel.

    The auto pack is the fixture precisely because it zeroes on exit. The CLO
    does not, which is why the bug was invisible on the pack the metric was
    written for.
    """
    result = api.run("auto_abs_esma_annex5", 400, tmp_path, seed=5, validate_output=False)
    report = pd.DataFrame(result["metrics"])
    panel = pd.read_parquet(result["panel"])

    time_column = api.load("auto_abs_esma_annex5").entity.time_column
    final = (
        panel.sort_values(["unique_identifier", time_column]).groupby("unique_identifier").tail(1)
    )
    departed = final[final[time_column] < panel[time_column].max()]
    assert not departed.empty, "nothing left the pool; the fixture proves nothing"
    assert (departed["current_principal_balance"] == 0).all(), (
        "this pack no longer zeroes on exit, so it no longer tests what it was chosen for"
    )

    assert report["portfolio_turnover"].fillna(0).sum() > 0.05


def test_turnover_tracks_what_actually_left(run):
    """Cross-checked against the panel, cut-off by cut-off."""
    panel, report = run["panel"], run["report"]
    dates = sorted(panel["reporting_date"].unique())

    positive = panel[panel["current_par"] > 0]
    last_seen = positive.sort_values("reporting_date").groupby("facility_id")["current_par"].last()

    for index in (3, 8, 15):
        before = set(panel[panel["reporting_date"] == dates[index - 1]]["facility_id"])
        after = set(panel[panel["reporting_date"] == dates[index]]["facility_id"])
        gone = before - after

        reported = report.loc[index, "portfolio_turnover"]
        if not gone:
            assert reported == pytest.approx(0.0, abs=1e-9)
            continue
        # Same direction and rough size; the metric's denominator carries
        # entities whose last positive balance is older than one period.
        rough = last_seen.reindex(list(gone)).sum() / last_seen.reindex(list(before)).sum()
        assert reported == pytest.approx(rough, rel=0.35)


# ---------------------------------------------------------------------------
# the sector overlay
# ---------------------------------------------------------------------------


def test_the_overlay_orders_sectors_as_declared(tmp_path):
    """Measured on transitions inside one run, not on outcomes across two.

    Comparing two runs conflates the overlay with reinvestment feedback and with
    ordinary Monte Carlo noise. The worsening *rate* per sector isolates the
    mechanism, and it should rank exactly as the scenario declares.
    """
    spec = api.load(PACK)
    order = {state: index for index, state in enumerate(spec.lifecycle.states)}
    declared = spec.scenarios["adverse"].segment_stress["industry"]

    result = api.run(PACK, 900, tmp_path, seed=11, scenario="adverse", validate_output=False)
    panel = pd.read_parquet(result["panel"]).sort_values(["facility_id", "reporting_date"])
    panel["_next"] = panel.groupby("facility_id")["credit_state"].shift(-1)

    live = panel[~panel["credit_state"].isin(spec.lifecycle.terminal) & panel["_next"].notna()]
    worsened = live["_next"].map(order) > live["credit_state"].map(order)
    rate = worsened.groupby(live["industry"]).mean()

    stressed = pd.Series({k: rate[k] for k in declared if k in rate.index})
    expected = pd.Series({k: declared[k] for k in stressed.index})

    # Strong rank agreement, not perfect. This asserted an exact 1.0 until a
    # change elsewhere shifted the random stream and two sectors with adjacent
    # multipliers — 1.5 and 1.7 — swapped places. Measured across seeds the
    # correlation sits at 0.93 to 0.96, so the exact form was a property of one
    # seed rather than of the overlay, and tightening it back would only pin the
    # noise again.
    assert stressed.corr(expected, method="spearman") > 0.85

    # The ends are the part that must not blur: whatever happens among the
    # middle ranks, the sector a scenario leans on hardest has to come out
    # materially worse than the one it spares.
    worst = max(declared, key=lambda k: declared[k])
    mildest = min(declared, key=lambda k: declared[k])
    assert rate[worst] > rate[mildest] * 1.5


def test_an_unnamed_sector_is_left_alone(tmp_path):
    """A scenario says which parts of the book it lands on, not which it spares."""
    spec = api.load(PACK)
    declared = spec.scenarios["adverse"].segment_stress["industry"]
    order = {state: index for index, state in enumerate(spec.lifecycle.states)}

    result = api.run(PACK, 900, tmp_path, seed=11, scenario="adverse", validate_output=False)
    panel = pd.read_parquet(result["panel"]).sort_values(["facility_id", "reporting_date"])
    panel["_next"] = panel.groupby("facility_id")["credit_state"].shift(-1)
    live = panel[~panel["credit_state"].isin(spec.lifecycle.terminal) & panel["_next"].notna()]
    worsened = live["_next"].map(order) > live["credit_state"].map(order)
    rate = worsened.groupby(live["industry"]).mean()

    untouched = [i for i in rate.index if i not in declared]
    baseline = rate[untouched].mean()
    assert rate[untouched].max() / baseline < 1.25
    assert rate[untouched].min() / baseline > 0.75


def test_the_overlay_changes_where_the_losses_land(tmp_path):
    """The point of §16, stated as the outcome it exists to produce.

    Under a uniform multiplier every book of the same size behaves identically
    however lopsided its sector mix. The overlay is what makes concentration
    cost something.
    """
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    flat = copy.deepcopy(spec)
    for scenario in flat["scenarios"].values():
        scenario.pop("segment_stress", None)

    def retail_share_of_distress(variant, tag):
        result = api.run(
            variant, 600, tmp_path / tag, seed=7, scenario="adverse", validate_output=False
        )
        panel = pd.read_parquet(result["panel"])
        bad = panel[panel["credit_state"].isin(["Distressed", "Defaulted", "Recovered"])]
        return float((bad["industry"] == "Retail").mean())

    assert retail_share_of_distress(spec, "overlay") > retail_share_of_distress(flat, "flat")


def test_a_zero_multiplier_is_refused():
    """Zero would mean the segment can never change state again, which is not a stress."""
    from sdd.spec.schema import Scenario

    with pytest.raises(ValueError, match="not a multiplier"):
        Scenario(name="broken", segment_stress={"industry": {"Retail": 0.0}})


# ---------------------------------------------------------------------------
# the invariants, and genericity
# ---------------------------------------------------------------------------


def test_the_factor_invariants_pass(run):
    named = {c["name"]: c for c in run["validation"]["checks"]}
    for check in (
        "custom::credit_factor_matches_the_rating",
        "custom::credit_factor_is_monotone_in_the_grade",
    ):
        assert named[check]["passed"], named[check]


def test_the_factor_invariants_catch_a_break(tmp_path):
    """Negative control. A check that cannot fail is not a check."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)

    broken = copy.deepcopy(spec)
    rules = next(d for d in broken["derivations"] if d["target"] == "credit_factor")["rules"]
    rules.insert(0, {"if": "second_lien_flag == 'Y'", "then": 9999})
    result = api.run(broken, 250, tmp_path / "mismatch", seed=3, validate_output=True)
    named = {c["name"]: c for c in result["validation"]["checks"]}
    assert not named["custom::credit_factor_matches_the_rating"]["passed"]

    inverted = copy.deepcopy(spec)
    rules = next(d for d in inverted["derivations"] if d["target"] == "credit_factor")["rules"]
    for rule in rules:
        if "CCC-" in rule["if"]:
            rule["then"] = 100
    result = api.run(inverted, 250, tmp_path / "inverted", seed=3, validate_output=True)
    named = {c["name"]: c for c in result["validation"]["checks"]}
    assert not named["custom::credit_factor_is_monotone_in_the_grade"]["passed"]


def test_the_new_kinds_are_not_clo_specific(tmp_path):
    """The genericity claim, which is the only reason these are metric *kinds*.

    A car pool is diversified across manufacturers and a mortgage book across
    provinces. If `effective_count` and `turnover` only made sense for corporate
    obligors they would belong in the CLO pack as bespoke arithmetic, not in the
    metric vocabulary every pack draws on.
    """
    for pack, column in (
        ("auto_abs_esma_annex5", "effective_manufacturers"),
        ("rmbs_nl_green_lion", "effective_provinces"),
    ):
        result = api.run(pack, 300, tmp_path / pack, seed=5, validate_output=False)
        report = pd.DataFrame(result["metrics"])

        assert column in report.columns
        assert (report[column] > 1).all(), f"{pack}: {column} collapsed"
        assert report["portfolio_turnover"].fillna(0).sum() > 0, f"{pack}: no turnover measured"
