"""A second state machine running alongside the lifecycle.

Credit ratings are the case this exists for. A rating is a forward-looking
opinion about whether a borrower can pay, not a record of whether it has, so it
moves while a facility is performing perfectly — and it normally moves *first*.
A company is downgraded from B to B- with every instalment paid on time, and
that downgrade is the early warning the rating exists to give.

Derived from the credit state, as the CLO pack did until now, a rating can only
ever agree with what is already obvious. The measurable symptom was that the CCC
share came out *exactly* equal to the distressed share in all three scenarios.
Every CLO indenture caps CCC assets, and a bucket that only fills once
facilities are visibly distressed cannot be used to test that cap.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd import api
from sdd.age.lifecycle import _scale_worse
from sdd.spec.schema import ChainCoupling, Lifecycle, SecondaryChain

PACK = "clo_eu_leveraged_loans"
CCC = {"CCC+", "CCC", "CCC-"}


@pytest.fixture(scope="module")
def panel(tmp_path_factory):
    result = api.run(PACK, 1200, tmp_path_factory.mktemp("chain"), seed=7)
    return result, pd.read_parquet(result["panel"]).sort_values(["facility_id", "reporting_date"])


# ---------------------------------------------------------------------------
# the chain itself
# ---------------------------------------------------------------------------


def test_the_run_still_validates(panel):
    result, _ = panel
    assert result["validation"]["passed"], [
        c["name"] for c in result["validation"]["checks"] if not c["passed"]
    ]


def test_ratings_migrate_on_their_own(panel):
    _, frame = panel
    moved = frame.groupby("facility_id")["rating_at_cutoff"].nunique()
    assert (moved > 1).mean() > 0.5, "most facilities never saw a rating change"


def test_every_grade_is_reachable(panel):
    _, frame = panel
    grades = set(frame["rating_at_cutoff"])
    assert {"BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "D"} <= grades


def test_a_rating_moves_while_the_facility_performs(panel):
    """The property a derived rating cannot have.

    A downgrade with every instalment paid on time is the ordinary case, and it
    is the one worth pinning: it is what makes the rating informative rather
    than a restatement of the credit state.
    """
    _, frame = panel
    performing = frame[frame["credit_state"] == "Performing"]
    changed = performing.groupby("facility_id")["rating_at_cutoff"].nunique()
    assert (changed > 1).any(), "no rating ever moved while the facility was performing"


# ---------------------------------------------------------------------------
# coupling
# ---------------------------------------------------------------------------


def test_a_defaulted_facility_is_rated_d(panel):
    """The direction with no ambiguity: the credit state simply overrules."""
    _, frame = panel
    bad = frame[frame["credit_state"].isin(["Defaulted", "Recovered"])]
    assert not bad.empty
    assert (bad["rating_at_cutoff"] == "D").all()


def test_a_worse_rating_makes_distress_more_likely(panel):
    """The direction carrying the modelling judgement.

    Measured as: given a performing facility, how often does it worsen next
    period? The pack stresses CCC+ at 2.0, CCC at 3.2 and CCC- at 4.5, and the
    observed rates should climb accordingly.
    """
    _, frame = panel
    frame = frame.copy()
    frame["next_state"] = frame.groupby("facility_id")["credit_state"].shift(-1)
    live = frame[frame["credit_state"] == "Performing"].dropna(subset=["next_state"])

    def worsening_rate(grades):
        subset = live[live["rating_at_cutoff"].isin(grades)]
        if len(subset) < 100:
            pytest.skip("too few observations to measure a rate")
        return subset["next_state"].isin(["Watchlist", "Distressed", "Defaulted"]).mean()

    healthy = worsening_rate({"BB", "BB-", "B+", "B"})
    stressed = worsening_rate(CCC)
    assert stressed > healthy * 1.5, (
        f"CCC facilities worsened at {stressed:.2%} against {healthy:.2%} for healthy "
        "ones; the upward coupling is not reaching the transition matrix"
    )


def test_the_rating_leads_the_distress(panel):
    """The early warning, which is the whole point.

    For facilities that reach CCC and later become distressed, the downgrade
    should usually come first — otherwise the rating is reporting history.
    """
    _, frame = panel
    first_ccc, first_bad = [], []
    for _, group in frame.groupby("facility_id"):
        group = group.reset_index(drop=True)
        ccc = group.index[group["rating_at_cutoff"].isin(CCC)]
        bad = group.index[group["credit_state"].isin(["Distressed", "Defaulted"])]
        if len(ccc) and len(bad):
            first_ccc.append(ccc[0])
            first_bad.append(bad[0])

    assert len(first_ccc) > 20, "too few facilities did both to judge"
    led = sum(1 for c, b in zip(first_ccc, first_bad, strict=True) if b > c)
    assert led / len(first_ccc) > 0.6, (
        f"the rating led the distress in only {led}/{len(first_ccc)} cases"
    )


def test_the_ccc_share_is_no_longer_the_distressed_share(panel):
    """The symptom that made the derived rating unusable.

    They matched to two decimal places in every scenario, because one was
    computed from the other.
    """
    _, frame = panel
    ccc = (frame["ccc_flag"] == "Y").mean()
    distressed = frame["credit_state"].isin(["Distressed", "Defaulted", "Recovered"]).mean()
    assert abs(ccc - distressed) > 0.01, (
        f"CCC {ccc:.4f} and distress {distressed:.4f} still move together"
    )


# ---------------------------------------------------------------------------
# the per-entity stress maths
# ---------------------------------------------------------------------------


def test_scaling_worsening_keeps_each_row_a_distribution():
    rows = np.array([[0.96, 0.03, 0.008, 0.002], [0.16, 0.70, 0.10, 0.04]])
    source = np.array([0, 1])
    for multiplier in (0.5, 1.0, 3.0, 25.0):
        out = _scale_worse(rows, source, np.full(2, multiplier))
        assert np.allclose(out.sum(axis=1), 1.0)
        assert (out >= 0).all()


def test_scaling_moves_mass_in_the_right_direction():
    rows = np.array([[0.96, 0.03, 0.008, 0.002]])
    source = np.array([0])
    base = _scale_worse(rows, source, np.ones(1))[0, 1:].sum()
    tripled = _scale_worse(rows, source, np.full(1, 3.0))[0, 1:].sum()
    assert tripled == pytest.approx(base * 3, rel=1e-6)


def test_entities_in_the_same_state_can_face_different_odds():
    """The reason this is per entity rather than per state."""
    rows = np.array([[0.96, 0.03, 0.008, 0.002]] * 2)
    out = _scale_worse(rows, np.array([0, 0]), np.array([1.0, 4.0]))
    assert out[1, 1:].sum() > out[0, 1:].sum() * 3


# ---------------------------------------------------------------------------
# spec validation
# ---------------------------------------------------------------------------


def _chain(**coupling):
    lifecycle = Lifecycle(
        state_column="grade",
        states=["A", "B"],
        transitions=[[0.9, 0.1], [0.05, 0.95]],
    )
    return SecondaryChain(name="g", lifecycle=lifecycle, coupling=ChainCoupling(**coupling))


def test_a_chain_cannot_end_a_life():
    """Only the lifecycle removes entities.

    A chain that could would silently drop rows the lifecycle still holds, and
    the panel would lose facilities for a reason nothing else could see.
    """
    with pytest.raises(ValueError, match="terminal"):
        SecondaryChain(
            name="g",
            lifecycle=Lifecycle(
                state_column="grade",
                states=["A", "B"],
                terminal=["B"],
                transitions=[[1.0]],
            ),
        )


def test_coupling_must_name_states_the_chain_has():
    with pytest.raises(ValueError, match="unknown"):
        _chain(forced_by={"Defaulted": "Z"})
    with pytest.raises(ValueError, match="unknown"):
        _chain(stress={"Z": 2.0})


def test_a_negative_stress_multiplier_is_refused():
    with pytest.raises(ValueError, match="negative"):
        _chain(stress={"B": -1.0})


def test_packs_without_a_chain_are_untouched(tmp_path):
    for pack in ("auto_abs_esma_annex5", "rmbs_nl_green_lion"):
        assert api.load(pack).secondary_chains == []
        a = api.run(pack, 200, tmp_path / f"{pack}_a", seed=5, validate_output=False)
        b = api.run(pack, 200, tmp_path / f"{pack}_b", seed=5, validate_output=False)
        assert pd.read_parquet(a["panel"]).equals(pd.read_parquet(b["panel"]))


def test_the_run_is_still_reproducible(tmp_path):
    a = api.run(PACK, 300, tmp_path / "a", seed=11, validate_output=False)
    b = api.run(PACK, 300, tmp_path / "b", seed=11, validate_output=False)
    assert pd.read_parquet(a["panel"]).equals(pd.read_parquet(b["panel"]))
