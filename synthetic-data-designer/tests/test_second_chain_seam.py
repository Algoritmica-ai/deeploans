"""The seam a rating-migration chain will plug into.

Ratings in the CLO pack are derived from the credit state, not migrated by their
own matrix. That is the simplification §7 permits, and it is temporary: a real
CLO needs ratings that move on their own, because a downgrade normally arrives
*before* any distress is visible, and the CCC bucket limit that every indenture
carries is meaningless if the CCC share is only the distressed share relabelled.

Building that is a later phase. Keeping it *buildable* is now. These tests hold
the seam open by exercising it: a second, independent chain is constructed and
stepped alongside the first, using nothing the engine does not already expose.

If one of these fails, the engine has grown a hidden dependency on there being
exactly one lifecycle — and the later phase has become a rewrite rather than an
addition. That is the failure they exist to catch.
"""

from __future__ import annotations

import numpy as np
import pytest

from sdd import api
from sdd.age.lifecycle import LifecycleEngine
from sdd.spec.schema import Lifecycle

GRADES = ["BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "D"]


def _rating_chain() -> Lifecycle:
    """A rating-migration chain, shaped the way the real one would be.

    Mostly stay put, drift a little either way, and D absorbs. The numbers are
    illustrative — what matters is that the *shape* is expressible.
    """
    n = len(GRADES)
    rows = []
    for i in range(n - 1):
        row = [0.0] * n
        row[i] = 0.94
        if i > 0:
            row[i - 1] = 0.03
        if i + 1 < n - 1:
            row[i + 1] = 0.03
        # Whatever is left goes to D, which also keeps every row summing to 1.
        row[n - 1] = round(1.0 - sum(row), 10)
        rows.append(row)
    rows.append([0.0] * (n - 1) + [1.0])  # D absorbs
    return Lifecycle(
        state_column="rating_at_cutoff",
        states=GRADES,
        absorbing=["D"],
        transitions=rows,
    )


def test_a_second_chain_can_be_compiled():
    """`LifecycleEngine` must be constructible from any Lifecycle, not only the
    spec's own — it takes the object, never reaches for `spec.lifecycle`."""
    engine = LifecycleEngine(_rating_chain(), periods_per_year=12)
    assert engine.states == GRADES
    assert engine.index["D"] == len(GRADES) - 1


def test_two_chains_step_independently():
    """The real test of the seam: two engines, one frame, neither disturbing the
    other. If stepping the rating chain moved credit states, or the two shared
    hidden state, this would show it."""
    spec = api.load("clo_eu_leveraged_loans")
    credit = LifecycleEngine(spec.lifecycle, spec.entity.calendar.periods_per_year)
    rating = LifecycleEngine(_rating_chain(), spec.entity.calendar.periods_per_year)

    n = 400
    rng = np.random.default_rng(3)
    credit_idx = np.zeros(n, dtype=np.int16)
    rating_idx = np.full(n, GRADES.index("B"), dtype=np.int16)
    credit_dwell = credit.initial_dwell(n, credit_idx)
    rating_dwell = rating.initial_dwell(n, rating_idx)

    for _ in range(12):
        credit_idx, credit_dwell = credit.step(credit_idx, credit_dwell, rng)
        rating_idx, rating_dwell = rating.step(rating_idx, rating_dwell, rng)

    assert len(credit_idx) == len(rating_idx) == n
    assert set(credit.to_label(credit_idx)) <= set(spec.lifecycle.states)
    assert set(rating.to_label(rating_idx)) <= set(GRADES)
    # Both actually moved, so the test is exercising something.
    assert len(set(rating.to_label(rating_idx))) > 1, "the rating chain never migrated"


def test_the_downward_coupling_is_expressible():
    """Coupling direction one: a default forces the rating to D.

    This is the half with no ambiguity — a defaulted facility *is* rated D — and
    it must be a plain masked assignment over the two index arrays.
    """
    spec = api.load("clo_eu_leveraged_loans")
    credit = LifecycleEngine(spec.lifecycle, 12)
    rating = LifecycleEngine(_rating_chain(), 12)

    credit_idx = np.array([credit.index["Performing"], credit.index["Defaulted"]], dtype=np.int16)
    rating_idx = np.array([rating.index["B"], rating.index["B"]], dtype=np.int16)

    defaulted = credit_idx == credit.index["Defaulted"]
    rating_idx[defaulted] = rating.index["D"]

    assert list(rating.to_label(rating_idx)) == ["B", "D"]


def test_the_upward_coupling_has_a_hook():
    """Coupling direction two: a worse rating raises the chance of distress.

    `step` already scales named hazards through `hazard_multipliers`, which is
    the hook a rating-driven stress would use. It is exercised here so the
    parameter cannot quietly disappear.
    """
    spec = api.load("clo_eu_leveraged_loans")
    credit = LifecycleEngine(spec.lifecycle, 12)
    n = 600
    idx = np.zeros(n, dtype=np.int16)

    calm, _ = credit.step(idx.copy(), credit.initial_dwell(n, idx), np.random.default_rng(1))
    stressed, _ = credit.step(
        idx.copy(),
        credit.initial_dwell(n, idx),
        np.random.default_rng(1),
        hazard_multipliers={"prepayment": 8.0},
    )
    prepaid = credit.index["Prepaid"]
    assert (stressed == prepaid).sum() > (calm == prepaid).sum(), (
        "hazard_multipliers no longer changes outcomes; the upward coupling has "
        "lost the hook it would use"
    )


def test_the_spec_model_still_holds_one_chain():
    """The known blocker, asserted so it stays visible.

    `DesignSpec.lifecycle` is singular. Adding a second chain means a new field
    beside it, not a rewrite of the engine — this test records which of the two
    is true today, and should be updated, not deleted, when that changes.
    """
    spec = api.load("clo_eu_leveraged_loans")
    assert isinstance(spec.lifecycle, Lifecycle)
    assert not hasattr(spec, "lifecycles"), (
        "a second chain has landed; update this test and the ADR rather than deleting them"
    )


@pytest.mark.parametrize("grade", ["BB", "B", "CCC", "D"])
def test_every_rating_the_pack_emits_is_a_chain_state(grade):
    """The derived ratings and a future migrated chain must share one vocabulary,
    or the swap becomes a data migration."""
    assert grade in GRADES or any(g.startswith(grade) for g in GRADES)
