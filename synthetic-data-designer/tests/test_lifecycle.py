"""The state machine: transitions, hazards, dwell time, absorbing vs terminal.

Tests use deterministic matrices (probability 1.0 on a single cell) wherever the
point is *which* transition happens rather than how often, so a failure means a
logic bug and never an unlucky seed.
"""

from __future__ import annotations

import numpy as np
import pytest

from sdd.age.lifecycle import LifecycleEngine
from sdd.spec.schema import Lifecycle


def engine_from(**kwargs) -> LifecycleEngine:
    base = {
        "state_column": "state",
        "states": ["A", "B", "Gone"],
        "terminal": ["Gone"],
        "transitions": [[1.0, 0.0], [0.0, 1.0]],
    }
    base.update(kwargs)
    return LifecycleEngine(Lifecycle(**base), periods_per_year=12.0)


@pytest.fixture
def rng() -> np.random.Generator:
    return np.random.default_rng(0)


# ---------------------------------------------------------------------------
# compilation
# ---------------------------------------------------------------------------


def test_transition_states_default_to_non_terminal():
    eng = engine_from(
        hazards=[{"kind": "bernoulli", "name": "h", "period_rate": 0.0, "to_state": "Gone"}]
    )
    assert [eng.states[i] for i in eng.trans_state_idx] == ["A", "B"]


def test_label_and_index_round_trip():
    eng = engine_from(
        hazards=[{"kind": "bernoulli", "name": "h", "period_rate": 0.0, "to_state": "Gone"}]
    )
    labels = np.array(["A", "Gone", "B"], dtype=object)
    np.testing.assert_array_equal(eng.to_label(eng.to_idx(labels)), labels)


# ---------------------------------------------------------------------------
# matrix transitions
# ---------------------------------------------------------------------------


def test_deterministic_matrix_moves_everyone(rng):
    eng = engine_from(
        transitions=[[0.0, 1.0], [0.0, 1.0]],
        hazards=[{"kind": "bernoulli", "name": "h", "period_rate": 0.0, "to_state": "Gone"}],
    )
    state = np.zeros(100, dtype=np.int16)  # all in A
    new, _ = eng.step(state, {}, rng)
    assert (eng.to_label(new) == "B").all()


def test_transition_frequencies_match_the_matrix(rng):
    eng = engine_from(
        transitions=[[0.7, 0.3], [0.0, 1.0]],
        hazards=[{"kind": "bernoulli", "name": "h", "period_rate": 0.0, "to_state": "Gone"}],
    )
    state = np.zeros(20000, dtype=np.int16)
    new, _ = eng.step(state, {}, rng)
    assert abs((eng.to_label(new) == "B").mean() - 0.3) < 0.02


def test_terminal_rows_are_not_moved_by_the_matrix(rng):
    """A terminal state has no matrix row; it must be left exactly as it is."""
    eng = engine_from(
        transitions=[[0.0, 1.0], [0.0, 1.0]],
        hazards=[{"kind": "bernoulli", "name": "h", "period_rate": 1.0, "to_state": "Gone"}],
    )
    state = np.full(50, eng.index["Gone"], dtype=np.int16)
    new, _ = eng.step(state, {}, rng)
    assert (eng.to_label(new) == "Gone").all()


# ---------------------------------------------------------------------------
# Bernoulli hazards
# ---------------------------------------------------------------------------


def test_certain_hazard_fires_for_everyone(rng):
    eng = engine_from(
        hazards=[{"kind": "bernoulli", "name": "prepay", "period_rate": 1.0, "to_state": "Gone"}]
    )
    new, _ = eng.step(np.zeros(100, dtype=np.int16), {}, rng)
    assert (eng.to_label(new) == "Gone").all()


def test_hazard_pre_empts_the_matrix(rng):
    """Prepayment is decided before delinquency: a loan paid off in full this
    month never gets the chance to fall behind in it."""
    eng = engine_from(
        transitions=[[0.0, 1.0], [0.0, 1.0]],
        hazards=[{"kind": "bernoulli", "name": "prepay", "period_rate": 1.0, "to_state": "Gone"}],
    )
    new, _ = eng.step(np.zeros(100, dtype=np.int16), {}, rng)
    assert (eng.to_label(new) == "Gone").all()


def test_excluded_states_are_immune(rng):
    eng = engine_from(
        hazards=[
            {
                "kind": "bernoulli",
                "name": "prepay",
                "period_rate": 1.0,
                "to_state": "Gone",
                "excluded_states": ["B"],
            }
        ]
    )
    state = np.ones(100, dtype=np.int16)  # all in B
    new, _ = eng.step(state, {}, rng)
    assert (eng.to_label(new) == "B").all()


def test_from_states_restricts_eligibility(rng):
    eng = engine_from(
        hazards=[
            {
                "kind": "bernoulli",
                "name": "prepay",
                "period_rate": 1.0,
                "to_state": "Gone",
                "from_states": ["A"],
            }
        ]
    )
    state = np.array([0] * 50 + [1] * 50, dtype=np.int16)
    new, _ = eng.step(state, {}, rng)
    labels = eng.to_label(new)
    assert (labels[:50] == "Gone").all()
    assert (labels[50:] == "B").all()


def test_annual_rate_converts_to_a_period_rate(rng):
    eng = engine_from(
        hazards=[{"kind": "bernoulli", "name": "prepay", "annual_rate": 0.07, "to_state": "Gone"}]
    )
    new, _ = eng.step(np.zeros(50000, dtype=np.int16), {}, rng)
    # 7% a year compounds to ~0.605% a month.
    assert abs((eng.to_label(new) == "Gone").mean() - 0.00605) < 0.001


def test_hazard_multiplier_scales_the_rate(rng):
    """This is how a stress scenario slows refinancing without editing the spec."""
    eng = engine_from(
        hazards=[{"kind": "bernoulli", "name": "prepay", "period_rate": 0.10, "to_state": "Gone"}]
    )
    new, _ = eng.step(np.zeros(50000, dtype=np.int16), {}, rng, hazard_multipliers={"prepay": 0.5})
    assert abs((eng.to_label(new) == "Gone").mean() - 0.05) < 0.01


# ---------------------------------------------------------------------------
# dwell-time hazards
# ---------------------------------------------------------------------------


def test_dwell_time_fires_on_exactly_the_nth_period(rng):
    eng = engine_from(
        states=["A", "Stuck", "Gone"],
        terminal=["Gone"],
        absorbing=["Stuck"],
        transitions=[[0.0, 1.0], [0.0, 1.0]],
        hazards=[
            {
                "kind": "dwell_time",
                "name": "chargeoff",
                "from_state": "Stuck",
                "periods": 3,
                "to_state": "Gone",
            }
        ],
    )
    state = np.zeros(10, dtype=np.int16)
    dwell = eng.initial_dwell(10)
    seen = []
    for _ in range(4):
        state, dwell = eng.step(state, dwell, rng)
        seen.append(eng.to_label(state)[0])
    # Enters Stuck on step 1, counts 1, 2, 3 — and charges off on the third.
    assert seen == ["Stuck", "Stuck", "Gone", "Gone"]


def test_dwell_counter_resets_on_leaving_the_state(rng):
    eng = engine_from(
        states=["A", "Stuck", "Gone"],
        terminal=["Gone"],
        transitions=[[1.0, 0.0], [1.0, 0.0]],  # Stuck always cures back to A
        hazards=[
            {
                "kind": "dwell_time",
                "name": "chargeoff",
                "from_state": "Stuck",
                "periods": 2,
                "to_state": "Gone",
            }
        ],
    )
    state = np.full(10, eng.index["Stuck"], dtype=np.int16)
    dwell = eng.initial_dwell(10)
    for _ in range(5):
        state, dwell = eng.step(state, dwell, rng)
    # It cures every period, so the counter never reaches 2.
    assert (eng.to_label(state) == "A").all()
    assert (dwell["chargeoff"] == 0).all()


def test_dwell_length_is_the_same_however_the_state_was_entered(rng):
    """An entity already in the state at period 0 must not get a free extra
    period before the hazard fires."""
    eng = engine_from(
        states=["A", "Stuck", "Gone"],
        terminal=["Gone"],
        absorbing=["Stuck"],
        transitions=[[0.0, 1.0], [0.0, 1.0]],
        hazards=[
            {
                "kind": "dwell_time",
                "name": "chargeoff",
                "from_state": "Stuck",
                "periods": 3,
                "to_state": "Gone",
            }
        ],
    )
    # Started in Stuck at period 0, so its counter is seeded to 1.
    started_stuck = np.full(1, eng.index["Stuck"], dtype=np.int16)
    dwell = eng.initial_dwell(1, started_stuck)
    assert dwell["chargeoff"][0] == 1

    visible = ["Stuck"]  # the period-0 row
    state = started_stuck
    for _ in range(4):
        state, dwell = eng.step(state, dwell, rng)
        visible.append(eng.to_label(state)[0])
    # Two more Stuck rows (counters 2 and 3 fire on the third), then terminal.
    assert visible[:3] == ["Stuck", "Stuck", "Gone"]


def test_absorbing_state_is_never_left(rng):
    eng = engine_from(
        states=["A", "Stuck", "Gone"],
        terminal=["Gone"],
        absorbing=["Stuck"],
        transitions=[[0.5, 0.5], [0.0, 1.0]],
        hazards=[
            {
                "kind": "bernoulli",
                "name": "prepay",
                "period_rate": 1.0,
                "to_state": "Gone",
                "excluded_states": ["Stuck"],
            }
        ],
    )
    state = np.full(200, eng.index["Stuck"], dtype=np.int16)
    for _ in range(5):
        state, _ = eng.step(state, {}, rng)
    assert (eng.to_label(state) == "Stuck").all()
