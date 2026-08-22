"""Fidelity scoring.

Two properties matter and pull against each other: the report must not fire on
two samples from the same generator (or it gets ignored), and it must fire on a
real difference (or it is useless). Both are tested here on hand-built
distributions where the right answer is known.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.validate.fidelity import (
    compare,
    correlation_delta,
    ks_distance,
    ks_noise_floor,
    transition_delta,
    transition_matrix,
    tv_distance,
    tv_noise_floor,
)


@pytest.fixture
def rng():
    return np.random.default_rng(0)


# ---------------------------------------------------------------------------
# the measures themselves
# ---------------------------------------------------------------------------


def test_ks_is_zero_for_identical_data():
    s = pd.Series(np.arange(1000, dtype=float))
    assert ks_distance(s, s) == 0.0


def test_ks_is_one_for_non_overlapping_data():
    a = pd.Series(np.arange(1000, dtype=float))
    b = pd.Series(np.arange(1000, 2000, dtype=float))
    assert ks_distance(a, b) == 1.0


def test_ks_grows_with_the_size_of_a_shift(rng):
    base = pd.Series(rng.normal(0, 1, 5000))
    small = pd.Series(rng.normal(0.1, 1, 5000))
    large = pd.Series(rng.normal(1.0, 1, 5000))
    assert ks_distance(base, small) < ks_distance(base, large)


def test_ks_returns_nan_when_there_is_too_little_data():
    a = pd.Series([1.0, 2.0, 3.0])
    assert np.isnan(ks_distance(a, a))


def test_tv_is_zero_for_identical_mixes():
    s = pd.Series(["a"] * 70 + ["b"] * 30)
    assert tv_distance(s, s) == 0.0


def test_tv_is_one_for_disjoint_categories():
    a = pd.Series(["a"] * 100)
    b = pd.Series(["z"] * 100)
    assert tv_distance(a, b) == pytest.approx(1.0)


def test_tv_measures_the_share_that_would_have_to_move():
    a = pd.Series(["x"] * 50 + ["y"] * 50)
    b = pd.Series(["x"] * 60 + ["y"] * 40)
    assert tv_distance(a, b) == pytest.approx(0.10)


def test_tv_counts_an_invented_category_in_full():
    a = pd.Series(["x"] * 100)
    b = pd.Series(["x"] * 90 + ["surprise"] * 10)
    assert tv_distance(a, b) == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# noise floors — the thing that makes the thresholds usable
# ---------------------------------------------------------------------------


def test_ks_noise_floor_shrinks_as_rows_grow():
    assert ks_noise_floor(1_000, 1_000) > ks_noise_floor(100_000, 100_000)


def test_tv_noise_floor_grows_with_category_count():
    """The reason a single flat threshold cannot work across column types."""
    few = pd.Series(["a"] * 500 + ["b"] * 500)
    many = pd.Series([f"c{i % 40}" for i in range(1000)])
    assert tv_noise_floor(many, 1000, 1000) > tv_noise_floor(few, 1000, 1000)


def test_noise_floor_covers_same_distribution_draws(rng):
    """Two draws from one distribution should land inside the floor."""
    floor = None
    breaches = 0
    for _ in range(40):
        a = pd.Series(rng.choice(list("abcdefghij"), size=4000))
        b = pd.Series(rng.choice(list("abcdefghij"), size=4000))
        floor = tv_noise_floor(a, 4000, 4000)
        if tv_distance(a, b) > floor:
            breaches += 1
    assert breaches <= 2, f"{breaches}/40 same-distribution pairs breached the floor"


# ---------------------------------------------------------------------------
# correlation
# ---------------------------------------------------------------------------


def test_correlation_delta_is_zero_for_the_same_frame(rng):
    df = pd.DataFrame({"a": rng.normal(size=500), "b": rng.normal(size=500)})
    delta, _, _ = correlation_delta(df, df, ["a", "b"])
    assert delta == pytest.approx(0.0, abs=1e-12)


def test_correlation_delta_catches_a_broken_relationship(rng):
    a = rng.normal(size=4000)
    linked = pd.DataFrame({"x": a, "y": a * 2 + rng.normal(0, 0.1, 4000)})
    broken = pd.DataFrame({"x": a, "y": rng.normal(size=4000)})
    delta, _, _ = correlation_delta(linked, broken, ["x", "y"])
    assert delta > 0.5


def test_sparse_columns_are_excluded_from_correlation(rng):
    """A column that is one value 96% of the time has no stable correlation."""
    n = 2000
    sparse = np.where(rng.random(n) < 0.04, rng.normal(size=n), 0.0)
    df = pd.DataFrame({"dense": rng.normal(size=n), "other": rng.normal(size=n), "sparse": sparse})
    _, excluded, n_used = correlation_delta(df, df, list(df.columns))
    assert excluded == ["sparse"]
    assert n_used == 2


def test_correlation_is_skipped_when_too_few_columns_remain(rng):
    df = pd.DataFrame({"only": rng.normal(size=100)})
    delta, _, _ = correlation_delta(df, df, ["only"])
    assert delta is None


# ---------------------------------------------------------------------------
# transitions
# ---------------------------------------------------------------------------


def _panel(states: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["L1"] * len(states),
            "t": [f"2024-{i + 1:02d}-01" for i in range(len(states))],
            "state": states,
        }
    )


def test_transition_matrix_counts_observed_moves():
    matrix, states = transition_matrix(_panel(["A", "A", "B", "B", "A"]), "id", "t", "state")
    assert states == ["A", "B"]
    # A -> A once, A -> B once; B -> B once, B -> A once.
    assert matrix.loc["A", "A"] == pytest.approx(0.5)
    assert matrix.loc["B", "A"] == pytest.approx(0.5)


def test_transition_rows_sum_to_one():
    matrix, _ = transition_matrix(_panel(["A", "B", "A", "B", "A"]), "id", "t", "state")
    np.testing.assert_allclose(matrix.sum(axis=1), 1.0)


def test_transition_delta_catches_different_dynamics():
    """Two panels can share a state mix and behave completely differently."""
    sticky = _panel(["A", "A", "A", "A", "B", "B", "B", "B"])
    flappy = _panel(["A", "B", "A", "B", "A", "B", "A", "B"])
    delta, _floor = transition_delta(sticky, flappy, "id", "t", "state")
    assert delta is not None and delta > 0.5


def test_transition_delta_is_none_without_the_needed_columns():
    df = pd.DataFrame({"a": [1, 2]})
    delta, _floor = transition_delta(df, df, "id", "t", "state")
    assert delta is None


def test_the_transition_floor_shrinks_as_evidence_grows(rng):
    """A matrix row resting on 40 observations cannot be judged as tightly as
    one resting on 40,000 — which is why a flat threshold reports on how rare a
    state is rather than on whether the dynamics match."""

    def panel(n_entities: int) -> pd.DataFrame:
        rows = []
        for i in range(n_entities):
            state = "A"
            for period in range(6):
                rows.append({"id": f"L{i}", "t": f"2024-{period + 1:02d}-28", "state": state})
                # A genuine 30% chance of switching, so cells sit away from 0/1
                # where the binomial standard error would vanish.
                if rng.random() < 0.3:
                    state = "B" if state == "A" else "A"
        return pd.DataFrame(rows)

    _delta, small = transition_delta(panel(20), panel(20), "id", "t", "state")
    _delta, large = transition_delta(panel(3000), panel(3000), "id", "t", "state")
    assert small > large


# ---------------------------------------------------------------------------
# the whole comparison
# ---------------------------------------------------------------------------


@pytest.fixture
def sample(rng) -> pd.DataFrame:
    n = 6000
    return pd.DataFrame(
        {
            "loan_id": [f"L{i}" for i in range(n)],
            "balance": rng.lognormal(12.0, 0.4, n),
            "rate": rng.normal(3.1, 0.65, n),
            "region": rng.choice(["N", "S", "E", "W"], size=n, p=[0.4, 0.3, 0.2, 0.1]),
            "flag": rng.choice(["Y", "N"], size=n, p=[0.45, 0.55]),
        }
    )


def test_a_second_draw_from_the_same_process_passes(sample, rng):
    n = len(sample)
    twin = pd.DataFrame(
        {
            "loan_id": [f"X{i}" for i in range(n)],
            "balance": rng.lognormal(12.0, 0.4, n),
            "rate": rng.normal(3.1, 0.65, n),
            "region": rng.choice(["N", "S", "E", "W"], size=n, p=[0.4, 0.3, 0.2, 0.1]),
            "flag": rng.choice(["Y", "N"], size=n, p=[0.45, 0.55]),
        }
    )
    report = compare(sample, twin, id_column="loan_id")
    assert report.passed, report.summary()


def test_a_shifted_numeric_column_fails(sample):
    shifted = sample.assign(balance=sample["balance"] * 1.4)
    report = compare(sample, shifted, id_column="loan_id")
    assert not report.passed
    assert "balance" in [c.column for c in report.failures]


def test_a_shifted_category_mix_fails(sample):
    flipped = sample.assign(region=sample["region"].replace({"N": "S"}))
    report = compare(sample, flipped, id_column="loan_id")
    assert not report.passed
    assert "region" in [c.column for c in report.failures]


def test_the_id_column_is_never_compared(sample):
    """Identifiers are unique by construction, so any distance test on them is
    meaningless and would fail every time."""
    report = compare(sample, sample, id_column="loan_id")
    assert "loan_id" not in [c.column for c in report.columns]


def test_a_missing_column_is_reported_as_skipped(sample):
    report = compare(sample, sample.drop(columns=["rate"]), id_column="loan_id")
    assert "rate" in report.skipped


def test_report_serialises_for_the_api(sample):
    payload = compare(sample, sample, id_column="loan_id").to_dict()
    assert payload["passed"] is True
    assert payload["columns_compared"] > 0
    assert all("distance" in row for row in payload["detail"])


def test_summary_names_the_worst_column(sample):
    shifted = sample.assign(balance=sample["balance"] * 2.0)
    assert "balance" in compare(sample, shifted, id_column="loan_id").summary()
