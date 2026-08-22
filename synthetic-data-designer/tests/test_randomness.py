"""The four randomness controls.

Each one has a property that must hold whatever the data looks like, and those
properties are what these tests pin. The controls exist to make output *less*
pristine, so the risk is not that they do nothing — it is that they quietly
break something the spec promises.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.generate.randomness import (
    add_noise,
    add_outliers,
    apply_randomness,
    blank_values,
    blankable_columns,
    impose_correlation,
    numeric_targets,
)
from sdd.spec import load_spec_dict


def _spec(**generation):
    """A minimal two-column spec with a lifecycle, as a dict the loader accepts."""
    return load_spec_dict(
        {
            "meta": {"name": "t"},
            "entity": {
                "id_column": "id",
                "time_column": "asof",
                "calendar": {"start": "2024-01-31", "periods": 3},
            },
            "columns": [
                {
                    "name": "id",
                    "role": "static",
                    "dtype": "str",
                    "generator": {"kind": "sequence", "prefix": "L"},
                },
                {
                    "name": "asof",
                    "role": "dynamic",
                    "dtype": "str",
                    "generator": {"kind": "constant", "value": "2024-01-31"},
                },
                {
                    "name": "balance",
                    "role": "static",
                    "dtype": "float",
                    "min": 0.0,
                    "max": 1000.0,
                    "generator": {"kind": "uniform", "low": 100, "high": 900},
                },
                {
                    "name": "income",
                    "role": "static",
                    "dtype": "float",
                    "required": False,
                    "generator": {"kind": "uniform", "low": 10, "high": 90},
                },
            ],
            "generation": generation,
        }
    )


def _frame(n=500, seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "id": [f"L{i:05d}" for i in range(n)],
            "asof": "2024-01-31",
            "balance": rng.uniform(100, 900, n),
            "income": rng.uniform(10, 90, n),
        }
    )


# ---------------------------------------------------------------------------
# what may be touched
# ---------------------------------------------------------------------------


def test_identifiers_and_dates_are_never_touched():
    spec, df = _spec(), _frame()
    assert numeric_targets(spec, df) == ["balance", "income"]
    assert blankable_columns(spec) == ["income"]


def test_engine_inputs_are_protected_from_randomness():
    """Jittering the column amortisation reads would make balances wander."""
    spec = _spec()
    spec.dynamics.amortisation = None
    spec = load_spec_dict(
        {
            **spec.model_dump(mode="json", exclude_none=True, by_alias=True),
            "dynamics": {
                "amortisation": {"kind": "linear", "balance": "balance", "payment": "income"}
            },
        }
    )
    assert numeric_targets(spec, _frame()) == []


# ---------------------------------------------------------------------------
# noise and outliers
# ---------------------------------------------------------------------------


def test_noise_moves_values_without_moving_the_mean():
    spec, df = _spec(noise=0.1), _frame()
    before = df["balance"].to_numpy().copy()
    add_noise(spec, df, ["balance"], np.random.default_rng(1))
    after = df["balance"].to_numpy()

    assert not np.allclose(before, after)
    # Zero-centred jitter: the level is preserved, the spread grows.
    assert abs(after.mean() - before.mean()) < before.std() * 0.05
    assert after.std() > before.std()


def test_noise_respects_a_declared_bound():
    """A balance that cannot be negative must not become negative through noise."""
    spec, df = _spec(noise=0.9), _frame()
    add_noise(spec, df, ["balance"], np.random.default_rng(2))
    assert df["balance"].min() >= 0.0
    assert df["balance"].max() <= 1000.0


def test_outliers_reach_the_requested_share_of_rows():
    spec, df = _spec(outliers=0.02, outlier_sigma=4.0), _frame()
    before = df["income"].to_numpy().copy()
    add_outliers(spec, df, ["income"], np.random.default_rng(3))

    moved = np.sum(~np.isclose(before, df["income"].to_numpy()))
    assert moved == pytest.approx(len(df) * 0.02, abs=1)
    # And they are genuinely in the tail, not merely different.
    assert df["income"].max() > before.max()


def test_zero_settings_change_nothing():
    spec, df = _spec(), _frame()
    original = df.copy()
    apply_randomness(spec, df, np.random.default_rng(4))
    pd.testing.assert_frame_equal(df, original)


# ---------------------------------------------------------------------------
# missing values
# ---------------------------------------------------------------------------


def test_only_optional_columns_are_blanked():
    spec, df = _spec(missing=0.3), _frame()
    blank_values(spec, df, np.random.default_rng(5))

    assert df["income"].isna().mean() == pytest.approx(0.3, abs=0.06)
    assert df["balance"].notna().all()
    assert df["id"].notna().all()


def test_a_per_column_rate_overrides_the_global_one():
    spec = _spec(missing=0.5)
    spec.column("income").null_rate = 0.05
    df = _frame()
    blank_values(spec, df, np.random.default_rng(6))
    assert df["income"].isna().mean() == pytest.approx(0.05, abs=0.03)


# ---------------------------------------------------------------------------
# correlation
# ---------------------------------------------------------------------------


def test_correlation_is_imposed_without_changing_any_marginal():
    """The whole point of reordering: the joint changes, the marginals cannot."""
    spec = _spec(
        correlation=1.0,
        correlation_target={
            "columns": ["balance", "income"],
            "matrix": [[1.0, 0.8], [0.8, 1.0]],
        },
    )
    df = _frame(2000)
    before = {c: np.sort(df[c].to_numpy()) for c in ("balance", "income")}

    touched = impose_correlation(spec, df, np.random.default_rng(7))
    assert touched == 2

    achieved = df["balance"].corr(df["income"], method="spearman")
    assert achieved == pytest.approx(0.8, abs=0.06)
    for column, values in before.items():
        np.testing.assert_allclose(np.sort(df[column].to_numpy()), values)


def test_the_correlation_control_scales_continuously():
    target = {"columns": ["balance", "income"], "matrix": [[1.0, 0.9], [0.9, 1.0]]}
    achieved = []
    for strength in (0.0, 0.5, 1.0):
        spec = _spec(correlation=strength, correlation_target=target)
        df = _frame(2000)
        impose_correlation(spec, df, np.random.default_rng(8))
        achieved.append(df["balance"].corr(df["income"], method="spearman"))

    assert achieved[0] == pytest.approx(0.0, abs=0.08)
    assert achieved[1] == pytest.approx(0.45, abs=0.08)
    assert achieved[2] == pytest.approx(0.9, abs=0.06)


def test_a_target_that_is_not_a_valid_correlation_matrix_is_repaired():
    """Pairwise estimates from real data need not be positive definite."""
    spec = _spec(
        correlation=1.0,
        correlation_target={
            "columns": ["balance", "income"],
            "matrix": [[1.0, 0.999999], [0.999999, 1.0]],
        },
    )
    df = _frame(400)
    assert impose_correlation(spec, df, np.random.default_rng(9)) == 2
