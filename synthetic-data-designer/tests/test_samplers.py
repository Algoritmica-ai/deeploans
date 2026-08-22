"""Every sampler kind, plus reproducibility."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.generate.samplers import SamplingError, sample
from sdd.spec.schema import (
    BernoulliGen,
    CategoricalGen,
    ConditionalCategoricalGen,
    ConstantGen,
    EmpiricalGen,
    GaussianGen,
    ScipyGen,
    SequenceGen,
    UniformGen,
    UUIDGen,
)

N = 4000


@pytest.fixture
def rng() -> np.random.Generator:
    return np.random.default_rng(7)


@pytest.fixture
def empty_ctx() -> pd.DataFrame:
    return pd.DataFrame(index=pd.RangeIndex(N))


def test_categorical_respects_weights(rng, empty_ctx):
    gen = CategoricalGen(values=["a", "b"], weights=[0.8, 0.2])
    out = sample(gen, N, rng, empty_ctx)
    assert set(out) == {"a", "b"}
    assert abs((out == "a").mean() - 0.8) < 0.03


def test_categorical_weights_need_not_sum_to_one(rng, empty_ctx):
    """Weights are normalised, so 8/2 behaves the same as 0.8/0.2."""
    gen = CategoricalGen(values=["a", "b"], weights=[8, 2])
    out = sample(gen, N, rng, empty_ctx)
    assert abs((out == "a").mean() - 0.8) < 0.03


def test_categorical_without_weights_is_uniform(rng, empty_ctx):
    out = sample(CategoricalGen(values=[1, 2, 3, 4]), N, rng, empty_ctx)
    assert abs((out == 1).mean() - 0.25) < 0.03


def test_conditional_categorical_stays_inside_its_parent_pool(rng):
    ctx = pd.DataFrame({"region": ["North"] * 2000 + ["South"] * 2000})
    gen = ConditionalCategoricalGen(
        parent="region", mapping={"North": ["N1", "N2"], "South": ["S1"]}
    )
    out = sample(gen, len(ctx), rng, ctx)
    north = out[ctx["region"].to_numpy() == "North"]
    south = out[ctx["region"].to_numpy() == "South"]
    assert set(north) <= {"N1", "N2"}
    assert set(south) == {"S1"}


def test_conditional_categorical_needs_its_parent_first(rng, empty_ctx):
    gen = ConditionalCategoricalGen(parent="missing", mapping={"a": ["x"]})
    with pytest.raises(SamplingError, match="sampled first"):
        sample(gen, N, rng, empty_ctx)


def test_conditional_categorical_falls_back_to_default(rng):
    ctx = pd.DataFrame({"region": ["Unmapped"] * 10})
    gen = ConditionalCategoricalGen(
        parent="region", mapping={"North": ["N1"]}, default=["FALLBACK"]
    )
    assert set(sample(gen, 10, rng, ctx)) == {"FALLBACK"}


def test_conditional_categorical_without_default_is_an_error(rng):
    ctx = pd.DataFrame({"region": ["Unmapped"] * 10})
    gen = ConditionalCategoricalGen(parent="region", mapping={"North": ["N1"]})
    with pytest.raises(SamplingError, match="no mapping"):
        sample(gen, 10, rng, ctx)


def test_scipy_lognormal_has_the_requested_median(rng, empty_ctx):
    gen = ScipyGen(dist="lognorm", params={"s": 0.4, "scale": 300000.0}, decimals=2)
    out = sample(gen, N, rng, empty_ctx)
    assert 285_000 < np.median(out) < 315_000
    assert (out > 0).all()


def test_scipy_unknown_distribution_is_named_in_the_error(rng, empty_ctx):
    with pytest.raises(SamplingError, match="no distribution named 'not_a_dist'"):
        sample(ScipyGen(dist="not_a_dist"), N, rng, empty_ctx)


def test_scipy_bad_params_are_reported(rng, empty_ctx):
    with pytest.raises(SamplingError, match="rejected params"):
        sample(ScipyGen(dist="lognorm", params={"nonsense": 1.0}), N, rng, empty_ctx)


def test_gaussian_and_clipping(rng, empty_ctx):
    gen = GaussianGen(mean=3.1, stddev=0.65, decimals=2, clip_min=1.0, clip_max=5.0)
    out = sample(gen, N, rng, empty_ctx)
    assert abs(out.mean() - 3.1) < 0.1
    assert out.min() >= 1.0 and out.max() <= 5.0


def test_uniform_decimals_zero_yields_integers(rng, empty_ctx):
    out = sample(UniformGen(low=10, high=24.999, decimals=0), N, rng, empty_ctx)
    assert out.dtype.kind == "i"
    assert out.min() >= 10 and out.max() <= 25


def test_bernoulli_maps_to_custom_labels(rng, empty_ctx):
    gen = BernoulliGen(p=0.45, true_value="Y", false_value="N")
    out = sample(gen, N, rng, empty_ctx)
    assert set(out) == {"Y", "N"}
    assert abs((out == "Y").mean() - 0.45) < 0.03


def test_empirical_resamples_observed_values(rng, empty_ctx):
    gen = EmpiricalGen(values=[1.0, 5.0, 9.0], weights=[0.5, 0.3, 0.2])
    out = sample(gen, N, rng, empty_ctx)
    assert set(out) == {1.0, 5.0, 9.0}
    assert abs((out == 1.0).mean() - 0.5) < 0.03


def test_sequence_is_zero_padded_and_ordered(rng, empty_ctx):
    out = sample(SequenceGen(prefix="GL_", width=6), 3, rng, empty_ctx)
    assert list(out) == ["GL_000001", "GL_000002", "GL_000003"]


def test_uuid_is_unique_and_prefixed(rng, empty_ctx):
    out = sample(UUIDGen(prefix="X-", short=False), N, rng, empty_ctx)
    assert len(set(out)) == N
    assert all(v.startswith("X-") and len(v) == 34 for v in out)


def test_constant(rng, empty_ctx):
    assert set(sample(ConstantGen(value="EUR"), 10, rng, empty_ctx)) == {"EUR"}


def test_same_seed_reproduces_the_same_draws(empty_ctx):
    gen = ScipyGen(dist="lognorm", params={"s": 0.4, "scale": 1000.0})
    a = sample(gen, 100, np.random.default_rng(1), empty_ctx)
    b = sample(gen, 100, np.random.default_rng(1), empty_ctx)
    np.testing.assert_array_equal(a, b)


def test_different_seeds_give_different_draws(empty_ctx):
    gen = ScipyGen(dist="lognorm", params={"s": 0.4, "scale": 1000.0})
    a = sample(gen, 100, np.random.default_rng(1), empty_ctx)
    b = sample(gen, 100, np.random.default_rng(2), empty_ctx)
    assert not np.array_equal(a, b)


def test_uuid_draws_are_seeded_not_global(empty_ctx):
    """UUIDs must come from the seeded generator, or a run cannot be reproduced."""
    a = sample(UUIDGen(short=True), 50, np.random.default_rng(3), empty_ctx)
    b = sample(UUIDGen(short=True), 50, np.random.default_rng(3), empty_ctx)
    np.testing.assert_array_equal(a, b)
