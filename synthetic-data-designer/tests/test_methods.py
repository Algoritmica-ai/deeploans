"""The six generation methods, and what each is honestly capable of.

A method is a rewrite of the spec's generators, so every test here asks the same
two questions: did the right generators change, and does the result still run?
The second matters more — a method that produces a beautiful spec the engine
rejects is worse than no method at all.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from sdd.api import load
from sdd.generate import build_book
from sdd.generate.methods import allowed_range, apply_method, moments, support, true_support
from sdd.profile import build_spec
from sdd.spec import load_spec_dict
from sdd.spec.schema import (
    CategoricalGen,
    EmpiricalGen,
    GaussianGen,
    ScipyGen,
    UniformGen,
)

PACK = "rmbs_nl_green_lion"


@pytest.fixture
def pack():
    return load(PACK)


@pytest.fixture
def profiled(tmp_path):
    """A spec profiled from a sample, which is what most methods need."""
    rng = np.random.default_rng(4)
    n = 600
    frame = pd.DataFrame(
        {
            "loan_id": [f"L{i:05d}" for i in range(n)],
            "reporting_date": "2024-01-31",
            "balance": rng.lognormal(12, 0.4, n).round(2),
            "rate": rng.normal(3.5, 0.6, n).round(2),
            "region": rng.choice(["N", "S", "E"], n, p=[0.5, 0.3, 0.2]),
        }
    )
    spec, profile = build_spec(frame, name="profiled")
    return spec, profile.to_dict()


# ---------------------------------------------------------------------------
# each method
# ---------------------------------------------------------------------------


def test_statistical_matches_the_moments_it_replaces(profiled):
    spec, profile = profiled
    before = moments(spec.column("balance").generator)

    out, notes = apply_method(spec, "statistical", profile=profile)
    generator = out.column("balance").generator

    assert isinstance(generator, GaussianGen)
    assert generator.mean == pytest.approx(before[0], rel=0.02)
    assert generator.stddev == pytest.approx(before[1], rel=0.02)
    assert any("rewritten as statistical" in n for n in notes)


def test_rule_based_keeps_only_bounds_and_domains(profiled):
    spec, profile = profiled
    out, _ = apply_method(spec, "rule_based", profile=profile)

    assert isinstance(out.column("balance").generator, UniformGen)
    region = out.column("region").generator
    # No data means no value is more likely than another.
    assert region.weights == [1.0] * len(region.values)


def test_sampling_draws_from_the_observed_values(profiled):
    spec, profile = profiled
    out, _ = apply_method(spec, "sampling", profile=profile)

    generator = out.column("balance").generator
    assert isinstance(generator, EmpiricalGen)
    assert sum(generator.weights) == pytest.approx(1.0, abs=1e-6)


def test_sampling_without_a_sample_says_so_rather_than_pretending(pack):
    out, notes = apply_method(pack, "sampling", profile=None)
    assert any("no observed values to resample" in n.lower() for n in notes)
    # Nothing was rewritten, so the spec still runs as it did.
    assert out.column("current_balance").generator == pack.column("current_balance").generator


def test_distribution_is_the_profilers_own_choice(profiled):
    spec, profile = profiled
    out, _ = apply_method(spec, "distribution", profile=profile)
    assert out.column("balance").generator == spec.column("balance").generator


def test_the_deep_methods_keep_the_spec_runnable_on_their_own(pack):
    """The model runs at generation time; until then the spec must still work."""
    for method in ("ctgan", "hybrid"):
        out, notes = apply_method(pack, method, profile=None)
        assert out.generation.method == method
        assert notes
        load_spec_dict(out.model_dump(mode="json", by_alias=True))


# ---------------------------------------------------------------------------
# what must never be rewritten
# ---------------------------------------------------------------------------


def test_identifiers_and_state_columns_keep_their_generators(pack):
    for method in ("statistical", "rule_based", "sampling"):
        out, _ = apply_method(pack, method, profile=None)
        assert out.column("loan_id").generator == pack.column("loan_id").generator
        state = pack.lifecycle.state_column
        assert out.column(state).generator == pack.column(state).generator


@pytest.mark.parametrize("method", ["statistical", "distribution", "rule_based", "sampling"])
def test_every_schema_only_method_still_generates(pack, method):
    out, _ = apply_method(pack, method, profile=None)
    reloaded = load_spec_dict(out.model_dump(mode="json", by_alias=True))
    book = build_book(reloaded, 120, seed=3)
    assert len(book) == 120


# ---------------------------------------------------------------------------
# a rewrite may narrow a support, never widen one
# ---------------------------------------------------------------------------


def test_true_support_reports_what_a_generator_can_actually_produce():
    """A quantile range is not a support: a lognormal genuinely cannot go below
    zero, and a normal genuinely can."""
    assert true_support(UniformGen(low=3, high=9)) == (3.0, 9.0)
    assert true_support(EmpiricalGen(values=[0.0, 5.0, 9.0])) == (0.0, 9.0)
    assert true_support(ScipyGen(dist="lognorm", params={"s": 0.5}))[0] == 0.0
    assert true_support(GaussianGen(mean=0, stddev=1)) == (-math.inf, math.inf)
    # A declared clip is part of what the generator can produce.
    assert true_support(GaussianGen(mean=0, stddev=1, clip_min=0.0)) == (0.0, math.inf)


def test_a_numeric_category_keeps_its_own_range():
    assert true_support(CategoricalGen(values=[1, 2, 3])) == (1.0, 3.0)
    # Labels have no numeric range to preserve.
    assert true_support(CategoricalGen(values=["a", "b"])) == (-math.inf, math.inf)


def test_moment_matching_cannot_add_a_left_tail_the_original_never_had(pack):
    """The bug this pins: a lognormal balance moment-matched onto a normal keeps
    the mean and the spread and gains a left tail, so a few per cent of the
    portfolio comes back with a negative balance."""
    out, _ = apply_method(pack, "statistical", profile=None)
    generator = out.column("original_balance").generator

    assert isinstance(generator, GaussianGen)
    assert generator.clip_min == 0.0
    assert generator.mean - 3 * generator.stddev < 0, "otherwise the test proves nothing"


@pytest.mark.parametrize("method", ["statistical", "rule_based", "sampling", "distribution"])
def test_no_method_can_produce_a_panel_its_own_spec_rejects(pack, tmp_path, method):
    """Every method, run end to end, against the invariants the spec declares."""
    from sdd import api

    configured, _ = apply_method(pack, method, profile=None)
    payload = configured.model_dump(mode="json", exclude_none=True, by_alias=True)
    result = api.run(payload, 400, tmp_path / method, seed=3, periods=3)

    failures = [c["name"] for c in result["validation"]["checks"] if not c["passed"]]
    assert result["validation"]["passed"], failures

    panel = pd.read_parquet(result["panel"])
    for column in configured.validation.non_negative_columns:
        assert pd.to_numeric(panel[column], errors="coerce").min() >= 0, column


def test_the_allowed_range_takes_the_narrowest_of_every_source(pack):
    column = pack.column("original_balance")
    low, _high = allowed_range(pack, column)
    assert low == 0.0, "the lognormal it replaces cannot go below zero"

    # A column the spec asserts is non-negative gets a floor even when its
    # generator would happily go lower.
    widened = pack.model_copy(deep=True)
    target = widened.column("original_balance")
    target.generator = GaussianGen(mean=100.0, stddev=400.0)
    assert allowed_range(widened, target)[0] == 0.0


# ---------------------------------------------------------------------------
# the moment/support helpers
# ---------------------------------------------------------------------------


def test_moments_are_read_from_every_numeric_generator_kind():
    assert moments(GaussianGen(mean=5, stddev=2)) == (5.0, 2.0)
    assert moments(UniformGen(low=0, high=12))[0] == pytest.approx(6.0)
    assert moments(EmpiricalGen(values=[1, 3], weights=[0.5, 0.5]))[0] == pytest.approx(2.0)
    assert moments(ScipyGen(dist="norm", params={"loc": 7, "scale": 1.5})) == pytest.approx(
        (7.0, 1.5)
    )


def test_support_prefers_a_declared_bound_over_an_inferred_one():
    generator = ScipyGen(dist="norm", params={"loc": 0, "scale": 1})
    assert support(generator, 10.0, 20.0) == (10.0, 20.0)
    low, high = support(generator, None, None)
    assert low < -2 and high > 2
