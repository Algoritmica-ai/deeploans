"""Aggregate targets: a portfolio that adds up to a stated size.

Generators draw each entity independently, so a portfolio's total is whatever
those draws happen to sum to. A deal has a size, and this is how a spec says so.

The implementation scales the *generator*, not the drawn values. The test that
matters most here is `reinvestment_cohorts_inherit_the_scale`: rescaling the
opening book alone would leave every later acquisition drawn at the unscaled
size, and a portfolio whose new assets are three times the size of its original
ones is a worse failure than missing a target by a few per cent.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scipy import stats

from sdd import api
from sdd.generate.targets import TargetError, _expected_value, apply_targets
from sdd.spec.schema import CategoricalGen, GaussianGen, ScipyGen, UniformGen

PACK = "clo_eu_leveraged_loans"
TARGET_COLUMN = "original_facility_amount"


def test_the_closed_form_means_match_the_distributions():
    """Everything is scaled against these, so they have to be right."""
    cases = [
        (
            ScipyGen(dist="lognorm", params={"s": 0.62, "loc": 0.0, "scale": 3_100_000.0}),
            stats.lognorm(s=0.62, loc=0.0, scale=3_100_000.0).mean(),
        ),
        (
            ScipyGen(dist="norm", params={"loc": 50.0, "scale": 7.0}),
            stats.norm(loc=50.0, scale=7.0).mean(),
        ),
        (
            ScipyGen(dist="expon", params={"loc": 2.0, "scale": 5.0}),
            stats.expon(loc=2.0, scale=5.0).mean(),
        ),
        (
            ScipyGen(dist="gamma", params={"a": 3.0, "loc": 0.0, "scale": 4.0}),
            stats.gamma(a=3.0, loc=0.0, scale=4.0).mean(),
        ),
    ]
    for generator, expected in cases:
        assert _expected_value(generator) == pytest.approx(expected, rel=1e-9)

    assert _expected_value(GaussianGen(mean=12.0, stddev=3.0)) == 12.0
    assert _expected_value(UniformGen(low=2.0, high=8.0)) == 5.0
    assert _expected_value(
        CategoricalGen(values=[10.0, 20.0], weights=[0.25, 0.75])
    ) == pytest.approx(17.5)


def test_the_portfolio_lands_on_its_target(tmp_path):
    result = api.run(PACK, 500, tmp_path, seed=42, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]

    total = opening["current_par"].sum()
    # Sampling error, not exactness: this aims the expected total, so a few per
    # cent at 500 entities is the honest tolerance.
    assert total == pytest.approx(500_000_000, rel=0.06), f"opening par was {total:,.0f}"


def test_asking_for_more_entities_buys_a_bigger_portfolio(tmp_path):
    """`entities` pins what the total assumes, so scale does not shrink loans."""
    small = api.run(PACK, 400, tmp_path / "s", seed=7, validate_output=False)
    large = api.run(PACK, 900, tmp_path / "l", seed=7, validate_output=False)

    def mean_size(result):
        panel = pd.read_parquet(result["panel"])
        opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]
        return opening[TARGET_COLUMN].mean()

    assert mean_size(large) == pytest.approx(mean_size(small), rel=0.12), (
        "facility size moved with the entity count; the target is being spread "
        "across the run rather than pinned to `entities`"
    )


def test_reinvestment_cohorts_inherit_the_scale(tmp_path):
    """The reason the generator is scaled rather than the drawn values.

    Rescale the opening book alone and every facility acquired later is drawn at
    the unscaled size — here, roughly three times too large.
    """
    result = api.run(PACK, 500, tmp_path, seed=42, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    first_seen = panel.groupby("facility_id")["reporting_date"].min()
    opening_ids = set(first_seen[first_seen == panel["reporting_date"].min()].index)
    sizes = panel.groupby("facility_id")[TARGET_COLUMN].first()

    opening = sizes[sizes.index.isin(opening_ids)]
    acquired = sizes[~sizes.index.isin(opening_ids)]
    assert len(acquired) > 50, "too few acquisitions to compare"

    assert acquired.mean() == pytest.approx(opening.mean(), rel=0.20), (
        f"acquired facilities average {acquired.mean():,.0f} against an opening "
        f"book of {opening.mean():,.0f}; cohorts are not inheriting the scale"
    )


def test_clips_travel_with_the_scale():
    """A floor left in the old units would truncate the scaled distribution.

    The CLO target shrinks facilities roughly threefold. An unscaled 400,000
    floor would then sit above the new median and quietly reinstate the old size.
    """
    spec = api.load(PACK)
    original = next(c for c in spec.columns if c.name == TARGET_COLUMN)
    before = original.generator.clip_min

    scaled, notes = apply_targets(spec, 500)
    after = next(c for c in scaled.columns if c.name == TARGET_COLUMN).generator.clip_min

    assert notes, "a target that did nothing must still say so"
    assert after < before, "clip_min did not scale with the generator"


def test_a_spec_without_targets_is_untouched(tmp_path):
    """Two packs declare no targets and must be unaffected."""
    for pack in ("auto_abs_esma_annex5", "rmbs_nl_green_lion"):
        spec = api.load(pack)
        scaled, notes = apply_targets(spec, 500)
        assert notes == []
        assert scaled is spec, "a spec with no targets should not even be copied"


def test_an_unknown_column_is_refused():
    spec = api.load(PACK)
    spec.entity.targets[0].column = "no_such_column"
    with pytest.raises(TargetError, match="no_such_column"):
        apply_targets(spec, 500)


def test_a_generator_with_no_closed_form_mean_is_refused():
    """Better to refuse than to scale something whose mean is unknown."""
    spec = api.load(PACK)
    column = next(c for c in spec.columns if c.name == TARGET_COLUMN)
    column.generator = ScipyGen(dist="cauchy", params={"loc": 0.0, "scale": 1.0})
    with pytest.raises(TargetError, match="closed form"):
        apply_targets(spec, 500)


def test_the_target_reaches_the_downloadable_spec():
    """The scaled spec is what the user gets, so the number they see ran."""
    spec = api.load(PACK)
    scaled, _ = apply_targets(spec, 500)
    assert scaled.entity.targets[0].total == 500_000_000
    before = next(c for c in spec.columns if c.name == TARGET_COLUMN).generator.params["scale"]
    after = next(c for c in scaled.columns if c.name == TARGET_COLUMN).generator.params["scale"]
    assert after != before, "the downloaded spec would not reproduce the run"


def test_targets_do_not_break_the_pack_invariants(tmp_path):
    result = api.run(PACK, 400, tmp_path, seed=11)
    assert result["validation"]["passed"], [
        c["name"] for c in result["validation"]["checks"] if not c["passed"]
    ]
