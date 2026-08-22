"""Aim a column's total at a stated aggregate.

A spec's generators draw each entity independently, so the portfolio's total is
whatever those draws happen to sum to. Real deals have a size — EUR 500m of
collateral, not "however much 500 loans came to" — and this closes that gap.

The approach is to scale the *generator* so the expected total lands on the
target, rather than to rescale the values after drawing them. Two reasons:

**Reinvestment.** A pool that buys new collateral builds later cohorts from the
same spec. Rescale only the opening book and every facility acquired afterwards
is drawn at the unscaled size — a portfolio whose new assets are three times the
size of its original ones. Scaling the generator applies everywhere for free.

**Legibility.** The scaled spec is the spec the user downloads, so the number
they see is the number that ran.

The cost is honest and worth stating: the realised total varies around the
target by ordinary sampling error, a few per cent at a few hundred entities.
This aims; it does not guarantee.
"""

from __future__ import annotations

import math
from typing import Any

from sdd.spec.schema import DesignSpec


class TargetError(ValueError):
    """A target could not be applied to the column it names."""


def _expected_value(gen: Any) -> float | None:
    """The mean of a generator's output, where that is knowable in closed form."""
    kind = getattr(gen, "kind", None)

    if kind == "gaussian":
        return float(gen.mean)

    if kind == "uniform":
        return (float(gen.low) + float(gen.high)) / 2.0

    if kind == "constant":
        try:
            return float(gen.value)
        except (TypeError, ValueError):
            return None

    if kind == "categorical":
        try:
            values = [float(v) for v in gen.values]
        except (TypeError, ValueError):
            return None
        weights = list(gen.weights) if gen.weights else [1.0 / len(values)] * len(values)
        total = sum(weights)
        return sum(v * w for v, w in zip(values, weights, strict=True)) / total

    if kind == "scipy":
        params = dict(gen.params)
        loc = float(params.get("loc", 0.0))
        scale = float(params.get("scale", 1.0))
        if gen.dist == "lognorm":
            sigma = float(params.get("s", 0.0))
            return loc + scale * math.exp(sigma * sigma / 2.0)
        if gen.dist == "norm":
            return loc
        if gen.dist == "expon":
            return loc + scale
        if gen.dist == "gamma":
            return loc + float(params.get("a", 1.0)) * scale
        if gen.dist == "beta":
            # a/(a+b) is the mean of the standard beta; loc and scale move it onto
            # the column's own units. Added because the profiler fits beta to
            # bounded balances, and without this a learned target on a perfectly
            # scalable generator was refused for no reason.
            a = float(params.get("a", 1.0))
            b = float(params.get("b", 1.0))
            return loc + scale * a / (a + b) if a + b > 0 else None
        return None

    return None


def _rescale(gen: Any, factor: float) -> None:
    """Multiply a generator's location so its mean scales by ``factor``."""
    kind = getattr(gen, "kind", None)

    if kind == "gaussian":
        gen.mean = gen.mean * factor
        gen.stddev = gen.stddev * factor
    elif kind == "uniform":
        gen.low = gen.low * factor
        gen.high = gen.high * factor
    elif kind == "constant":
        gen.value = float(gen.value) * factor
    elif kind == "categorical":
        gen.values = [float(v) * factor for v in gen.values]
    elif kind == "scipy":
        params = dict(gen.params)
        params["scale"] = float(params.get("scale", 1.0)) * factor
        if "loc" in params:
            params["loc"] = float(params["loc"]) * factor
        gen.params = params
    else:
        raise TargetError(f"generator kind {kind!r} cannot be scaled")

    # Clips are in the column's own units, so they travel with it. Left alone,
    # a target that shrinks a column by 3x would leave a floor that truncates
    # most of the distribution and quietly reinstates the old scale.
    for bound in ("clip_min", "clip_max"):
        value = getattr(gen, bound, None)
        if value is not None:
            setattr(gen, bound, float(value) * factor)


def apply_targets(spec: DesignSpec, num_records: int) -> tuple[DesignSpec, list[str]]:
    """Scale each targeted column's generator so its expected total hits the mark.

    Returns the adjusted spec and a note per target, because a control that
    silently did nothing is worse than no control.
    """
    if not spec.entity.targets:
        return spec, []

    spec = spec.model_copy(deep=True)
    by_name = {c.name: c for c in spec.columns}
    notes: list[str] = []

    for target in spec.entity.targets:
        column = by_name.get(target.column)
        if column is None:
            raise TargetError(f"target names unknown column {target.column!r}")

        entities = target.entities or num_records
        mean = _expected_value(column.generator)
        if mean is None:
            raise TargetError(
                f"target on {target.column!r} cannot be applied: the mean of a "
                f"{getattr(column.generator, 'kind', '?')!r} generator is not known in "
                "closed form, so there is nothing to scale against"
            )
        if mean <= 0:
            raise TargetError(
                f"target on {target.column!r} needs a positive expected value, got {mean}"
            )

        wanted = target.total / entities
        factor = wanted / mean
        _rescale(column.generator, factor)
        notes.append(
            f"target: {target.column} scaled x{factor:.4f} so {entities:,} entities "
            f"average {wanted:,.0f} and total about {target.total:,.0f}"
        )

    return spec, notes
