"""The six generation methods, as rewrites of the spec's own generators.

A method is not a separate engine. Each one takes the spec produced by the
profiler and rewrites the per-column generators, so whatever you pick is visible
in the YAML, editable afterwards, and reproducible from the document alone. That
is the whole point: "CTGAN" is not a black box you switch on, it is a line in a
spec you can read.

===============  =============================================================
``distribution`` The fitted named distribution per column — the profiler's own
                 choice, and the most faithful of the closed-form options.
``statistical``  Every numeric column becomes a normal with the same mean and
                 spread. Fast, obvious, and wrong in the tails on purpose: use
                 it when the shape does not matter and the level does.
``rule_based``   No fitted shape at all. Numbers are uniform inside their
                 observed bounds, categories are equally likely inside their
                 declared domain. This is the "I have a schema, not data" path.
``sampling``     Resample the observed values, spikes and all. Highest fidelity
                 per column, and the only one that reproduces a zero-inflated
                 column exactly — but it can only reproduce values that occurred.
``ctgan``        A deep tabular model trained on the real tape, learning how
                 columns move *together*. Needs the sample and the `deep` extra.
``hybrid``       ``distribution`` for the whole schema, then the deep model over
                 the columns it can improve — structure where it helps, an
                 auditable spec everywhere else.
===============  =============================================================

Every method is applied to the *base* spec, never to an already-rewritten one:
switching from ``statistical`` back to ``distribution`` has to recover the fitted
shapes, and it cannot do that from a normal.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from sdd.spec.schema import (
    BernoulliGen,
    CategoricalGen,
    ConstantGen,
    DesignSpec,
    EmpiricalGen,
    GaussianGen,
    GenerationMethod,
    ScipyGen,
    UniformGen,
)

# Kinds that describe identity or structure rather than a distribution. Rewriting
# any of them turns a key into noise.
STRUCTURAL_KINDS = ("sequence", "uuid", "constant", "conditional_categorical")

METHOD_LABELS: dict[str, str] = {
    "statistical": "Statistical",
    "distribution": "Distribution based",
    "rule_based": "Rule based",
    "sampling": "Sampling",
    "ctgan": "CTGAN",
    "hybrid": "Hybrid",
}


def apply_method(
    spec: DesignSpec,
    method: GenerationMethod,
    *,
    profile: dict[str, Any] | None = None,
) -> tuple[DesignSpec, list[str]]:
    """Return a copy of ``spec`` whose generators implement ``method``.

    ``profile`` is the analysis dict from :func:`sdd.api.design`; ``sampling``
    needs it, because resampling observed values requires the observations. The
    notes returned say what was rewritten and what could not be, so the UI can
    tell the user rather than quietly doing something else.
    """
    out = spec.model_copy(deep=True)
    out.generation.method = method
    notes: list[str] = []

    if method in ("distribution", "ctgan", "hybrid"):
        if method == "ctgan":
            notes.append(
                "Columns keep their fitted distributions as a starting point; the deep model "
                "replaces them at generation time, trained on the uploaded sample."
            )
        elif method == "hybrid":
            notes.append(
                "Fitted distributions everywhere, then a deep polish over the columns present "
                "in both the sample and the schema."
            )
        return out, notes

    protected = _protected_columns(out)
    resamples = _resamples(profile)
    rewritten = 0
    bounded = 0
    skipped: list[str] = []

    for column in out.columns:
        gen = column.generator
        if gen is None or column.name in protected or gen.kind in STRUCTURAL_KINDS:
            continue

        low, high = allowed_range(out, column)

        replacement: Any = None
        if method == "statistical":
            replacement = _as_normal(gen, low, high)
        elif method == "rule_based":
            replacement = _as_rule(gen, low, high, column.domain)
        elif method == "sampling":
            replacement = _as_resample(gen, resamples.get(column.name))

        if replacement is None:
            skipped.append(column.name)
            continue
        column.generator = replacement
        rewritten += 1
        if low is not None or high is not None:
            bounded += 1

    notes.append(f"{rewritten} column(s) rewritten as {METHOD_LABELS[method].lower()}.")
    if bounded:
        notes.append(
            f"{bounded} of them are held inside the range their original distribution allowed, "
            "so a balance that could never be negative still cannot be."
        )
    if skipped:
        head = ", ".join(skipped[:6])
        more = f" (+{len(skipped) - 6} more)" if len(skipped) > 6 else ""
        notes.append(
            f"{len(skipped)} column(s) kept their existing generator because this method has "
            f"nothing better to offer them: {head}{more}."
        )
    if method == "sampling" and not resamples:
        notes.append(
            "No sample was analysed, so there are no observed values to resample. Upload sample "
            "data, or use a method that works from the schema alone."
        )
    return out, notes


def _protected_columns(spec: DesignSpec) -> set[str]:
    """Columns whose generator carries meaning that a rewrite would destroy."""
    protected = {spec.entity.id_column, spec.entity.time_column}
    if spec.lifecycle:
        protected.add(spec.lifecycle.state_column)
        for fields in spec.lifecycle.state_fields.values():
            protected.update(fields)
    return {name for name in protected if name}


def _resamples(profile: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not profile:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for column in profile.get("columns", []):
        if column.get("resample"):
            out[column["name"]] = column["resample"]
        elif column.get("fit") and (column.get("dtype") in ("category", "bool", "str")):
            # Categorical fits are already the observed shares — resampling and
            # fitting are the same thing for them.
            continue
    return out


# ---------------------------------------------------------------------------
# per-generator rewrites
# ---------------------------------------------------------------------------


def true_support(gen: Any) -> tuple[float, float]:
    """The values a generator can actually produce, as ``(low, high)``.

    Unbounded ends come back as infinities. This is the *true* support, not a
    quantile range: a lognormal is reported as bounded below at zero because it
    genuinely cannot go lower, while a normal is reported as unbounded because it
    genuinely can.
    """
    low, high = -math.inf, math.inf

    match gen:
        case UniformGen():
            low, high = float(gen.low), float(gen.high)
        case EmpiricalGen():
            low, high = float(min(gen.values)), float(max(gen.values))
        case CategoricalGen() | BernoulliGen():
            values = (
                gen.values if isinstance(gen, CategoricalGen) else [gen.true_value, gen.false_value]
            )
            numbers = [float(v) for v in values if isinstance(v, int | float)]
            if len(numbers) == len(values) and numbers:
                low, high = min(numbers), max(numbers)
        case ScipyGen():
            from scipy import stats

            dist = getattr(stats, gen.dist, None)
            if dist is not None:
                try:
                    low, high = (float(v) for v in dist.support(**gen.params))
                except Exception:
                    low, high = -math.inf, math.inf

    # A declared clip is part of what the generator can produce, whatever the
    # underlying distribution allows.
    clip_min, clip_max = getattr(gen, "clip_min", None), getattr(gen, "clip_max", None)
    if clip_min is not None:
        low = max(low, float(clip_min))
    if clip_max is not None:
        high = min(high, float(clip_max))
    return low, high


def allowed_range(spec: DesignSpec, column: Any) -> tuple[float | None, float | None]:
    """The range a rewritten generator must stay inside for one column.

    Three sources, narrowest wins:

    *What the column declares.* ``min`` and ``max`` are the author's statement
    about the column, and a method must not contradict it.

    *What the spec asserts elsewhere.* A column listed in
    ``validation.non_negative_columns`` has a floor of zero — the validator will
    fail the run otherwise, and a method that knowingly produces a panel its own
    spec rejects is a bug, not a modelling choice.

    *What the generator being replaced could produce.* This is the one that
    actually bites: moment-matching a lognormal balance onto a normal keeps the
    mean and the spread but adds a left tail the original never had, and a few
    per cent of a portfolio comes back with a negative balance. A rewrite may
    narrow a support. It may not widen one.
    """
    low, high = column.min, column.max

    if column.name in spec.validation.non_negative_columns:
        low = max(low, 0.0) if low is not None else 0.0

    if column.generator is not None:
        original_low, original_high = true_support(column.generator)
        if math.isfinite(original_low):
            low = original_low if low is None else max(low, original_low)
        if math.isfinite(original_high):
            high = original_high if high is None else min(high, original_high)

    return low, high


def moments(gen: Any) -> tuple[float, float] | None:
    """Mean and standard deviation of a generator, when it has numeric ones."""
    match gen:
        case GaussianGen():
            return float(gen.mean), float(gen.stddev)
        case UniformGen():
            return (gen.low + gen.high) / 2.0, (gen.high - gen.low) / math.sqrt(12.0)
        case EmpiricalGen():
            return _weighted_moments(gen.values, gen.weights)
        case CategoricalGen():
            values = [v for v in gen.values if isinstance(v, int | float)]
            if len(values) != len(gen.values) or not values:
                return None
            return _weighted_moments(values, gen.weights)
        case BernoulliGen():
            if not isinstance(gen.true_value, int | float) or not isinstance(
                gen.false_value, int | float
            ):
                return None
            mean = gen.p * gen.true_value + (1 - gen.p) * gen.false_value
            spread = abs(gen.true_value - gen.false_value) * math.sqrt(gen.p * (1 - gen.p))
            return mean, spread
        case ScipyGen():
            from scipy import stats

            dist = getattr(stats, gen.dist, None)
            if dist is None:
                return None
            try:
                mean, var = dist.stats(moments="mv", **gen.params)
            except Exception:
                return None
            mean, var = float(mean), float(var)
            if not (np.isfinite(mean) and np.isfinite(var) and var > 0):
                return None
            return mean, math.sqrt(var)
    return None


def _weighted_moments(
    values: list[float], weights: list[float] | None
) -> tuple[float, float] | None:
    array = np.asarray(values, dtype=float)
    if array.size == 0:
        return None
    w = np.ones_like(array) if not weights else np.asarray(weights, dtype=float)
    if w.sum() <= 0:
        return None
    w = w / w.sum()
    mean = float((array * w).sum())
    var = float((w * (array - mean) ** 2).sum())
    return mean, math.sqrt(var) if var > 0 else 0.0


def support(gen: Any, low: float | None, high: float | None) -> tuple[float, float] | None:
    """The range a generator draws in — declared bounds first, inferred second."""
    if low is not None and high is not None and high > low:
        return float(low), float(high)

    match gen:
        case UniformGen():
            return float(gen.low), float(gen.high)
        case EmpiricalGen():
            return float(min(gen.values)), float(max(gen.values))
        case ScipyGen():
            from scipy import stats

            dist = getattr(stats, gen.dist, None)
            if dist is not None:
                try:
                    lo, hi = dist.ppf([0.001, 0.999], **gen.params)
                except Exception:
                    lo = hi = float("nan")
                if np.isfinite(lo) and np.isfinite(hi) and hi > lo:
                    return float(lo), float(hi)

    found = moments(gen)
    if found is None:
        return None
    mean, sd = found
    if sd <= 0:
        return None
    return mean - 3 * sd, mean + 3 * sd


def _decimals(gen: Any) -> int | None:
    return getattr(gen, "decimals", None)


def _as_normal(gen: Any, low: float | None, high: float | None) -> GaussianGen | None:
    """Moment matching: keep where the column sits and how wide it is."""
    found = moments(gen)
    if found is None:
        return None
    mean, sd = found
    if not np.isfinite(mean) or not np.isfinite(sd) or sd <= 0:
        return None
    return GaussianGen(
        mean=round(mean, 6),
        stddev=round(sd, 6),
        decimals=_decimals(gen),
        clip_min=low,
        clip_max=high,
    )


def _as_rule(
    gen: Any, low: float | None, high: float | None, domain: list[Any] | None
) -> Any | None:
    """Bounds and domains only — what a schema alone can justify."""
    if isinstance(gen, CategoricalGen):
        values = domain or gen.values
        if not values:
            return None
        # Equal weights: without data, no value is more likely than another.
        return CategoricalGen(values=list(values), weights=[1.0] * len(values))
    if isinstance(gen, BernoulliGen):
        return BernoulliGen(p=0.5, true_value=gen.true_value, false_value=gen.false_value)

    bounds = support(gen, low, high)
    if bounds is None:
        return None
    lo, hi = bounds
    # The inferred range comes from quantiles when the column declares no bounds,
    # and a quantile range can reach below what the column is allowed to be.
    if low is not None:
        lo = max(lo, low)
    if high is not None:
        hi = min(hi, high)
    if not (np.isfinite(lo) and np.isfinite(hi)) or hi <= lo:
        return None
    return UniformGen(low=round(lo, 6), high=round(hi, 6), decimals=_decimals(gen))


def _as_resample(gen: Any, resample: dict[str, Any] | None) -> Any | None:
    """Draw from the observed values rather than from a fitted shape."""
    if isinstance(gen, CategoricalGen | EmpiricalGen):
        # Already the observed shares.
        return None
    if not resample:
        return None
    try:
        return EmpiricalGen.model_validate(resample)
    except Exception:
        return None


def constant_like(value: Any) -> ConstantGen:
    """A generator that emits one value — used when a column is pinned by hand."""
    return ConstantGen(value=value)
