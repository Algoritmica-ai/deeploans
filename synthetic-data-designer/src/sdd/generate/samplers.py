"""Numpy sampling backend — one function per generator kind.

This is the default engine: no external services, no GPU, no API keys, fully
seeded and reproducible. Upstream deeploans routed this through NVIDIA NeMo Data
Designer; that remains available as an optional backend
(:mod:`sdd.generate.backend_nemo`) but is no longer required to run.

Every sampler takes ``(gen, n, rng, ctx)`` and returns an array of length ``n``.
``ctx`` carries columns already sampled, which is how conditional generators see
their parent.
"""

from __future__ import annotations

import uuid as _uuid
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

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


class SamplingError(RuntimeError):
    """A generator that cannot produce values as configured."""


def _normalise(weights: list[float] | None, n: int) -> np.ndarray | None:
    if weights is None:
        return None
    w = np.asarray(weights, dtype=float)
    total = w.sum()
    if total <= 0:
        raise SamplingError("weights sum to zero")
    return w / total


def _finish(
    values: np.ndarray, decimals: int | None, clip_min: float | None, clip_max: float | None
) -> np.ndarray:
    if clip_min is not None or clip_max is not None:
        values = np.clip(values, clip_min, clip_max)
    if decimals is not None:
        values = np.round(values, decimals)
        if decimals == 0:
            values = values.astype("int64")
    return values


# ---------------------------------------------------------------------------
# individual samplers
# ---------------------------------------------------------------------------


def sample_categorical(gen: CategoricalGen, n: int, rng: np.random.Generator) -> np.ndarray:
    p = _normalise(gen.weights, len(gen.values))
    idx = rng.choice(len(gen.values), size=n, p=p)
    return np.asarray(gen.values, dtype=object)[idx]


def sample_conditional_categorical(
    gen: ConditionalCategoricalGen, n: int, rng: np.random.Generator, ctx: pd.DataFrame
) -> np.ndarray:
    if gen.parent not in ctx.columns:
        raise SamplingError(
            f"conditional generator needs column {gen.parent!r} to be sampled first; "
            "reorder the `columns` list so the parent comes before the child"
        )
    parent = ctx[gen.parent].to_numpy()
    out = np.empty(n, dtype=object)

    # Group by parent value so each pool is drawn in one vectorised call.
    for value in pd.unique(parent):
        mask = parent == value
        pool = gen.mapping.get(str(value), gen.mapping.get(value, gen.default))
        if pool is None:
            raise SamplingError(
                f"conditional generator has no mapping for parent value {value!r} and no "
                "`default` pool was given"
            )
        weights = None
        if gen.weights:
            raw = gen.weights.get(str(value), gen.weights.get(value))
            weights = _normalise(raw, len(pool)) if raw else None
        idx = rng.choice(len(pool), size=int(mask.sum()), p=weights)
        out[mask] = np.asarray(pool, dtype=object)[idx]
    return out


def sample_scipy(gen: ScipyGen, n: int, rng: np.random.Generator) -> np.ndarray:
    dist = getattr(stats, gen.dist, None)
    if dist is None:
        raise SamplingError(f"scipy.stats has no distribution named {gen.dist!r}")
    try:
        values = dist.rvs(size=n, random_state=rng, **gen.params)
    except TypeError as exc:
        raise SamplingError(f"scipy.stats.{gen.dist} rejected params {gen.params}: {exc}") from exc
    return _finish(np.asarray(values, dtype=float), gen.decimals, gen.clip_min, gen.clip_max)


def sample_gaussian(gen: GaussianGen, n: int, rng: np.random.Generator) -> np.ndarray:
    values = rng.normal(gen.mean, gen.stddev, size=n)
    return _finish(values, gen.decimals, gen.clip_min, gen.clip_max)


def sample_uniform(gen: UniformGen, n: int, rng: np.random.Generator) -> np.ndarray:
    values = rng.uniform(gen.low, gen.high, size=n)
    return _finish(values, gen.decimals, None, None)


def sample_bernoulli(gen: BernoulliGen, n: int, rng: np.random.Generator) -> np.ndarray:
    hits = rng.random(n) < gen.p
    return np.where(hits, gen.true_value, gen.false_value)


def sample_empirical(gen: EmpiricalGen, n: int, rng: np.random.Generator) -> np.ndarray:
    p = _normalise(gen.weights, len(gen.values))
    idx = rng.choice(len(gen.values), size=n, p=p)
    values = np.asarray(gen.values, dtype=float)[idx]
    return _finish(values, gen.decimals, None, None)


def sample_sequence(gen: SequenceGen, n: int, offset: int = 0) -> np.ndarray:
    """``offset`` continues the sequence past entities that already exist.

    An open pool builds several books over one run, and a second book starting
    again at 1 would hand new entities the identifiers of existing ones.
    """
    numbers = np.arange(gen.start + offset, gen.start + offset + n)
    return np.array([f"{gen.prefix}{i:0{gen.width}d}" for i in numbers], dtype=object)


def sample_uuid(gen: UUIDGen, n: int, rng: np.random.Generator) -> np.ndarray:
    # Draw from the seeded generator rather than `uuid.uuid4()` so runs reproduce.
    width = 8 if gen.short else 32
    raw = rng.integers(0, 16, size=(n, width))
    digits = np.array(list("0123456789abcdef"))
    body = ["".join(digits[row]) for row in raw]
    if gen.uppercase:
        body = [b.upper() for b in body]
    return np.array([f"{gen.prefix}{b}" for b in body], dtype=object)


def sample_constant(gen: ConstantGen, n: int) -> np.ndarray:
    return np.full(n, gen.value, dtype=object)


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


def sample(
    gen: Any, n: int, rng: np.random.Generator, ctx: pd.DataFrame, offset: int = 0
) -> np.ndarray:
    """Draw ``n`` values from any generator.

    ``offset`` only reaches the sequence generator, which is the one kind whose
    output depends on how many entities came before.
    """
    match gen:
        case CategoricalGen():
            return sample_categorical(gen, n, rng)
        case ConditionalCategoricalGen():
            return sample_conditional_categorical(gen, n, rng, ctx)
        case ScipyGen():
            return sample_scipy(gen, n, rng)
        case GaussianGen():
            return sample_gaussian(gen, n, rng)
        case UniformGen():
            return sample_uniform(gen, n, rng)
        case BernoulliGen():
            return sample_bernoulli(gen, n, rng)
        case EmpiricalGen():
            return sample_empirical(gen, n, rng)
        case SequenceGen():
            return sample_sequence(gen, n, offset)
        case UUIDGen():
            return sample_uuid(gen, n, rng)
        case ConstantGen():
            return sample_constant(gen, n)
        case _:
            raise SamplingError(f"no sampler for generator kind {getattr(gen, 'kind', gen)!r}")


def new_uuid_prefix() -> str:
    """A short random tag for naming a run's outputs."""
    return _uuid.uuid4().hex[:8]
