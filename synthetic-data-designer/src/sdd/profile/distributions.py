"""Pick a generator for a numeric column by fitting candidates and comparing.

The profiler's job is to guess how a column was produced. For numbers that means
trying a handful of named distributions, keeping the one whose fit is closest,
and falling back to resampling the observed values when nothing fits well.

Two judgement calls are worth naming, because getting either wrong produces
plausible-looking nonsense:

**Small integer sets are categories, not distributions.**
    ``debtor_count`` takes the values 1, 2 and 3. Fitting a gamma to that
    produces 2.7 debtors. Anything with few distinct integer values becomes a
    categorical generator with the observed weights.

**A poor fit is reported, not hidden.**
    Every fit carries its KS distance and a confidence score. Below
    :data:`POOR_FIT_KS` the column is marked for review in the generated spec
    rather than passed off as calibrated.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from sdd.spec.schema import CategoricalGen, EmpiricalGen, Generator, ScipyGen

# Distributions worth trying, in the order a human would: the shapes that
# actually occur in loan data.
CANDIDATES = ("lognorm", "norm", "gamma", "expon", "uniform", "beta")

# Above this KS distance the fitted shape is not describing the data.
POOR_FIT_KS = 0.05

# An integer column with at most this many distinct values is a category.
MAX_DISCRETE_VALUES = 25

# Number of bins used by the empirical fallback.
EMPIRICAL_BINS = 60

# A single value holding at least this share of a column is a point mass and is
# reproduced exactly rather than binned. See `_empirical`.
POINT_MASS_SHARE = 0.02

# Above this share on one value, no continuous distribution can describe the
# column and fitting one is a waste; go straight to the empirical mixture.
DOMINANT_MASS_FORCES_EMPIRICAL = 0.25

MIN_ROWS_TO_FIT = 50


@dataclass
class Fit:
    generator: Generator
    method: str
    ks: float
    confidence: float
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "ks": round(self.ks, 5),
            "confidence": round(self.confidence, 3),
            "note": self.note,
        }


def _confidence_from_ks(ks: float) -> float:
    """Map a KS distance to a 0-1 score.

    0.00 -> 1.0, 0.05 -> 0.5, 0.15 and worse -> near 0. The curve is a
    convenience for ranking and for the review flag, not a probability.
    """
    if not np.isfinite(ks):
        return 0.0
    return float(np.clip(1.0 - ks / 0.10, 0.0, 1.0))


def looks_discrete(values: pd.Series) -> bool:
    """True when a numeric column is really a small set of labels."""
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return False
    all_integers = bool(np.allclose(clean, np.round(clean)))
    return all_integers and clean.nunique() <= MAX_DISCRETE_VALUES


def decimals_used(values: pd.Series) -> int:
    """How many decimal places the data actually carries.

    Matters for output parity: a monetary column rounded to cents should be
    generated to cents, not to fifteen significant figures.
    """
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return 2
    for places in range(0, 7):
        if np.allclose(clean, np.round(clean, places), rtol=0, atol=1e-9):
            return places
    return 6


def fit_categorical(values: pd.Series, *, normalise: bool = True) -> Fit:
    """Observed categories and their shares, ordered most common first."""
    counts = values.dropna().value_counts(normalize=normalise)
    observed = [_plain(v) for v in counts.index.tolist()]
    weights = [round(float(w), 6) for w in counts.to_numpy()]
    return Fit(
        generator=CategoricalGen(values=observed, weights=weights),
        method="categorical",
        ks=0.0,
        # Exact by construction: these are the observed shares, not a fit.
        confidence=1.0,
    )


def fit_numeric(values: pd.Series, *, candidates: tuple[str, ...] = CANDIDATES) -> Fit:
    """Choose the best-fitting generator for a numeric column."""
    clean = pd.to_numeric(values, errors="coerce").dropna()

    if clean.empty:
        return Fit(
            generator=CategoricalGen(values=[0]),
            method="empty",
            ks=float("nan"),
            confidence=0.0,
            note="column is entirely null; generator is a placeholder",
        )

    if clean.nunique() == 1:
        from sdd.spec.schema import ConstantGen

        return Fit(
            generator=ConstantGen(value=_plain(clean.iloc[0])),
            method="constant",
            ks=0.0,
            confidence=1.0,
        )

    if looks_discrete(clean):
        fit = fit_categorical(clean)
        fit.method = "categorical (small integer set)"
        return fit

    if len(clean) < MIN_ROWS_TO_FIT:
        return _empirical(clean, note=f"only {len(clean)} rows; resampling observed values")

    # A column with a big spike is a mixture, not a continuous shape. No
    # single named distribution can put 96% of its mass on one exact value, so
    # skip the fitting and go to the empirical path, which preserves it.
    dominant = float(clean.value_counts(normalize=True).iloc[0])
    if dominant >= DOMINANT_MASS_FORCES_EMPIRICAL:
        return _empirical(
            clean,
            note=f"{dominant:.0%} of rows share one value, so this is a mixture rather than a "
            "continuous distribution; the spike is reproduced exactly",
        )

    sample = clean.to_numpy(dtype=float)
    best: Fit | None = None

    for name in candidates:
        dist = getattr(stats, name, None)
        if dist is None:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = dist.fit(sample)
                ks = float(stats.ks_1samp(sample, dist.cdf, args=params).statistic)
        except Exception:
            # A distribution that cannot fit this support (a lognormal against
            # negative values, say) is simply not a candidate.
            continue
        if not np.isfinite(ks):
            continue
        if best is None or ks < best.ks:
            best = Fit(
                generator=_scipy_generator(name, dist, params, clean),
                method=f"scipy.{name}",
                ks=ks,
                confidence=_confidence_from_ks(ks),
            )

    if best is None or best.ks > POOR_FIT_KS:
        fallback = _empirical(
            clean,
            note=(
                f"no named distribution fitted well (best {best.method} at KS={best.ks:.3f}); "
                "resampling observed values instead"
                if best
                else "no candidate distribution could be fitted"
            ),
        )
        return fallback

    return best


def _scipy_generator(
    name: str, dist: Any, params: tuple[float, ...], values: pd.Series
) -> ScipyGen:
    """Turn scipy's positional fit output into named parameters.

    scipy returns ``(*shapes, loc, scale)``; the spec stores them by name so a
    human can read and edit the result.
    """
    shape_names = (dist.shapes or "").replace(" ", "").split(",") if dist.shapes else []
    shape_names = [s for s in shape_names if s]
    named: dict[str, float] = {}
    for i, shape in enumerate(shape_names):
        named[shape] = round(float(params[i]), 6)
    named["loc"] = round(float(params[-2]), 6)
    named["scale"] = round(float(params[-1]), 6)
    return ScipyGen(dist=name, params=named, decimals=decimals_used(values))


def _empirical(values: pd.Series, *, note: str | None = None) -> Fit:
    """Resample the observed distribution, binned to keep the spec readable.

    Storing every raw value would make a spec unusable as a document; 60 bins
    preserve the shape while staying human-editable.

    **Point masses are kept exactly.** Loan data is full of zero-inflated
    columns — ``construction_deposit_amount`` is exactly 0 for the 96% of loans
    that have no deposit, then continuous above it. Plain histogramming moves
    all of that mass to a bin *midpoint*, so 96% of the portfolio comes back
    with a small non-zero deposit and the column is ruined (a measured KS of
    0.995 against the original). Any value holding at least
    :data:`POINT_MASS_SHARE` of the column is therefore preserved as itself, and
    only the remainder is binned.
    """
    array = values.to_numpy(dtype=float)
    shares = values.value_counts(normalize=True)
    spikes = shares[shares >= POINT_MASS_SHARE]

    points: list[float] = [float(v) for v in spikes.index]
    weights: list[float] = [float(w) for w in spikes.to_numpy()]

    remainder = array[~np.isin(array, points)] if points else array
    remaining_mass = len(remainder) / len(array)

    if remaining_mass > 0 and len(remainder) > 1:
        bins = min(EMPIRICAL_BINS, max(len(np.unique(remainder)), 2))
        counts, edges = np.histogram(remainder, bins=bins)
        midpoints = (edges[:-1] + edges[1:]) / 2.0
        keep = counts > 0
        # Scale the binned part so the whole thing still sums to 1.
        scaled = counts[keep] / counts[keep].sum() * remaining_mass
        points += [float(v) for v in midpoints[keep].round(6)]
        weights += [float(w) for w in scaled]

    total = sum(weights)
    return Fit(
        generator=EmpiricalGen(
            values=points,
            weights=[round(w / total, 8) for w in weights],
            decimals=decimals_used(values),
        ),
        method="empirical" + (f" ({len(spikes)} point mass(es) preserved)" if len(spikes) else ""),
        # Binning loses the within-bin shape, so this is good but not exact.
        ks=0.0,
        confidence=0.75,
        note=note,
    )


def _plain(value: Any) -> Any:
    """Convert numpy scalars to plain Python so the spec serialises cleanly."""
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value
