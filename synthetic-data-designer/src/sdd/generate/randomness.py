"""The four randomness controls, applied after sampling and before derivations.

Sampling column-by-column gives you a book where every column is individually
right and jointly wrong: no correlation, no anomalies, no gaps, and no jitter.
Real tapes have all four. These functions put them back, in an order that matters:

1. **Correlation** — reorder values within each column so the columns move
   together the way the sample did. Reordering cannot change any column's own
   distribution, so this buys joint structure for free.
2. **Outliers** — push a small share of rows into the tail. Done before noise so
   an outlier is a *decision*, not an accident of a wide jitter.
3. **Noise** — add gaussian jitter proportional to each column's own spread.
4. **Missing values** — blank a share of optional columns. Done last, so a
   blanked value is not first used to compute something else.

Everything here is driven by ``spec.generation`` and seeded from the run's own
generator, so two runs with the same seed produce the same book.

Derivations run *after* this, which is deliberate: an LTV recomputed from a
jittered balance still equals that balance divided by that valuation. Randomness
that broke internal consistency would not be realism, it would be corruption.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sdd.spec.schema import DesignSpec


def protected_columns(spec: DesignSpec) -> set[str]:
    """Names that randomness must leave exactly as generated.

    These are the columns the engine or the validator depends on: blanking or
    jittering any of them breaks a promise the spec itself makes about the
    output.
    """
    names = {spec.entity.id_column, spec.entity.time_column}
    lc = spec.lifecycle
    if lc:
        names.add(lc.state_column)
        for fields in lc.state_fields.values():
            names.update(fields)
    names.update(c.column for c in spec.dynamics.counters)
    names.update(a.column for a in spec.dynamics.accruals)
    am = spec.dynamics.amortisation
    if am:
        names.update(x for x in (am.balance, am.rate, am.payment, am.term) if x)
    if spec.dynamics.recovery:
        names.add(spec.dynamics.recovery.target)
    # A bucket derived from a column must keep matching it, and it will: the
    # derivation runs after this. But a column *feeding* a derivation still gets
    # jittered — that is the point, and the derived column follows it.
    return {n for n in names if n}


def numeric_targets(spec: DesignSpec, df: pd.DataFrame) -> list[str]:
    """Sampled numeric columns that randomness may touch."""
    protected = protected_columns(spec)
    out = []
    for column in spec.columns:
        if column.name in protected or column.role not in ("static", "dynamic"):
            continue
        if column.name not in df.columns:
            continue
        if column.dtype not in ("int", "float"):
            continue
        if pd.api.types.is_numeric_dtype(df[column.name]) and df[column.name].nunique() > 2:
            out.append(column.name)
    return out


def apply_randomness(
    spec: DesignSpec, df: pd.DataFrame, rng: np.random.Generator
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Run every enabled control over a freshly sampled book.

    Returns the frame and a small report of how many columns each control
    touched — the UI shows it, because a slider that silently did nothing is
    worse than no slider.
    """
    gen = spec.generation
    report = {"correlated": 0, "outliers": 0, "noised": 0, "blanked": 0}
    targets = numeric_targets(spec, df)

    if gen.correlation_target is not None:
        report["correlated"] = impose_correlation(spec, df, rng)
    if gen.outliers > 0 and targets:
        report["outliers"] = add_outliers(spec, df, targets, rng)
    if gen.noise > 0 and targets:
        report["noised"] = add_noise(spec, df, targets, rng)
    if gen.missing > 0 or any(c.null_rate for c in spec.columns):
        report["blanked"] = blank_values(spec, df, rng)
    return df, report


# ---------------------------------------------------------------------------
# correlation
# ---------------------------------------------------------------------------


def impose_correlation(spec: DesignSpec, df: pd.DataFrame, rng: np.random.Generator) -> int:
    """Reorder columns so their ranks match a target correlation (Iman-Conover).

    The trick, and the reason this is worth having: sorting a column's *existing*
    values into a new order cannot change its distribution. So the marginals the
    profiler fitted survive untouched while the joint structure appears. The
    target is scaled by ``generation.correlation``, which is what makes the
    slider continuous — 0 leaves the columns independent, 1 reproduces the
    sample's rank correlation.
    """
    return reorder_to_correlation(
        df, spec.generation.correlation_target, float(spec.generation.correlation), rng
    )


def reorder_to_correlation(
    df: pd.DataFrame,
    target: Any,
    strength: float,
    rng: np.random.Generator,
) -> int:
    """The Iman-Conover reordering itself, against any frame and any target.

    Split out from `impose_correlation` so a group table can use it. A group's
    attributes are drawn marginal by marginal, one generator each, and without
    this a company's revenue and its leverage vary independently — the same gap
    this function already closes for entity columns, on a different frame.

    Nothing here reads the spec, which is the point: there is one implementation
    of the reordering, so entity correlation and group correlation cannot drift
    into meaning different things.
    """
    if target is None:
        return 0

    columns = [c for c in target.columns if c in df.columns]
    if len(columns) < 2:
        return 0
    index = [target.columns.index(c) for c in columns]
    matrix = np.asarray(target.matrix, dtype=float)[np.ix_(index, index)]

    # Shrink toward the identity: a partly-correlated book is a real request
    # ("some structure, not all of it"), and shrinking is the only way to make it
    # mean something continuous.
    scaled = matrix * strength
    np.fill_diagonal(scaled, 1.0)
    scaled = _nearest_positive_definite(scaled)

    n = len(df)
    if n < 3:
        return 0

    # Van der Waerden scores: normal scores with the target correlation, whose
    # ranks then drive the reordering.
    cholesky = np.linalg.cholesky(scaled)
    scores = rng.standard_normal((n, len(columns))) @ cholesky.T

    touched = 0
    for position, name in enumerate(columns):
        values = pd.to_numeric(df[name], errors="coerce")
        if values.isna().any() or values.nunique() < 3:
            continue
        order = np.argsort(np.argsort(scores[:, position]))
        df[name] = np.sort(values.to_numpy())[order]
        touched += 1
    return touched


def _nearest_positive_definite(matrix: np.ndarray) -> np.ndarray:
    """Repair a correlation matrix that measurement noise made non-PSD.

    A matrix estimated pairwise from real data, then scaled, need not be a valid
    correlation matrix — and Cholesky refuses it. Clipping the eigenvalues at a
    small positive floor and renormalising gives the nearest one that works.
    """
    symmetric = (matrix + matrix.T) / 2.0
    values, vectors = np.linalg.eigh(symmetric)
    if values.min() > 1e-8:
        return symmetric
    repaired = vectors @ np.diag(np.clip(values, 1e-8, None)) @ vectors.T
    scale = np.sqrt(np.diag(repaired))
    repaired = repaired / np.outer(scale, scale)
    np.fill_diagonal(repaired, 1.0)
    return repaired


# ---------------------------------------------------------------------------
# outliers and noise
# ---------------------------------------------------------------------------


def add_outliers(
    spec: DesignSpec, df: pd.DataFrame, targets: list[str], rng: np.random.Generator
) -> int:
    """Push a share of rows into the tail of each numeric column.

    Direction is random but the magnitude is not: an outlier lands
    ``outlier_sigma`` standard deviations out, then is clipped back to any bound
    the column declares. A "600% LTV" is a data-quality artefact worth testing
    against; a negative balance is not, because the spec says it cannot happen.
    """
    rate = float(spec.generation.outliers)
    sigma = float(spec.generation.outlier_sigma)
    n = len(df)
    count = round(n * rate)
    if count < 1:
        return 0

    touched = 0
    for name in targets:
        # Copied, because the rows are written in place below and pandas hands
        # out a read-only array under copy-on-write.
        values = pd.to_numeric(df[name], errors="coerce").to_numpy(dtype=float, copy=True)
        spread = float(np.nanstd(values))
        if not np.isfinite(spread) or spread <= 0:
            continue
        rows = rng.choice(n, size=count, replace=False)
        direction = rng.choice([-1.0, 1.0], size=count)
        values[rows] = values[rows] + direction * sigma * spread
        df[name] = _respect_bounds(spec, name, values)
        touched += 1
    return touched


def add_noise(
    spec: DesignSpec, df: pd.DataFrame, targets: list[str], rng: np.random.Generator
) -> int:
    """Add gaussian jitter scaled to each column's own standard deviation."""
    level = float(spec.generation.noise)
    touched = 0
    for name in targets:
        values = pd.to_numeric(df[name], errors="coerce").to_numpy(dtype=float)
        spread = float(np.nanstd(values))
        if not np.isfinite(spread) or spread <= 0:
            continue
        values = values + rng.normal(0.0, level * spread, size=len(values))
        df[name] = _respect_bounds(spec, name, values)
        touched += 1
    return touched


def _respect_bounds(spec: DesignSpec, name: str, values: np.ndarray) -> np.ndarray:
    """Clip to the column's declared range and restore its decimal precision."""
    column = spec.column(name)
    if column is None:
        return values
    low = column.min
    high = column.max
    if low is not None or high is not None:
        values = np.clip(values, low, high)
    if name in spec.validation.non_negative_columns:
        values = np.clip(values, 0.0, None)
    decimals = getattr(column.generator, "decimals", None)
    if decimals is not None:
        values = np.round(values, decimals)
    if column.dtype == "int":
        values = np.round(values)
    return values


# ---------------------------------------------------------------------------
# missing values
# ---------------------------------------------------------------------------


def blankable_columns(spec: DesignSpec, available: set[str] | None = None) -> list[str]:
    """Columns a missing-value rate is allowed to touch.

    Optional columns only, and never the ones the engine or the validator reads:
    an identifier, a cut-off date, the state column, anything a state pins a
    value to, or an input to amortisation. Blanking those would not simulate
    messy data, it would simulate a broken file.
    """
    protected = protected_columns(spec)
    out = []
    for column in spec.columns:
        if column.role == "helper" or column.name in protected:
            continue
        if column.required and not column.null_rate:
            continue
        if available is not None and column.name not in available:
            continue
        out.append(column.name)
    return out


def blank_values(spec: DesignSpec, df: pd.DataFrame, rng: np.random.Generator) -> int:
    """Blank a share of the values in every optional column."""
    default_rate = float(spec.generation.missing)
    touched = 0
    for name in blankable_columns(spec, set(df.columns)):
        column = spec.column(name)
        rate = column.null_rate if column and column.null_rate is not None else default_rate
        if rate <= 0:
            continue
        mask = rng.random(len(df)) < rate
        if not mask.any():
            continue
        # object dtype takes None cleanly; numeric columns need to become float
        # first or pandas refuses the NaN.
        if pd.api.types.is_integer_dtype(df[name]):
            df[name] = df[name].astype("float64")
        df.loc[mask, name] = np.nan if pd.api.types.is_numeric_dtype(df[name]) else None
        touched += 1
    return touched
