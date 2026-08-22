"""Spot columns that are computed from other columns, not sampled independently.

Marginal profiling has one structural weakness: it fits every column on its own,
so any relationship *between* columns is lost. Sample a balance and an LTV band
separately and you get loans whose band does not match their balance — internally
inconsistent data that looks fine column by column.

Regulatory tapes are full of these. Upstream's 71-column layout had seven
pre-computed bucket columns, each a binning of a number sitting a few columns
away. Detecting them turns seven independent samplers into seven derivations,
which fixes both their own distribution and their correlation with the source.

What is detectable here is deliberately narrow: **binnings**, which can be
verified exactly rather than guessed at. A candidate is only accepted if every
observed value of the numeric column falls in a contiguous range that no other
category overlaps — a property a coincidence will not satisfy across thousands
of rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sdd.profile.profiler import DatasetProfile

# A binning has few labels; more than this and it is a category in its own right.
MAX_BUCKET_LABELS = 40

# Rows needed before an interval structure means anything.
MIN_ROWS = 200

# Suffixes that advertise a binning, checked first to keep the search cheap.
BUCKET_SUFFIXES = ("_bucket", "_band", "_bin", "_range", "_group", "_category")


@dataclass
class BucketDiscovery:
    target: str
    source: str
    bins: list[float]
    labels: list[str]
    confidence: float
    note: str | None = None


def find_bucket_columns(df: pd.DataFrame, profile: DatasetProfile) -> list[BucketDiscovery]:
    """Find categorical columns that are binnings of a numeric column."""
    if len(df) < MIN_ROWS:
        return []

    numeric = [
        c.name
        for c in profile.columns
        if c.dtype in ("int", "float")
        and c.name in df.columns
        and df[c.name].nunique() > MAX_BUCKET_LABELS
    ]
    if not numeric:
        return []

    found: list[BucketDiscovery] = []
    for col in profile.columns:
        if col.dtype not in ("category", "str") or col.name not in df.columns:
            continue
        if not 1 < col.distinct <= MAX_BUCKET_LABELS:
            continue

        for candidate in _ranked_sources(col.name, numeric):
            discovery = _test_binning(df, col.name, candidate)
            if discovery:
                found.append(discovery)
                break
    return found


def _ranked_sources(target: str, numeric: list[str]) -> list[str]:
    """Try the likeliest source column first.

    ``balance_bucket`` almost certainly bins ``balance``, so stripping the
    suffix and matching by name avoids testing every numeric column against
    every categorical one.
    """
    stem = target.lower()
    for suffix in BUCKET_SUFFIXES:
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break

    exact = [n for n in numeric if n.lower() == stem]
    partial = [n for n in numeric if n not in exact and (stem in n.lower() or n.lower() in stem)]
    rest = [n for n in numeric if n not in exact and n not in partial]
    return exact + partial + rest


def _test_binning(df: pd.DataFrame, target: str, source: str) -> BucketDiscovery | None:
    """Accept ``target`` as a binning of ``source`` only if the intervals are clean."""
    pair = df[[target, source]].dropna()
    if len(pair) < MIN_ROWS:
        return None

    grouped = pair.groupby(target, observed=True)[source].agg(["min", "max", "count"])
    if len(grouped) < 2:
        return None
    grouped = grouped.sort_values("min")

    lows = grouped["min"].to_numpy(dtype=float)
    highs = grouped["max"].to_numpy(dtype=float)

    # Every group must sit entirely to the right of the previous one. One
    # overlap and this is not a binning.
    if not np.all(highs[:-1] < lows[1:]):
        return None

    # Cut points sit between the top of one band and the bottom of the next.
    interior = [(float(highs[i]) + float(lows[i + 1])) / 2.0 for i in range(len(lows) - 1)]
    span = float(highs[-1] - lows[0]) or 1.0
    edges = [float(lows[0]) - span, *interior, float(highs[-1]) + span]

    labels = [str(v) for v in grouped.index.tolist()]

    # Verify by re-deriving: the recovered rule must reproduce every observed
    # label. This is what makes the detection safe rather than suggestive.
    redone = pd.cut(pair[source], bins=edges, labels=labels, include_lowest=True).astype(str)
    agreement = float((redone == pair[target].astype(str)).mean())
    if agreement < 0.999:
        return None

    return BucketDiscovery(
        target=target,
        source=source,
        bins=[round(e, 6) for e in edges],
        labels=labels,
        confidence=round(agreement, 4),
        note=(
            f"recovered as a binning of {source!r}; the recovered edges reproduce "
            f"{agreement:.1%} of observed labels"
        ),
    )
