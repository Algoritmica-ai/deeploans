"""How closely does the synthetic data resemble the sample it was built from?

Invariants ask "is this internally consistent?". Fidelity asks "does it look
like the real thing?" — a different question, and the one that decides whether a
generated tape is usable for pretraining or back-testing.

Four measures, each chosen because it is interpretable rather than merely
computable:

**KS distance** (numeric columns)
    The largest gap between the two cumulative distributions, from 0 (identical)
    to 1 (no overlap). 0.02 means the worst-matched percentile is off by two
    percentage points.

**Total-variation distance** (categorical columns)
    Half the sum of absolute differences in category shares, again 0 to 1. 0.01
    means you would have to move 1% of the rows to make the mixes match.

**Correlation delta**
    The largest change in any pairwise correlation. Marginals can match
    perfectly while the joint structure is wrong; this is what catches that.

**Transition delta** (panels only)
    The largest change in any state-to-state transition probability. Two panels
    can have identical delinquency *mixes* and completely different dynamics.

All four are "smaller is better" and share a scale, so one table reads across
every column type without needing a statistics background.

Each column's threshold is raised to its **noise floor** — the distance two
samples drawn from the *same* distribution would typically reach at these row
counts. Without that, the measures would report on sample size rather than on
fidelity: at 20k rows a 40-category column lands ~0.02 apart by pure chance
while a 2-category flag lands under 0.005, so any single flat threshold is
simultaneously too strict for one and too lax for the other. The raw distance
is always reported alongside, so nothing is hidden by the adjustment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

# Defaults are the parity bar: tight enough to catch a wrong distribution,
# loose enough not to fire on Monte-Carlo noise at ~50k rows.
DEFAULT_KS_THRESHOLD = 0.02
DEFAULT_TV_THRESHOLD = 0.01
DEFAULT_CORR_THRESHOLD = 0.05
DEFAULT_TRANSITION_THRESHOLD = 0.02

# Below this many rows, sampling noise swamps the signal and a KS test says
# more about the sample size than about the generator.
MIN_ROWS_FOR_COMPARISON = 30


@dataclass
class ColumnFidelity:
    column: str
    kind: str
    metric: str
    distance: float
    threshold: float
    passed: bool
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "kind": self.kind,
            "metric": self.metric,
            "distance": round(self.distance, 6),
            "threshold": self.threshold,
            "passed": self.passed,
            **self.detail,
        }


@dataclass
class FidelityReport:
    columns: list[ColumnFidelity]
    correlation_delta: float | None = None
    correlation_threshold: float = DEFAULT_CORR_THRESHOLD
    correlation_excluded: list[str] = field(default_factory=list)
    transition_delta: float | None = None
    transition_threshold: float = DEFAULT_TRANSITION_THRESHOLD
    skipped: list[str] = field(default_factory=list)

    @property
    def failures(self) -> list[ColumnFidelity]:
        return [c for c in self.columns if not c.passed]

    @property
    def passed(self) -> bool:
        """True when every column, the joint structure, and the dynamics all fit."""
        over = [
            bool(self.failures),
            self.correlation_delta is not None
            and self.correlation_delta > self.correlation_threshold,
            self.transition_delta is not None and self.transition_delta > self.transition_threshold,
        ]
        return not any(over)

    @property
    def worst(self) -> ColumnFidelity | None:
        return max(self.columns, key=lambda c: c.distance) if self.columns else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "columns_compared": len(self.columns),
            "columns_failed": len(self.failures),
            "correlation_delta": self.correlation_delta,
            "correlation_threshold": self.correlation_threshold,
            "correlation_excluded": self.correlation_excluded,
            "transition_delta": self.transition_delta,
            "skipped": self.skipped,
            "detail": [c.to_dict() for c in self.columns],
        }

    def summary(self) -> str:
        lines = [
            f"{len(self.columns) - len(self.failures)}/{len(self.columns)} columns within tolerance"
        ]
        if self.correlation_delta is not None:
            note = (
                f", {len(self.correlation_excluded)} sparse column(s) excluded"
                if self.correlation_excluded
                else ""
            )
            lines.append(
                f"  max correlation delta  {self.correlation_delta:.4f} "
                f"(limit {self.correlation_threshold}{note})"
            )
        if self.transition_delta is not None:
            lines.append(
                f"  max transition delta   {self.transition_delta:.4f} "
                f"(limit {self.transition_threshold})"
            )
        if self.worst and self.columns:
            w = self.worst
            lines.append(f"  worst column           {w.column} ({w.metric}={w.distance:.4f})")
        for c in self.failures:
            lines.append(f"  FAIL {c.column}: {c.metric}={c.distance:.4f} > {c.threshold}")
        if self.skipped:
            lines.append(f"  skipped: {', '.join(self.skipped[:8])}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# per-column measures
# ---------------------------------------------------------------------------


def ks_distance(reference: pd.Series, synthetic: pd.Series) -> float:
    """Kolmogorov-Smirnov statistic: the widest gap between the two CDFs."""
    a = pd.to_numeric(reference, errors="coerce").dropna().to_numpy()
    b = pd.to_numeric(synthetic, errors="coerce").dropna().to_numpy()
    if len(a) < MIN_ROWS_FOR_COMPARISON or len(b) < MIN_ROWS_FOR_COMPARISON:
        return float("nan")
    return float(stats.ks_2samp(a, b).statistic)


def ks_noise_floor(n_ref: int, n_syn: int, alpha: float = 0.01) -> float:
    """The KS distance two samples from the *same* distribution typically reach.

    Standard two-sample critical value ``c(alpha) * sqrt((n+m)/(nm))``. Below
    this, a difference is indistinguishable from sampling noise.
    """
    if n_ref <= 0 or n_syn <= 0:
        return 1.0
    c = float(np.sqrt(-np.log(alpha / 2.0) / 2.0))
    return c * float(np.sqrt((n_ref + n_syn) / (n_ref * n_syn)))


def tv_noise_floor(reference: pd.Series, n_ref: int, n_syn: int, multiple: float = 3.0) -> float:
    """The TV distance two samples from the *same* mix typically reach.

    This matters more than it sounds. TV noise grows with the number of
    categories: a 40-region column drawn twice from one distribution lands
    around 0.02 apart at 20k rows purely by chance, while a 2-category flag
    lands under 0.005. A single flat threshold would therefore flag every
    high-cardinality column as a fidelity failure and pass every low-cardinality
    one regardless of how wrong it was.

    Each category's share differs by a half-normal amount with standard
    deviation ``sqrt(p(1-p)(1/n + 1/m))``, so the *expected* total is
    ``0.5 * sqrt(2/pi) * sqrt(1/n + 1/m) * sum_i sqrt(p_i(1-p_i))``.

    ``multiple`` turns that expectation into a tolerance. It must exceed 1: half
    of all columns land above their own expected noise by chance, so a threshold
    set at the mean would fail roughly half of a perfectly good dataset.

    The default of 3 was measured, not assumed. Over 868 same-generator column
    comparisons on the RMBS pack (28 seed pairs, 20k rows), the ratio of
    observed distance to expected noise had a median of 0.99 — confirming the
    formula above — but a far heavier tail than a concentration argument would
    suggest, reaching 3.55 at the maximum:

    ========  ======  ======  ======  ========
    quantile     p50     p90     p99      max
    ratio       0.99    1.72    2.88     3.55
    ========  ======  ======  ======  ========

    That puts the false-failure rate at 5.1% per column for a multiple of 2 and
    0.7% for 3. Three is the better trade because a fidelity report is used as a
    gate, and a gate that cries wolf gets ignored.

    What that costs is worth stating plainly. At 20k rows the check reliably
    catches a 5% shift in a category's share, and a 2% shift in a
    *low*-cardinality column. A 2% shift spread across a 12-category column sits
    at 1.8x the noise floor and is **not** detectable at any sane multiple — it
    is genuinely indistinguishable from sampling variation at that row count.
    Detecting it needs more rows, not a looser threshold.
    """
    if n_ref <= 0 or n_syn <= 0:
        return 1.0
    p = reference.astype(str).value_counts(normalize=True).to_numpy()
    spread = float(np.sqrt(p * (1.0 - p)).sum())
    expected = 0.5 * float(np.sqrt(2.0 / np.pi)) * float(np.sqrt(1 / n_ref + 1 / n_syn)) * spread
    return multiple * expected


def tv_distance(reference: pd.Series, synthetic: pd.Series) -> float:
    """Total-variation distance between two category mixes.

    Categories present in only one of the two count in full, which is the point:
    inventing a category the sample never had is a fidelity failure, not a
    rounding difference.
    """
    p = reference.astype(str).value_counts(normalize=True)
    q = synthetic.astype(str).value_counts(normalize=True)
    categories = p.index.union(q.index)
    p = p.reindex(categories, fill_value=0.0)
    q = q.reindex(categories, fill_value=0.0)
    return float(0.5 * np.abs(p - q).sum())


# A column where one value takes this much of the mass has no stably estimable
# correlation — see `correlation_delta`.
MAX_DOMINANT_SHARE = 0.90


def correlation_noise_floor(n_rows: int, n_columns: int) -> float:
    """The largest correlation change two same-distribution samples reach.

    A single correlation's standard error shrinks as ``1/sqrt(n)``, and taking
    the maximum over ``n_columns^2`` pairs pulls the observed peak out into the
    tail. Measured against the RMBS pack (15 seed pairs, 28 numeric columns,
    20k rows) the peak sat at 0.042, which pins the constant at about 6.
    """
    if n_rows <= 3 or n_columns < 2:
        return 1.0
    return 6.0 / float(np.sqrt(n_rows))


def correlation_delta(
    reference: pd.DataFrame, synthetic: pd.DataFrame, columns: list[str]
) -> tuple[float | None, list[str], int]:
    """Largest absolute change in any pairwise correlation.

    Returns ``(delta, excluded_columns, n_compared)``.

    Columns dominated by a single value are excluded, and this is a statistical
    necessity rather than a convenience. ``arrears_amount`` is zero for 96.5% of
    a healthy mortgage pool, so its correlation rests on a few hundred rows and
    on which of them happen to carry large payments; two runs of the *same*
    generator disagree by 0.076 on it while every dense column stays inside
    0.042. Pearson's usual standard error assumes bivariate normality, which a
    96.5% spike at zero violates badly, so no analytic bound rescues it either.

    Nothing is lost by the exclusion: sparse columns are still checked by their
    own KS or TV distance, which behaves properly on that shape. The check moves
    to the statistic that works instead of being dropped.
    """
    candidates = [
        c
        for c in columns
        if c in reference.columns
        and c in synthetic.columns
        and pd.api.types.is_numeric_dtype(reference[c])
        and pd.api.types.is_numeric_dtype(synthetic[c])
        # A constant column has undefined correlation.
        and reference[c].nunique() > 1
        and synthetic[c].nunique() > 1
    ]
    usable, excluded = [], []
    for c in candidates:
        counts = reference[c].value_counts(normalize=True)
        if len(counts) and counts.iloc[0] > MAX_DOMINANT_SHARE:
            excluded.append(c)
        else:
            usable.append(c)

    if len(usable) < 2:
        return None, excluded, len(usable)

    # Copied before the diagonal is written: under pandas' copy-on-write, the
    # default from pandas 3, the array behind a frame is handed out read-only.
    delta = (reference[usable].corr() - synthetic[usable].corr()).abs().to_numpy(copy=True)
    np.fill_diagonal(delta, 0.0)
    return float(np.nanmax(delta)), excluded, len(usable)


def transition_matrix(
    df: pd.DataFrame, id_column: str, time_column: str, state_column: str
) -> tuple[pd.DataFrame, list[str]]:
    """Empirical state-to-state transition probabilities observed in a panel.

    Counts each consecutive pair of observations for an entity, then normalises
    each row. This is also what the profiler uses to *learn* a matrix from real
    data, rather than having one hand-set.
    """
    ordered = df[[id_column, time_column, state_column]].sort_values([id_column, time_column])
    nxt = ordered.groupby(id_column)[state_column].shift(-1)
    pairs = pd.DataFrame({"from": ordered[state_column], "to": nxt}).dropna()
    if pairs.empty:
        return pd.DataFrame(), []

    counts = pd.crosstab(pairs["from"], pairs["to"])
    states = sorted(set(counts.index) | set(counts.columns))
    counts = counts.reindex(index=states, columns=states, fill_value=0)
    totals = counts.sum(axis=1).replace(0, np.nan)
    return counts.div(totals, axis=0).fillna(0.0), states


def transition_delta(
    reference: pd.DataFrame,
    synthetic: pd.DataFrame,
    id_column: str,
    time_column: str,
    state_column: str,
) -> tuple[float | None, float]:
    """Largest difference between two empirical transition matrices, and its floor.

    Returns ``(delta, noise_floor)``.

    A transition matrix is estimated far less evenly than a marginal. In a
    healthy mortgage pool the Performing row rests on hundreds of thousands of
    observations while the 60-89-days-late row rests on a few hundred, so its
    cells carry roughly thirty times the standard error. Comparing every cell
    against one flat threshold therefore reports on how rare a state is rather
    than on whether the dynamics match: measured against upstream, the largest
    cell difference sat at 1.2 standard errors and still breached a flat 0.02.

    Each cell is a binomial proportion, so the two estimates differ with
    standard error ``sqrt(p(1-p)(1/n_a + 1/n_b))`` where the ``n`` are the row
    counts behind them. The floor is three of those, taken over whichever cell
    is worst — the same tail-bound reasoning used for the other measures.
    """
    for frame in (reference, synthetic):
        if not {id_column, time_column, state_column} <= set(frame.columns):
            return None, DEFAULT_TRANSITION_THRESHOLD
    a, _ = transition_matrix(reference, id_column, time_column, state_column)
    b, _ = transition_matrix(synthetic, id_column, time_column, state_column)
    if a.empty or b.empty:
        return None, DEFAULT_TRANSITION_THRESHOLD

    states = sorted(set(a.index) | set(b.index))
    a = a.reindex(index=states, columns=states, fill_value=0.0)
    b = b.reindex(index=states, columns=states, fill_value=0.0)
    delta = float(np.nanmax((a - b).abs().to_numpy()))

    counts_a = _transition_row_counts(reference, id_column, time_column, state_column, states)
    counts_b = _transition_row_counts(synthetic, id_column, time_column, state_column, states)

    floor = 0.0
    for i, state in enumerate(states):
        n_a, n_b = counts_a.get(state, 0), counts_b.get(state, 0)
        if n_a < 2 or n_b < 2:
            # A terminal state has no outgoing transitions by construction, so
            # there is nothing to judge and nothing to widen the floor for.
            # Treating it as unjudgeable would push the limit to 1.0 and make
            # the whole check vacuous.
            continue
        for j in range(len(states)):
            p = (a.iloc[i, j] + b.iloc[i, j]) / 2.0
            se = float(np.sqrt(max(p * (1.0 - p), 0.0) * (1.0 / n_a + 1.0 / n_b)))
            floor = max(floor, 3.0 * se)
    return delta, floor


def _transition_row_counts(
    df: pd.DataFrame, id_column: str, time_column: str, state_column: str, states: list[str]
) -> dict[str, int]:
    """How many observed transitions start from each state."""
    ordered = df[[id_column, time_column, state_column]].sort_values([id_column, time_column])
    nxt = ordered.groupby(id_column)[state_column].shift(-1)
    starts = ordered.loc[nxt.notna(), state_column].value_counts()
    return {s: int(starts.get(s, 0)) for s in states}


# ---------------------------------------------------------------------------
# the comparison
# ---------------------------------------------------------------------------


def effective_sample_sizes(
    df: pd.DataFrame, columns: list[str], id_column: str | None
) -> dict[str, int]:
    """How many *independent* observations each column really has.

    A panel repeats every entity once per period, so a 20,000-loan panel over 24
    cut-offs has 480,000 rows — but a loan's province is the same value 24 times
    over. Treating those as 480,000 independent draws makes the noise floor
    about five times too tight, and every static column is then reported as a
    fidelity failure on what is really sampling noise.

    So: a column that never varies within an entity gets the entity count; one
    that moves gets the row count. One groupby covers every column at once,
    which matters on a 71-column panel.
    """
    rows = len(df)
    if not id_column or id_column not in df.columns:
        return dict.fromkeys(columns, rows)

    entities = int(df[id_column].nunique())
    checkable = [c for c in columns if c in df.columns and c != id_column]
    if not checkable or entities == rows:
        return dict.fromkeys(columns, rows)

    varies = df.groupby(id_column)[checkable].nunique().max()
    return {c: (rows if varies.get(c, 2) > 1 else entities) for c in columns}


def compare(
    reference: pd.DataFrame,
    synthetic: pd.DataFrame,
    *,
    id_column: str | None = None,
    time_column: str | None = None,
    state_column: str | None = None,
    ks_threshold: float = DEFAULT_KS_THRESHOLD,
    tv_threshold: float = DEFAULT_TV_THRESHOLD,
    corr_threshold: float = DEFAULT_CORR_THRESHOLD,
    transition_threshold: float = DEFAULT_TRANSITION_THRESHOLD,
    ignore: list[str] | None = None,
) -> FidelityReport:
    """Score ``synthetic`` against ``reference`` column by column.

    Only columns present in both are compared; the rest are listed as skipped so
    a missing column is visible rather than silently ignored.
    """
    skip = set(ignore or [])
    # Identifiers and dates are unique by construction, so distance measures on
    # them are meaningless — every value differs and every test would fail.
    for name in (id_column, time_column):
        if name:
            skip.add(name)

    shared = [c for c in reference.columns if c in synthetic.columns and c not in skip]
    missing = [c for c in reference.columns if c not in synthetic.columns]

    # Panel rows are not independent; see `effective_sample_sizes`.
    n_ref_by_column = effective_sample_sizes(reference, shared, id_column)
    n_syn_by_column = effective_sample_sizes(synthetic, shared, id_column)

    results: list[ColumnFidelity] = []
    skipped: list[str] = list(missing)

    for col in shared:
        ref, syn = reference[col], synthetic[col]
        numeric = pd.api.types.is_numeric_dtype(ref) and pd.api.types.is_numeric_dtype(syn)

        n_ref = min(n_ref_by_column.get(col, len(ref)), len(ref.dropna()))
        n_syn = min(n_syn_by_column.get(col, len(syn)), len(syn.dropna()))

        if numeric:
            distance = ks_distance(ref, syn)
            if np.isnan(distance):
                skipped.append(col)
                continue
            floor = ks_noise_floor(n_ref, n_syn)
            effective = max(ks_threshold, floor)
            results.append(
                ColumnFidelity(
                    column=col,
                    kind="numeric",
                    metric="ks",
                    distance=distance,
                    threshold=round(effective, 6),
                    passed=distance <= effective,
                    detail={
                        "noise_floor": round(floor, 6),
                        "effective_n": n_ref,
                        "reference_mean": _safe(ref.mean()),
                        "synthetic_mean": _safe(syn.mean()),
                        "reference_std": _safe(ref.std()),
                        "synthetic_std": _safe(syn.std()),
                    },
                )
            )
        else:
            distance = tv_distance(ref, syn)
            floor = tv_noise_floor(ref, n_ref, n_syn)
            effective = max(tv_threshold, floor)
            results.append(
                ColumnFidelity(
                    column=col,
                    kind="categorical",
                    metric="tv",
                    distance=distance,
                    threshold=round(effective, 6),
                    passed=distance <= effective,
                    detail={
                        "noise_floor": round(floor, 6),
                        "effective_n": n_ref,
                        "reference_categories": int(ref.nunique()),
                        "synthetic_categories": int(syn.nunique()),
                    },
                )
            )

    corr, corr_excluded, n_corr = correlation_delta(reference, synthetic, shared)
    effective_rows = min(
        min(n_ref_by_column.values(), default=len(reference)),
        min(n_syn_by_column.values(), default=len(synthetic)),
    )
    effective_corr = max(corr_threshold, correlation_noise_floor(effective_rows, n_corr))
    trans, trans_floor = (
        transition_delta(reference, synthetic, id_column, time_column, state_column)
        if id_column and time_column and state_column
        else (None, transition_threshold)
    )

    return FidelityReport(
        columns=results,
        correlation_delta=corr,
        correlation_threshold=round(effective_corr, 6),
        correlation_excluded=corr_excluded,
        transition_delta=trans,
        transition_threshold=round(max(transition_threshold, trans_floor), 6),
        skipped=skipped,
    )


def _safe(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(out) else round(out, 4)
