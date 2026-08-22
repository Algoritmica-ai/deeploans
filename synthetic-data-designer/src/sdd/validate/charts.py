"""The four views of a finished run, computed server-side.

Charts are aggregations, and a browser is the wrong place to do aggregation over
a million-row panel. Each function here returns a small JSON-shaped dict — a few
hundred numbers at most — that a front end can draw directly without ever holding
the panel.

Which column is which is answered from the spec first and by name second. The
spec knows its balance column because the amortisation rule names it, and it
knows its distressed states because the lifecycle declares them. Name matching is
only the fallback for the columns nothing declares, such as LTV.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sdd.spec.schema import DesignSpec

HISTOGRAM_BINS = 30

# Only used when the spec does not name the column itself. Ordered by preference:
# a *current* loan-to-value moves as balances amortise and valuations drift,
# which is the interesting one. An *original* LTV is fixed at origination, so
# charting its first and last cut-off draws the same histogram twice.
LTV_HINTS = (
    "current_ltv",
    "ltv_current",
    "cltomv",
    "cltimv",
    "indexed_ltv",
    "ltv",
    "loan_to_value",
    "oltomv",
    "ltomv",
    "loan_to_val",
)
BALANCE_HINTS = ("current_balance", "outstanding", "balance", "principal", "exposure")


def _find(frame: pd.DataFrame, hints: tuple[str, ...], *, numeric: bool = False) -> str | None:
    for hint in hints:
        for column in frame.columns:
            if hint not in str(column).lower():
                continue
            if numeric and not pd.to_numeric(frame[column], errors="coerce").notna().any():
                # A bucket label such as `cltomv_current_bucket` matches the same
                # hint as the ratio it was derived from, and cannot be plotted.
                continue
            return str(column)
    return None


def balance_column(spec: DesignSpec, frame: pd.DataFrame) -> str | None:
    am = spec.dynamics.amortisation
    if am and am.balance in frame.columns:
        return am.balance
    return _find(frame, BALANCE_HINTS, numeric=True)


def ltv_column(spec: DesignSpec, frame: pd.DataFrame) -> str | None:
    return _find(frame, LTV_HINTS, numeric=True)


def distressed_states(spec: DesignSpec) -> list[str]:
    """States that mean a borrower is behind, in the spec's own order.

    Everything past the first state on a delinquency ladder, minus the states
    that mean the entity left healthily. Redeeming early is not distress.
    """
    lc = spec.lifecycle
    if lc is None:
        return []
    from sdd.age.calibrate import default_states, prepayment_hazard

    healthy_exit = prepayment_hazard(spec)
    leaving_well = {healthy_exit.to_state} if healthy_exit else set()
    bad = set(default_states(spec))
    return [s for s in lc.states[1:] if s not in leaving_well or s in bad]


# ---------------------------------------------------------------------------
# the four charts
# ---------------------------------------------------------------------------


def distribution_comparison(
    spec: DesignSpec,
    panel: pd.DataFrame,
    reference: pd.DataFrame | None = None,
    *,
    columns: list[str] | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Histograms of the generated columns, against the sample where there is one.

    Both series are binned on the *same* edges, computed from the two together.
    Binning them separately would produce two histograms that look similar and
    cannot be compared, which is worse than no chart.
    """
    first = _first_period(spec, panel)
    wanted = columns or _interesting_numeric(spec, first)
    out: list[dict[str, Any]] = []

    for name in wanted[:limit]:
        synthetic = pd.to_numeric(first[name], errors="coerce").dropna()
        if synthetic.empty:
            continue
        actual = None
        if reference is not None and name in reference.columns:
            actual = pd.to_numeric(reference[name], errors="coerce").dropna()
            if actual.empty:
                actual = None

        pool = synthetic if actual is None else pd.concat([synthetic, actual])
        low, high = float(pool.quantile(0.001)), float(pool.quantile(0.999))
        if not np.isfinite(low) or not np.isfinite(high) or high <= low:
            low, high = float(pool.min()), float(pool.max())
        if high <= low:
            continue
        edges = np.linspace(low, high, HISTOGRAM_BINS + 1)

        entry: dict[str, Any] = {
            "column": name,
            "edges": [round(float(e), 4) for e in edges],
            "synthetic": _density(synthetic, edges),
            "reference": _density(actual, edges) if actual is not None else None,
            "stats": {
                "synthetic": _summary(synthetic),
                "reference": _summary(actual) if actual is not None else None,
            },
        }
        out.append(entry)
    return out


def _density(values: pd.Series, edges: np.ndarray) -> list[float]:
    counts, _ = np.histogram(values.to_numpy(dtype=float), bins=edges)
    total = counts.sum()
    return [round(float(c) / float(total), 6) if total else 0.0 for c in counts]


def _summary(values: pd.Series) -> dict[str, float]:
    return {
        "mean": round(float(values.mean()), 4),
        "median": round(float(values.median()), 4),
        "std": round(float(values.std()), 4),
        "min": round(float(values.min()), 4),
        "max": round(float(values.max()), 4),
    }


def delinquency_curve(spec: DesignSpec, panel: pd.DataFrame) -> dict[str, Any] | None:
    """The share of the surviving pool in each distressed state, period by period.

    Shares rather than counts: the pool shrinks as loans redeem, so counts fall
    even when behaviour is unchanged, and a curve that falls for that reason is
    actively misleading.
    """
    lc = spec.lifecycle
    if lc is None or lc.state_column not in panel.columns:
        return None
    time_column = spec.entity.time_column
    if time_column not in panel.columns:
        return None

    states = distressed_states(spec)
    if not states:
        return None

    grouped = panel.groupby(time_column)[lc.state_column]
    counts = grouped.value_counts().unstack(fill_value=0)
    totals = counts.sum(axis=1).replace(0, np.nan)

    periods = [str(p) for p in counts.index]
    series = [
        {
            "state": state,
            "values": [
                round(float(v), 6) for v in (counts.get(state, 0) / totals).fillna(0.0).to_numpy()
            ],
        }
        for state in states
        if state in counts.columns
    ]
    if not series:
        return None

    combined = np.sum([s["values"] for s in series], axis=0)
    return {
        "periods": periods,
        "series": series,
        "total_delinquent": [round(float(v), 6) for v in combined],
    }


def ltv_distribution(spec: DesignSpec, panel: pd.DataFrame) -> dict[str, Any] | None:
    """Where leverage sits at the start and at the end.

    Two histograms on shared bins. The movement between them is the point: an
    amortising pool with a rising index drifts left, and a falling one does not.
    """
    column = ltv_column(spec, panel)
    if column is None:
        return None
    time_column = spec.entity.time_column
    values = pd.to_numeric(panel[column], errors="coerce")
    if values.dropna().empty:
        return None

    if time_column in panel.columns:
        first_key, last_key = panel[time_column].min(), panel[time_column].max()
        first = values[panel[time_column] == first_key].dropna()
        last = values[panel[time_column] == last_key].dropna()
    else:
        first_key = last_key = None
        first, last = values.dropna(), values.dropna()

    pool = pd.concat([first, last])
    low, high = float(pool.quantile(0.001)), float(pool.quantile(0.999))
    if high <= low:
        low, high = float(pool.min()), float(pool.max())
    if high <= low:
        return None
    edges = np.linspace(low, high, HISTOGRAM_BINS + 1)

    return {
        "column": column,
        "edges": [round(float(e), 4) for e in edges],
        "first": {"label": str(first_key), "values": _density(first, edges), **_summary(first)},
        "last": {"label": str(last_key), "values": _density(last, edges), **_summary(last)},
    }


def pool_balance(spec: DesignSpec, panel: pd.DataFrame) -> dict[str, Any] | None:
    """Total balance and loan count per cut-off — the pool's amortisation curve."""
    time_column = spec.entity.time_column
    if time_column not in panel.columns:
        return None
    column = balance_column(spec, panel)
    if column is None:
        return None

    frame = panel[[time_column, column]].copy()
    frame[column] = pd.to_numeric(frame[column], errors="coerce")
    grouped = frame.groupby(time_column)[column].agg(["sum", "count", "mean"])

    opening = float(grouped["sum"].iloc[0]) if len(grouped) else 0.0
    return {
        "column": column,
        "periods": [str(p) for p in grouped.index],
        "balance": [round(float(v), 2) for v in grouped["sum"]],
        "loans": [int(v) for v in grouped["count"]],
        "average": [round(float(v), 2) for v in grouped["mean"]],
        "factor": [round(float(v) / opening, 6) if opening else 0.0 for v in grouped["sum"]],
    }


# ---------------------------------------------------------------------------
# the whole set
# ---------------------------------------------------------------------------


def build_charts(
    spec: DesignSpec,
    panel: str | Path | pd.DataFrame,
    reference: str | Path | pd.DataFrame | None = None,
    *,
    columns: list[str] | None = None,
    metrics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Every chart the results step shows, plus why any of them is missing."""
    from sdd.profile import read_sample

    frame = panel if isinstance(panel, pd.DataFrame) else read_sample(panel)
    sample = None
    if reference is not None:
        sample = reference if isinstance(reference, pd.DataFrame) else read_sample(reference)

    charts: dict[str, Any] = {
        "distribution": distribution_comparison(spec, frame, sample, columns=columns),
        "delinquency": delinquency_curve(spec, frame),
        "ltv": ltv_distribution(spec, frame),
        "pool_balance": pool_balance(spec, frame),
        "configured": configured_charts(spec, frame, metrics),
        "numeric_columns": _interesting_numeric(spec, frame),
        "has_reference": sample is not None,
    }
    charts["unavailable"] = {
        key: reason
        for key, reason in (
            (
                "delinquency",
                "this spec has no lifecycle, so no loan is ever behind on a payment",
            ),
            ("ltv", "no column looks like a loan-to-value ratio"),
            ("pool_balance", "no column looks like an outstanding balance"),
        )
        if charts.get(key) is None
    }
    return charts


def _first_period(spec: DesignSpec, panel: pd.DataFrame) -> pd.DataFrame:
    time_column = spec.entity.time_column
    if time_column not in panel.columns:
        return panel
    return panel[panel[time_column] == panel[time_column].min()]


def _interesting_numeric(spec: DesignSpec, frame: pd.DataFrame) -> list[str]:
    """Numeric columns worth plotting, best first.

    The balance and the LTV lead because they are what a portfolio is judged on;
    identifiers and near-constant columns are dropped because a histogram of a
    key is a straight line.
    """
    leading = [c for c in (balance_column(spec, frame), ltv_column(spec, frame)) if c is not None]
    skip = {spec.entity.id_column, spec.entity.time_column, *leading}

    others = []
    for column in spec.columns:
        if column.name in skip or column.name not in frame.columns:
            continue
        if column.dtype not in ("int", "float"):
            continue
        series = pd.to_numeric(frame[column.name], errors="coerce")
        if series.notna().sum() > 0 and series.nunique() > 5:
            others.append(column.name)

    return leading + others


# ---------------------------------------------------------------------------
# charts a pack asks for
# ---------------------------------------------------------------------------


def configured_charts(
    spec: DesignSpec,
    frame: pd.DataFrame,
    metrics: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build whatever the spec's `results.charts` declares.

    A series chart reads the metrics table rather than re-aggregating the panel.
    That is not only cheaper: it means the line drawn on screen is the same
    number the downloaded report carries, instead of a second calculation of it
    that can quietly disagree.
    """
    if not spec.results.charts:
        return []

    time_column = spec.entity.time_column
    built: list[dict[str, Any]] = []

    for chart in spec.results.charts:
        payload: dict[str, Any] = {
            "kind": chart.kind,
            "title": chart.title,
            "unit": chart.unit,
            "description": chart.description,
            "explain": chart.explain,
        }
        try:
            data = _one_chart(chart, spec, frame, metrics, time_column)
        except Exception as exc:
            payload["unavailable"] = str(exc)
            built.append(payload)
            continue

        if data is None:
            payload["unavailable"] = "nothing to draw from this run"
        else:
            payload.update(data)
        built.append(payload)

    return built


def _one_chart(
    chart: Any,
    spec: DesignSpec,
    frame: pd.DataFrame,
    metrics: list[dict[str, Any]] | None,
    time_column: str,
) -> dict[str, Any] | None:
    if chart.kind == "series":
        if chart.metric:
            if not metrics:
                raise ValueError("this run produced no metrics table")
            table = pd.DataFrame(metrics)
            if chart.metric not in table.columns:
                raise ValueError(f"no metric named {chart.metric!r}")
            values = pd.to_numeric(table[chart.metric], errors="coerce")
            return {
                "periods": [str(d) for d in table["date"]],
                "values": [None if pd.isna(v) else round(float(v), 6) for v in values],
                "label": chart.metric,
            }
        grouped = frame.groupby(time_column)[chart.column].sum()
        return {
            "periods": [str(d) for d in grouped.index],
            "values": [round(float(v), 6) for v in grouped],
            "label": chart.column,
        }

    if chart.kind == "stacked_series":
        if chart.column not in frame.columns:
            raise ValueError(f"{chart.column!r} is not in the panel")
        counts = pd.crosstab(frame[time_column], frame[chart.column], normalize="index")
        wanted = chart.states or [str(c) for c in counts.columns]
        series = []
        for state in wanted:
            if state in counts.columns:
                series.append(
                    {"label": str(state), "values": [round(float(v), 6) for v in counts[state]]}
                )
        if not series:
            return None
        return {"periods": [str(d) for d in counts.index], "series": series}

    if chart.kind == "category_bar":
        for name in (chart.group, chart.column):
            if name not in frame.columns:
                raise ValueError(f"{name!r} is not in the panel")
        # The final cut-off: a concentration figure is about the book as it
        # stands, not summed over every month it stood there.
        last = frame[frame[time_column] == frame[time_column].max()]
        totals = (
            pd.to_numeric(last[chart.column], errors="coerce")
            .groupby(last[chart.group])
            .sum()
            .sort_values(ascending=False)
        )
        total = float(totals.sum())
        if total <= 0:
            return None
        return {
            "categories": [str(k) for k in totals.index],
            "values": [round(float(v), 6) for v in totals],
            "shares": [round(float(v) / total, 6) for v in totals],
            "as_of": str(last[time_column].iloc[0]),
        }

    if chart.kind == "histogram":
        if chart.column not in frame.columns:
            raise ValueError(f"{chart.column!r} is not in the panel")
        values = pd.to_numeric(frame[chart.column], errors="coerce").dropna()
        if values.empty:
            return None
        counts, edges = np.histogram(values, bins=min(30, max(6, values.nunique())))
        return {
            "edges": [round(float(e), 6) for e in edges],
            "counts": [int(c) for c in counts],
            "stats": _summary(values),
        }

    raise ValueError(f"unknown chart kind {chart.kind!r}")
