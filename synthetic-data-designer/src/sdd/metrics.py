"""The portfolio, summarised at every cut-off.

The panel says what each loan did. A portfolio report says what the *book* did:
its size, its average coupon, how much of it is in trouble, how concentrated it
is. Those are the numbers an investor reads, and until now the engine reported
only a count of entities per state.

Computed inside the ageing loop, on the frame that is about to be written, so
the figures are of exactly the rows they describe. Recomputing them afterwards
from the panel would work and would be a second implementation of the same
arithmetic — which is how a report and the data it reports on drift apart.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sdd.spec.schema import DesignSpec, Metric


class MetricError(ValueError):
    """A metric could not be computed from the columns available."""


def _selected(frame: pd.DataFrame, metric: Metric, period: int) -> pd.Series | None:
    """The `where` mask, or None when the metric takes every row."""
    if not metric.where:
        return None
    from sdd.generate.deriver import evaluate_mask

    try:
        return pd.Series(evaluate_mask(metric.where, frame, {"period": period}), index=frame.index)
    except Exception as exc:
        raise MetricError(
            f"metric {metric.name!r} could not evaluate {metric.where!r}: {exc}"
        ) from exc


def _numeric(frame: pd.DataFrame, column: str, metric: Metric) -> pd.Series:
    if column not in frame.columns:
        raise MetricError(f"metric {metric.name!r} reads {column!r}, which is not in the panel")
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


def _one(frame: pd.DataFrame, metric: Metric, period: int, running: dict[str, Any]) -> float:
    mask = _selected(frame, metric, period)
    kind = metric.kind

    if kind == "count":
        rows = frame if mask is None else frame[mask]
        return float(len(rows))

    if kind == "distinct_count":
        rows = frame if mask is None else frame[mask]
        if metric.column not in rows.columns:
            raise MetricError(
                f"metric {metric.name!r} reads {metric.column!r}, which is not in the panel"
            )
        return float(rows[metric.column].nunique())

    values = _numeric(frame, metric.column, metric)

    if kind == "sum":
        return float(values.sum() if mask is None else values[mask].sum())

    if kind == "cumulative":
        # A running total of a *flow*: a column that is zero except in the period
        # the event happens. Summing a stock column here would count the same
        # balance again every month.
        this_period = float(values.sum() if mask is None else values[mask].sum())
        running[metric.name] = running.get(metric.name, 0.0) + this_period
        return running[metric.name]

    if kind == "weighted_mean":
        weights = _numeric(frame, metric.weight, metric)
        if mask is not None:
            values, weights = values[mask], weights[mask]
        total = float(weights.sum())
        # Unweighted would treat a EUR 40m facility and a EUR 400k one alike; an
        # empty or zero-weight book has no average rather than a zero one.
        return float((values * weights).sum() / total) if total > 0 else float("nan")

    if kind == "share_where":
        assert mask is not None
        total = float(values.sum())
        return float(values[mask].sum() / total) if total > 0 else float("nan")

    if kind == "max_group_share":
        if metric.group not in frame.columns:
            raise MetricError(
                f"metric {metric.name!r} groups by {metric.group!r}, which is not in the panel"
            )
        rows = frame if mask is None else frame[mask]
        if rows.empty:
            return float("nan")
        by_group = _numeric(rows, metric.column, metric).groupby(rows[metric.group]).sum()
        total = float(by_group.sum())
        return float(by_group.max() / total) if total > 0 else float("nan")

    if kind == "effective_count":
        return _effective_count(frame, metric, mask, values)

    if kind == "turnover":
        return _turnover(frame, metric, mask, values, running)

    raise MetricError(f"metric {metric.name!r} has unknown kind {kind!r}")


def _effective_count(
    frame: pd.DataFrame, metric: Metric, mask: pd.Series | None, values: pd.Series
) -> float:
    """How many groups the book *behaves* as, given how concentrated it is.

    A portfolio of a hundred obligors where two of them carry half the money is
    not a hundred-name portfolio, and a plain count says it is. This is the
    inverse Herfindahl — one over the sum of squared shares — which answers the
    same question a rating agency's diversity score answers, from a formula that
    is ordinary statistics rather than anyone's model.

    It reads as a count, which is the point. A hundred equal obligors return
    100.0; move half the money into two of them and it falls toward 8. The
    number is directly comparable to the headline obligor count, and the gap
    between the two *is* the concentration.
    """
    if metric.group not in frame.columns:
        raise MetricError(
            f"metric {metric.name!r} groups by {metric.group!r}, which is not in the panel"
        )
    rows = frame if mask is None else frame[mask]
    if rows.empty:
        return float("nan")

    weights = values if mask is None else values[mask]
    by_group = weights.groupby(rows[metric.group]).sum()
    total = float(by_group.sum())
    if total <= 0:
        return float("nan")

    shares = by_group / total
    herfindahl = float((shares**2).sum())
    return float(1.0 / herfindahl) if herfindahl > 0 else float("nan")


def _turnover(
    frame: pd.DataFrame,
    metric: Metric,
    mask: pd.Series | None,
    values: pd.Series,
    running: dict[str, Any],
) -> float:
    """How much of the book left since the last cut-off, as a share of it.

    A managed pool trades: facilities are sold, prepay or mature, and the
    proceeds buy replacements. Total balance can sit almost flat through all of
    it, so the size of the book says nothing about how much of it changed hands.

    Measured on the entities themselves rather than on the balance total, since
    that is the only way to tell a facility that left from one that amortised.
    Departures only: arrivals are already visible as the movement in total
    balance, and counting both would report a pool that replaced every asset as
    200% turned over.

    **Valued at the last balance the entity carried while still outstanding**,
    which is the part that had to be measured rather than assumed. Most packs
    zero the balance as an entity enters its terminal state — that is what
    `state_fields` is for — so the row on which a loan disappears reads zero, and
    a first version of this metric duly reported zero turnover on a book that
    lost a quarter of its loans. It read non-zero on the CLO only because that
    pack happens not to zero `current_par` on exit, which is an accident of one
    pack and not a property of the measure.

    The first cut-off has nothing to compare against and reports nothing rather
    than zero — a book cannot have turned over before it existed, and a zero in
    that slot would drag every average down.
    """
    key = f"_turnover::{metric.name}"
    id_column = metric.entity_column or metric.group
    if not id_column or id_column not in frame.columns:
        raise MetricError(
            f"metric {metric.name!r} needs an `entity_column` naming the identifier that "
            "persists between cut-offs; without it there is no way to tell which entities left"
        )

    rows = frame if mask is None else frame[mask]
    weights = values if mask is None else values[mask]
    current = weights.groupby(rows[id_column]).sum()

    held: pd.Series | None = running.get(key)
    if held is None:
        running[key] = current[current > 0]
        return float("nan")

    departed = held.index.difference(current.index)
    opening = float(held.sum())
    turnover = float(held.reindex(departed).sum() / opening) if opening > 0 else float("nan")

    # Carry the last positive balance forward for everything still here, and
    # forget what has gone. An entity that has amortised to zero but is still
    # reporting keeps its last real balance, which is stale by a period or two
    # and only ever affects the denominator.
    surviving = held.reindex(current.index)
    running[key] = current.where(current > 0, surviving).dropna()
    return turnover


def compute(
    spec: DesignSpec,
    frame: pd.DataFrame,
    period: int,
    date: str,
    running: dict[str, Any],
) -> dict[str, Any]:
    """Every metric for one cut-off.

    ``running`` carries what a metric needs to remember between periods: a
    cumulative total, or the previous cut-off's per-entity balances for
    turnover. Keys are namespaced by metric name, so two metrics of the same
    kind cannot tread on each other.
    """
    row: dict[str, Any] = {"period": period, "date": date}
    for metric in spec.metrics:
        value = _one(frame, metric, period, running)
        if metric.decimals is not None and not np.isnan(value):
            value = round(value, metric.decimals)
        row[metric.name] = value
    return row


def to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)
