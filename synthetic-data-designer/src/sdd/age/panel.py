"""The ageing loop: turn a period-0 book into a panel.

One pass per cut-off. The order below is not arbitrary — each step depends on the
one before it:

1. **Lifecycle step.** Who prepaid, who fell behind, who charged off. Everything
   downstream keys off the new state.
2. **Counters.** Seasoning up, remaining term down.
3. **Amortisation.** Only the entities the lifecycle says are paying.
4. **Indices.** House prices, residual values, whatever the spec overlays.
5. **Accruals.** Arrears grow for another missed payment.
6. **State overrides.** ``state_fields`` is applied last among the value steps so
   a forced value always wins — a redeemed loan's balance is 0 regardless of what
   the amortisation kernel computed.
7. **Per-period derivations.** Ratios and buckets, computed from final values so
   an LTV always matches the balance printed beside it.
8. **Emit**, then drop anything terminal before the next period.

A terminal row is written *once* — the period it goes terminal shows the final
zero balance — and then leaves the pool. That is what makes this a closed pool
with attrition rather than a fixed panel.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sdd.age.dynamics import (
    amortise,
    apply_accruals,
    apply_counters,
    apply_indices,
    seed_accrual_counters,
)
from sdd.age.lifecycle import LifecycleEngine
from sdd.calendar import format_filename, period_dates
from sdd.generate.book import apply_derivations, coerce_dtypes
from sdd.spec.schema import DesignSpec, Scenario

ProgressFn = Callable[[str, float], None]


class AgeingError(RuntimeError):
    """The panel could not be advanced."""


class RowLimitExceeded(AgeingError):
    """The panel grew past the ceiling it was given and was stopped."""


class _Chain:
    """One secondary chain's state, carried across the ageing loop.

    Holds its own state indices and dwell counters, exactly as the primary
    lifecycle does. It has to survive the terminal-row cull the same way, or the
    arrays fall out of step with the frame and every entity is handed some other
    entity's rating.
    """

    def __init__(self, spec: Any, engine: LifecycleEngine) -> None:
        self.spec = spec
        self.engine = engine
        self.state = np.zeros(0, dtype=np.int16)
        self.dwell: dict[str, np.ndarray] = {}

    @property
    def column(self) -> str:
        return self.spec.lifecycle.state_column

    def seed(self, df: pd.DataFrame, rng: np.random.Generator) -> None:
        """Opening states, from the chain's own distribution or its column."""
        lc = self.spec.lifecycle
        n = len(df)
        if lc.initial_distribution:
            states = list(lc.initial_distribution)
            weights = np.array([lc.initial_distribution[s] for s in states], dtype=float)
            labels = rng.choice(states, size=n, p=weights / weights.sum())
        elif self.column in df.columns:
            labels = df[self.column].to_numpy()
        else:
            labels = np.full(n, lc.states[0])
        self.state = self.engine.to_idx(np.asarray(labels))
        self.dwell = self.engine.initial_dwell(n, self.state)

    def keep(self, mask: np.ndarray) -> None:
        self.state = self.state[mask]
        self.dwell = {k: v[mask] for k, v in self.dwell.items()}

    def grow(self, n: int, rng: np.random.Generator) -> None:
        """Seats for entities joining an open pool part-way through."""
        if n <= 0:
            return
        lc = self.spec.lifecycle
        if lc.initial_distribution:
            states = list(lc.initial_distribution)
            weights = np.array([lc.initial_distribution[s] for s in states], dtype=float)
            labels = rng.choice(states, size=n, p=weights / weights.sum())
        else:
            labels = np.full(n, lc.states[0])
        joined = self.engine.to_idx(np.asarray(labels))
        self.state = np.concatenate([self.state, joined])
        fresh = self.engine.initial_dwell(n, joined)
        self.dwell = {
            k: np.concatenate([v, fresh.get(k, np.zeros(n, dtype=np.int32))])
            for k, v in self.dwell.items()
        } or fresh

    def stress_multipliers(self, n: int) -> np.ndarray | None:
        """Per-entity multiplier on the primary's worsening transitions."""
        table = self.spec.coupling.stress
        if not table or len(self.state) != n:
            return None
        by_index = np.ones(len(self.engine.states), dtype=float)
        for label, multiplier in table.items():
            by_index[self.engine.index[label]] = float(multiplier)
        return by_index[self.state]

    def step(self, rng: np.random.Generator) -> None:
        self.state, self.dwell = self.engine.step(self.state, self.dwell, rng)

    def force(self, primary_labels: np.ndarray) -> None:
        """Apply the primary state's veto — a defaulted facility is rated D."""
        for primary_state, forced in self.spec.coupling.forced_by.items():
            hit = primary_labels == primary_state
            if hit.any():
                self.state[hit] = self.engine.index[forced]

    def write(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column] = self.engine.to_label(self.state)
        return df


def run_ageing(
    spec: DesignSpec,
    book: pd.DataFrame,
    out_dir: str | Path,
    *,
    seed: int = 42,
    scenario: Scenario | None = None,
    progress: ProgressFn | None = None,
    write_files: bool = True,
    max_rows: int | None = None,
    group_state: dict[str, pd.DataFrame] | None = None,
) -> dict:
    """Age ``book`` across the spec's calendar, writing one file per period.

    Returns a summary dict: per-period row counts, the state mix, and the paths
    written — enough for a caller (CLI today, API later) to report without
    re-reading the output.
    """
    if spec.lifecycle is None:
        raise AgeingError(
            "this spec has no `lifecycle` section, so there is nothing to age. "
            "Generate the period-0 book instead."
        )

    out_path = Path(out_dir)
    if write_files:
        out_path.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    engine = LifecycleEngine(spec.lifecycle, spec.entity.calendar.periods_per_year)
    chains = [
        _Chain(
            spec=chain,
            engine=LifecycleEngine(chain.lifecycle, spec.entity.calendar.periods_per_year),
        )
        for chain in spec.secondary_chains
    ]
    dates = period_dates(spec.entity.calendar)
    lc = spec.lifecycle

    performing_state = lc.states[0]
    hazard_mult, index_shift, recovery_mult = _scenario_knobs(spec, scenario)
    segment_stress = scenario.segment_stress if scenario else None

    current = book.copy().reset_index(drop=True)
    dwell = engine.initial_dwell(len(current), engine.to_idx(current[lc.state_column].to_numpy()))
    accrual_counters = seed_accrual_counters(current, spec.dynamics.accruals, _dpd_column(spec))
    # The opening states have to reach the opening book, not just the chain's
    # own arrays. Seeded but unwritten, period 0 carried whatever the column's
    # generator produced — every facility rated "B" from a constant — and the
    # declared opening mix only appeared from period 1. Anything derived from
    # the column, the CCC flag among it, was wrong for exactly one cut-off,
    # which is the hardest kind of wrong to notice.
    if chains:
        for chain in chains:
            chain.seed(current, rng)
            current = chain.write(current)
        current = apply_derivations(spec, current, stage="both", extra={**spec.params, "period": 0})
        current = coerce_dtypes(spec, current)

    written: list[str] = []
    mixes: list[dict] = []
    frames: list[pd.DataFrame] = []
    metric_rows: list[dict] = []
    running_totals: dict[str, float] = {}

    opening_size = len(current)
    # Identifiers continue from the opening book, so a later cohort cannot be
    # handed an identifier an existing entity already holds.
    next_index = opening_size
    originated = 0
    total_rows = 0

    for period, date in enumerate(dates):
        joined = 0
        if period > 0:
            if current.empty and spec.originations is None:
                break
            current, dwell, accrual_counters = _step(
                spec,
                engine,
                current,
                dwell,
                accrual_counters,
                chains=chains,
                period=period,
                date=date,
                rng=rng,
                hazard_mult=hazard_mult,
                index_shift=index_shift,
                recovery_mult=recovery_mult,
                performing_state=performing_state,
                segment_stress=segment_stress,
            )
            if spec.originations is not None:
                current, dwell, accrual_counters, joined = originate(
                    spec,
                    engine,
                    current,
                    dwell,
                    accrual_counters,
                    period=period,
                    date=date,
                    rng=rng,
                    opening_size=opening_size,
                    next_index=next_index,
                    group_state=group_state,
                )
                next_index += joined
                originated += joined
                for chain in chains:
                    chain.grow(joined, rng)

        out = to_output(spec, current)

        # The ceiling is checked here, per period, rather than projected up
        # front. A projection has to model originations, terminal-state exits
        # and the scenario overlay, and a projection that is wrong is worse than
        # none: it reads as a limit and does not hold. This is the row count
        # itself, and the period is abandoned before it is written or kept, so
        # the run stops having spent one period's memory rather than the pool's.
        total_rows += len(out)
        if max_rows is not None and total_rows > max_rows:
            raise RowLimitExceeded(
                f"this run reached {total_rows:,} rows at period {period + 1} of "
                f"{len(dates)}, past the {max_rows:,}-row ceiling. Rows are entities "
                "times cut-offs, and originations add entities as the pool ages, so a "
                "small entity count can still produce a very large panel. Ask for "
                "fewer entities or fewer periods, or run it locally, where nothing "
                "is capped."
            )

        # Computed on the frame about to be written, so the figures are of
        # exactly the rows they describe. Recomputing them from the finished
        # panel would be a second implementation of the same arithmetic, which
        # is how a report and its data drift apart.
        if spec.metrics:
            from sdd import metrics as metrics_module

            metric_rows.append(
                metrics_module.compute(spec, out, period, date.strftime("%Y-%m-%d"), running_totals)
            )

        mix = current[lc.state_column].value_counts().to_dict()
        mixes.append(
            {
                "period": period,
                "date": date.strftime("%Y-%m-%d"),
                "rows": len(out),
                "originated": joined,
                **mix,
            }
        )

        if write_files:
            written += _write_period(spec, out, date, period, out_path)
        if spec.emit.write_panel:
            frames.append(out)

        if progress:
            progress(f"period {period + 1}/{len(dates)}", (period + 1) / len(dates))

        # Terminal rows were just written; they do not survive into the next period.
        terminal = engine.is_terminal(engine.to_idx(current[lc.state_column].to_numpy()))
        if terminal.any():
            keep = ~terminal
            current = current.loc[keep].reset_index(drop=True)
            dwell = {k: v[keep] for k, v in dwell.items()}
            accrual_counters = {k: v[keep] for k, v in accrual_counters.items()}
            for chain in chains:
                chain.keep(keep)

    panel_path = None
    if spec.emit.write_panel and frames and write_files:
        panel = pd.concat(frames, ignore_index=True)
        panel_path = out_path / spec.emit.panel_filename
        panel.to_parquet(panel_path, index=False)

    metrics_path = None
    if spec.metrics and metric_rows and write_files:
        from sdd import metrics as metrics_module

        table = metrics_module.to_frame(metric_rows)
        metrics_path = out_path / "portfolio_metrics.parquet"
        table.to_parquet(metrics_path, index=False)
        table.to_csv(out_path / "portfolio_metrics.csv", index=False)

    return {
        "periods": len(mixes),
        "files": written,
        "metrics": metric_rows,
        "metrics_path": str(metrics_path) if metrics_path else None,
        "panel": str(panel_path) if panel_path else None,
        "mix": mixes,
        "final_rows": len(current),
        "total_rows": total_rows,
        "opening_entities": opening_size,
        "originated": originated,
    }


# ---------------------------------------------------------------------------
# one period
# ---------------------------------------------------------------------------


def _segment_multipliers(
    df: pd.DataFrame, segment_stress: dict[str, dict[str, float]] | None
) -> np.ndarray | None:
    """Per-entity multipliers from a scenario's segment overlay.

    `default_multiplier` moves the whole book at once, which is not how a
    downturn arrives: 2008 was not every sector worsening by the same factor. A
    recession lands on the sectors exposed to it, and a portfolio's real risk is
    how much of it sits in those sectors — which a uniform multiplier cannot
    express, because the concentration figures come out identical no matter how
    severe it is set.

    Segments compose by multiplication. A facility that is both in a stressed
    industry and in a stressed country carries both, which is the intended
    reading: those are two independent reasons to worry about it.
    """
    if not segment_stress:
        return None

    out: np.ndarray | None = None
    for column, table in segment_stress.items():
        if column not in df.columns or not table:
            continue
        # Segments not named in the table are left alone rather than zeroed —
        # a scenario says which parts of the book it lands on, not which parts
        # it spares.
        mapped = df[column].map(table).astype(float).fillna(1.0).to_numpy()
        out = mapped if out is None else out * mapped
    return out


def _step(
    spec: DesignSpec,
    engine: LifecycleEngine,
    df: pd.DataFrame,
    dwell: dict[str, np.ndarray],
    accrual_counters: dict[str, np.ndarray],
    *,
    period: int,
    date: pd.Timestamp,
    rng: np.random.Generator,
    hazard_mult: dict[str, float],
    index_shift: dict[str, float],
    recovery_mult: float,
    performing_state: str,
    chains: list[_Chain] | None = None,
    segment_stress: dict[str, dict[str, float]] | None = None,
) -> tuple[pd.DataFrame, dict[str, np.ndarray], dict[str, np.ndarray]]:
    lc = spec.lifecycle
    assert lc is not None
    df = df.reset_index(drop=True)

    # 1. lifecycle
    prev_idx = engine.to_idx(df[lc.state_column].to_numpy())
    condition_masks = _condition_masks(spec, engine, df, period)
    # Secondary state feeds the primary's odds *before* the primary moves: a
    # facility already rated CCC is more likely to slip this month, and reading
    # its rating after the move would apply this month's downgrade to last
    # month's transition.
    row_multipliers = None
    for chain in chains or []:
        contribution = chain.stress_multipliers(len(df))
        if contribution is None:
            continue
        row_multipliers = (
            contribution if row_multipliers is None else row_multipliers * contribution
        )

    # A scenario's segment overlay rides the same per-entity channel as the
    # rating coupling, deliberately. Both say the same kind of thing — *these*
    # entities are more likely to slip than the book as a whole — and giving
    # them one mechanism means a rating-driven stress and a sector-driven one
    # compose by multiplication instead of one silently overwriting the other.
    contribution = _segment_multipliers(df, segment_stress)
    if contribution is not None:
        row_multipliers = (
            contribution if row_multipliers is None else row_multipliers * contribution
        )

    new_idx, dwell = engine.step(
        prev_idx,
        dwell,
        rng,
        hazard_multipliers=hazard_mult,
        condition_masks=condition_masks,
        row_multipliers=row_multipliers,
    )
    labels = engine.to_label(new_idx)
    df[lc.state_column] = labels

    terminal_mask = engine.is_terminal(new_idx)
    active_mask = ~terminal_mask

    # 2. counters
    if spec.dynamics.counters:
        df = apply_counters(df, spec.dynamics.counters, spec.params)

    # 3. amortisation
    am = spec.dynamics.amortisation
    if am:
        if am.only_when_state is None:
            pays = active_mask
        else:
            wanted = (
                [am.only_when_state] if isinstance(am.only_when_state, str) else am.only_when_state
            )
            pays = np.isin(labels, wanted)
        df[am.balance] = np.round(amortise(df, am, pays_mask=pays, active_mask=active_mask), 2)

    # 4. indices
    if spec.dynamics.indices:
        df = apply_indices(
            df,
            spec.dynamics.indices,
            period,
            spec.entity.calendar.periods_per_year,
            rng,
            annual_shift=index_shift,
        )

    # 4b. recovery — booked on the way into a write-off state, from the balance
    # the entity carried in. Computed before `state_fields`, which is usually
    # what zeroes that balance.
    if spec.dynamics.recovery:
        df = apply_recovery(
            spec,
            df,
            previous=engine.to_label(prev_idx),
            current=labels,
            rate_multiplier=recovery_mult,
        )

    # 5. accruals
    if spec.dynamics.accruals:
        df, accrual_counters = apply_accruals(
            df,
            spec.dynamics.accruals,
            state_labels=labels,
            terminal_mask=terminal_mask,
            performing_state=performing_state,
            counters=accrual_counters,
        )

    # 6. forced per-state values — these win over everything computed above
    df = apply_state_fields(spec, df, labels)

    # 6b. secondary chains: migrate, then let the primary state overrule.
    #
    # Order is the point. Stepping first lets a rating drift on its own, which
    # is the whole reason the chain exists — a downgrade normally arrives before
    # any distress is visible. Forcing second means a facility that defaulted
    # *this* period is rated D in the row that records the default, rather than
    # carrying whatever its own chain happened to draw.
    for chain in chains or []:
        chain.step(rng)
        chain.force(labels)
        df = chain.write(df)

    # 7. time column, then per-period derivations that depend on it
    df[spec.entity.time_column] = date.strftime("%Y-%m-%d")
    df = apply_derivations(spec, df, stage="period", extra={**spec.params, "period": period})
    df = coerce_dtypes(spec, df)

    return df, dwell, accrual_counters


def originate(
    spec: DesignSpec,
    engine: LifecycleEngine,
    df: pd.DataFrame,
    dwell: dict[str, np.ndarray],
    accrual_counters: dict[str, np.ndarray],
    *,
    period: int,
    date: pd.Timestamp,
    rng: np.random.Generator,
    opening_size: int,
    next_index: int,
    group_state: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, np.ndarray], dict[str, np.ndarray], int]:
    """Add the entities joining the pool at this cut-off.

    They are built by the same book builder as the opening cohort, so they are
    drawn from the same distributions and carry the same derived columns — a new
    loan looks like the portfolio it is joining. Three things differ:

    *They enter at this cut-off, not the first.* The time column is stamped with
    this period's date rather than the calendar's start.

    *They have not been aged.* This runs after the whole ageing step, so a loan
    written this month is not also charged a month of interest this month.

    *Under ``fresh`` they are new, not acquired.* The healthiest state, and every
    upward-ticking counter set to zero — a counter that rises each period
    measures elapsed time, and none has elapsed for a loan written today.
    """
    plan = spec.originations
    assert plan is not None
    count = plan.count_for(period, opening_size)
    if count < 1:
        return df, dwell, accrual_counters, 0

    from sdd.generate.book import build_book

    # Drawn from the run's own generator so the whole panel reproduces from one
    # seed, and differs period to period.
    cohort_seed = int(rng.integers(0, 2**32 - 1))
    new = build_book(
        spec,
        count,
        seed=cohort_seed,
        id_offset=next_index,
        at=date.strftime("%Y-%m-%d"),
        group_state=group_state,
        fresh_cohort=True,
    )

    lc = spec.lifecycle
    assert lc is not None
    if plan.fresh:
        new[lc.state_column] = lc.states[0]
        for counter in spec.dynamics.counters:
            if counter.step is not None and counter.step > 0 and counter.column in new.columns:
                new[counter.column] = 0
    for column, value in plan.reset.items():
        new[column] = value
    if plan.reset_expr:
        from sdd.generate.deriver import evaluate_on

        env = {
            **spec.params,
            "period": period,
            "period_year": date.year,
            "period_month": date.month,
            "period_day": date.day,
        }
        for column, expression in plan.reset_expr.items():
            try:
                new[column] = evaluate_on(expression, new, env)
            except Exception as exc:
                raise AgeingError(f"originations.reset_expr for {column!r} failed: {exc}") from exc

    labels = new[lc.state_column].to_numpy()
    new = apply_state_fields(spec, new, labels)
    # Re-derived after the resets, so a ratio computed at origination matches the
    # values the entity actually enters with.
    new = apply_derivations(spec, new, stage="book")
    new = coerce_dtypes(spec, new)

    missing = [c for c in df.columns if c not in new.columns]
    if missing:
        raise AgeingError(
            f"the entities joining at period {period} are missing columns the pool already "
            f"has: {missing}"
        )
    new = new[list(df.columns)]

    id_column = spec.entity.id_column
    clashing = set(new[id_column]) & set(df[id_column])
    if clashing:
        raise AgeingError(
            f"{len(clashing)} entity identifier(s) created at period {period} already exist in "
            f"the pool, e.g. {sorted(clashing)[:3]}. An open pool needs identifiers that cannot "
            f"repeat: give `entity.id_format` a '{{seq}}' placeholder, or generate "
            f"{id_column!r} with a `sequence` or `uuid` generator"
        )

    joined_dwell = engine.initial_dwell(len(new), engine.to_idx(labels))
    dwell = {
        name: np.concatenate([values, joined_dwell.get(name, np.zeros(len(new), dtype=np.int32))])
        for name, values in dwell.items()
    }
    joined_counters = seed_accrual_counters(new, spec.dynamics.accruals, _dpd_column(spec))
    accrual_counters = {
        name: np.concatenate(
            [values, joined_counters.get(name, np.zeros(len(new), dtype=values.dtype))]
        )
        for name, values in accrual_counters.items()
    }

    return pd.concat([df, new], ignore_index=True), dwell, accrual_counters, len(new)


def apply_recovery(
    spec: DesignSpec,
    df: pd.DataFrame,
    *,
    previous: np.ndarray,
    current: np.ndarray,
    rate_multiplier: float = 1.0,
) -> pd.DataFrame:
    """Book the recovered share of the balance for entities writing off this period.

    Only entities *entering* a recovery state are booked. Without that test an
    absorbing state would book a recovery every period an entity sat in it, and
    a pool would recover several times what it lost.
    """
    rec = spec.dynamics.recovery
    assert rec is not None
    if rec.balance not in df.columns:
        raise AgeingError(
            f"dynamics.recovery reads {rec.balance!r}, which is not in the panel at this point"
        )

    entering = np.isin(current, rec.on_states) & (current != previous)
    # to_numpy before filling: a balance column can arrive as object dtype, and
    # pandas' own fillna would downcast it with a deprecation warning.
    balance = np.nan_to_num(pd.to_numeric(df[rec.balance], errors="coerce").to_numpy(dtype=float))
    # Capped at 1.0: a scenario may lower what comes back, and may not conjure a
    # recovery larger than the balance being recovered against.
    rate = min(rec.rate * rate_multiplier, 1.0)
    booked = np.where(entering, np.round(balance * rate, 2), 0.0)
    df[rec.target] = booked
    return df


def apply_state_fields(spec: DesignSpec, df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    """Force the values a spec pins to each state (zero balance when redeemed, …)."""
    lc = spec.lifecycle
    if lc is None or not lc.state_fields:
        return df
    for state, fields in lc.state_fields.items():
        mask = labels == state
        if not mask.any():
            continue
        for column, value in fields.items():
            if column not in df.columns:
                df[column] = value
            else:
                df.loc[mask, column] = value
    return df


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------


def _condition_masks(
    spec: DesignSpec, engine: LifecycleEngine, df: pd.DataFrame, period: int
) -> dict[str, np.ndarray]:
    """Evaluate each condition hazard's expression against the current frame.

    The lifecycle engine works in state indices and never sees the frame, so the
    evaluation happens here, where both the frame and the restricted expression
    evaluator are already to hand.

    Counters are advanced first, on a throwaway copy. A condition describes the
    period about to be lived, so it has to read counters as they will stand at
    that period's close, not as they stood at its open. Without this a loan with
    one month left is still "performing" through the month its term runs out and
    only matures the month after — every loan reporting one period past the end
    of its own term. The real counters still tick in their usual place; this view
    is discarded.
    """
    hazards = engine.condition_hazards
    if not hazards:
        return {}

    from sdd.generate.deriver import evaluate_mask

    view = df
    if spec.dynamics.counters:
        view = apply_counters(df.copy(), spec.dynamics.counters, spec.params)

    masks: dict[str, np.ndarray] = {}
    for hz in hazards:
        try:
            masks[hz.name] = evaluate_mask(hz.when, view, {"period": period})
        except Exception as exc:
            raise AgeingError(
                f"condition hazard {hz.name!r} could not evaluate {hz.when!r}: {exc}"
            ) from exc
    return masks


def to_output(spec: DesignSpec, df: pd.DataFrame) -> pd.DataFrame:
    """Select and order the columns that go to disk, dropping helpers."""
    cols = spec.output_columns()
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AgeingError(f"output columns missing from the panel: {missing}")
    return df[cols].copy()


def _write_period(
    spec: DesignSpec, out: pd.DataFrame, date: pd.Timestamp, period: int, out_dir: Path
) -> list[str]:
    written: list[str] = []
    stem = format_filename(spec.emit.filename, date, period, spec.meta.name)
    base, ext = os.path.splitext(stem)
    target_dir = out_dir / spec.emit.cutoff_dir if spec.emit.cutoff_dir else out_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    for fmt in spec.emit.formats:
        path = target_dir / (stem if ext.lstrip(".") == fmt else f"{base}.{fmt}")
        if fmt == "csv":
            out.to_csv(path, index=False, float_format=spec.emit.float_format)
        else:
            out.to_parquet(path, index=False)
        written.append(str(path))
    return written


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------


def _scenario_knobs(
    spec: DesignSpec, scenario: Scenario | None
) -> tuple[dict[str, float], dict[str, float], float]:
    if scenario is None:
        return {}, {}, 1.0
    hazard_mult = {
        hz.name: scenario.prepayment_multiplier
        for hz in (spec.lifecycle.hazards if spec.lifecycle else [])
        if hz.kind == "bernoulli"
    }
    return hazard_mult, dict(scenario.index_shift), scenario.recovery_multiplier


def stress_transitions(spec: DesignSpec, scenario: Scenario) -> list[list[float]] | None:
    """Scale the probability of *worsening* under a scenario overlay.

    The same rescaling the configure form uses to hit a target default rate, so a
    scenario and a hand-set rate cannot drift into meaning different things.
    """
    lc = spec.lifecycle
    if lc is None or lc.transitions is None or scenario.default_multiplier == 1.0:
        return None if lc is None else lc.transitions

    from sdd.age.calibrate import scale_worsening

    return scale_worsening(lc.transitions, scenario.default_multiplier)


def _dpd_column(spec: DesignSpec) -> str | None:
    """Best guess at a days-past-due column, used to seed arrears counters."""
    for candidate in ("days_past_due", "dpd", "days_in_arrears"):
        if spec.column(candidate) or candidate in {d.target for d in spec.derivations}:
            return candidate
    return None
