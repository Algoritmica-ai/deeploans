"""How the numbers move each period: amortisation, indices, counters, accruals.

Each function here is a small vectorised operation selected by a ``kind`` in the
spec, so adding an asset class means adding a kernel, not editing the panel loop.

The one modelling decision worth stating plainly, because it drives most of the
output: **a borrower who does not pay does not pay down principal.** When a loan
is in any delinquency state its balance is *frozen*, not amortised and not grown
by accrued interest. Under ESMA reporting, ``current_balance`` is outstanding
*principal*; unpaid interest is tracked separately as arrears. Upstream made the
same choice and it is the reason arrears accrue while balances sit still.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sdd.generate.deriver import evaluate_mask
from sdd.spec.schema import Accrual, Amortisation, Counter, Index


class DynamicsError(RuntimeError):
    """A dynamics rule that cannot be applied to the data at hand."""


# ---------------------------------------------------------------------------
# amortisation
# ---------------------------------------------------------------------------


def amortise(
    df: pd.DataFrame,
    am: Amortisation,
    *,
    pays_mask: np.ndarray,
    active_mask: np.ndarray,
) -> np.ndarray:
    """Return the next-period balance for every row.

    ``pays_mask``
        Rows whose borrower is deemed to have paid this period — normally the
        performing ones. Everyone else keeps last period's balance.
    ``active_mask``
        Rows still in the pool. Terminal rows are zeroed by the lifecycle's
        ``state_fields``, not here, so this only guards the arithmetic.
    """
    balance = df[am.balance].astype(float).to_numpy()
    nxt = _amortisation_kernel(df, am, balance)

    # Rows the spec pins flat (interest-only loans, say) never amortise.
    if am.flat_when:
        flat = evaluate_mask(am.flat_when, df)
        nxt = np.where(flat, balance, nxt)

    nxt = np.where(pays_mask, nxt, balance)
    nxt = np.maximum(nxt, am.floor)
    return np.where(active_mask, nxt, balance)


def _amortisation_kernel(df: pd.DataFrame, am: Amortisation, balance: np.ndarray) -> np.ndarray:
    kind = am.kind

    if kind in ("interest_only", "none"):
        return balance

    if kind == "annuity":
        # Level payment: interest accrues on the balance, the payment covers it,
        # and whatever is left over retires principal.
        rate = df[am.rate].astype(float).to_numpy() / 100.0 / 12.0
        payment = df[am.payment].astype(float).to_numpy()
        return balance * (1.0 + rate) - payment

    if kind == "linear":
        # Equal principal each period.
        if am.payment:
            return balance - df[am.payment].astype(float).to_numpy()
        term = df[am.term].astype(float).to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            principal = np.where(term > 0, balance / term, balance)
        return balance - principal

    if kind == "bullet":
        # Nothing repays until maturity, then the whole balance falls due.
        if not am.term:
            return balance
        term = df[am.term].astype(float).to_numpy()
        return np.where(term <= 1, 0.0, balance)

    if kind in ("revolving", "depreciation"):
        # A proportional drift: negative for a paydown, positive for a drawdown.
        assert am.rate_per_period is not None
        sign = -1.0 if kind == "depreciation" else 1.0
        return balance * (1.0 + sign * am.rate_per_period)

    raise DynamicsError(f"no amortisation kernel for kind {kind!r}")


def annuity_payment(
    principal: np.ndarray, annual_rate_pct: np.ndarray, n_periods: np.ndarray
) -> np.ndarray:
    """The level payment that retires ``principal`` over ``n_periods``.

    Exposed because both the book (setting ``scheduled_monthly_payment``) and the
    profiler (checking whether observed balances follow an annuity) need it.
    """
    r = np.asarray(annual_rate_pct, dtype=float) / 100.0 / 12.0
    n = np.asarray(n_periods, dtype=float)
    growth = np.power(1.0 + r, n)
    with np.errstate(divide="ignore", invalid="ignore"):
        payment = np.where(r > 0, principal * r * growth / (growth - 1.0), principal / n)
    return payment


# ---------------------------------------------------------------------------
# index overlays
# ---------------------------------------------------------------------------


def index_multiplier(
    idx: Index, period: int, periods_per_year: float, rng: np.random.Generator
) -> float:
    """The factor applied to ``idx.applies_to`` columns for one period.

    ``period`` is 1-based: period 0 is the book itself and is never indexed.
    """
    if idx.kind == "series":
        assert idx.series
        pos = min(period - 1, len(idx.series) - 1)
        base = float(idx.series[max(pos, 0)])
    else:
        assert idx.annual is not None
        base = float((1.0 + idx.annual) ** (1.0 / periods_per_year))

    if idx.volatility > 0:
        base *= float(np.exp(rng.normal(0.0, idx.volatility)))
    return base


def apply_indices(
    df: pd.DataFrame,
    indices: list[Index],
    period: int,
    periods_per_year: float,
    rng: np.random.Generator,
    *,
    annual_shift: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Scale every indexed column. ``annual_shift`` is the scenario hook."""
    shift = annual_shift or {}
    for idx in indices:
        eff = idx
        if idx.name in shift and idx.kind == "constant_drift":
            eff = idx.model_copy(update={"annual": (idx.annual or 0.0) + shift[idx.name]})
        mult = index_multiplier(eff, period, periods_per_year, rng)
        for col in idx.applies_to:
            if col not in df.columns:
                raise DynamicsError(f"index {idx.name!r} applies to missing column {col!r}")
            values = df[col].astype(float) * mult
            # Clipped every step rather than once at the end, so the walk cannot
            # spend twenty periods out of bounds and be pulled back at the last
            # one. A bound that only holds on the final cut-off is not a bound.
            if idx.clip_min is not None or idx.clip_max is not None:
                values = values.clip(lower=idx.clip_min, upper=idx.clip_max)
            df[col] = values.round(2)
    return df


# ---------------------------------------------------------------------------
# counters
# ---------------------------------------------------------------------------


def apply_counters(
    df: pd.DataFrame, counters: list[Counter], extra: dict | None = None
) -> pd.DataFrame:
    """Tick each counter: a fixed step, or a formula recomputed from scratch."""
    from sdd.generate.book import _coerce
    from sdd.generate.deriver import evaluate_on

    for ctr in counters:
        if ctr.column not in df.columns and ctr.step is not None:
            raise DynamicsError(
                f"counter {ctr.column!r} steps by {ctr.step} but the column does not exist; "
                "it must be produced at period 0 by a generator or derivation"
            )
        values = (
            df[ctr.column].astype(float) + ctr.step
            if ctr.step is not None
            else evaluate_on(ctr.expr, df, extra)  # type: ignore[arg-type]
        )
        values = np.asarray(values, dtype=float)
        if ctr.clip_min is not None or ctr.clip_max is not None:
            values = np.clip(values, ctr.clip_min, ctr.clip_max)
        df[ctr.column] = values
        if ctr.dtype:
            df[ctr.column] = _coerce(df[ctr.column], ctr.dtype)
    return df


# ---------------------------------------------------------------------------
# accruals
# ---------------------------------------------------------------------------


def apply_accruals(
    df: pd.DataFrame,
    accruals: list[Accrual],
    *,
    state_labels: np.ndarray,
    terminal_mask: np.ndarray,
    performing_state: str,
    counters: dict[str, np.ndarray],
) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    """Grow or reset each running total.

    The counter is carried across periods rather than recomputed, because arrears
    are a *history*: three missed payments is three payments owed, and nothing in
    the current row records how long the borrower has been behind.

    Terminal states always reset. A redeemed loan owes nothing, whatever its
    delinquency state was the month before.
    """
    out = dict(counters)
    for acc in accruals:
        perf = acc.performing_state or performing_state
        if acc.when == "always":
            qualifies = np.ones(len(df), dtype=bool)
        elif acc.when == "in_states":
            qualifies = np.isin(state_labels, list(acc.states or []))
        else:  # not_performing
            qualifies = state_labels != perf

        reset = np.isin(state_labels, list(acc.reset_states or [perf])) | terminal_mask
        qualifies &= ~reset

        prev = out.get(acc.column, np.zeros(len(df), dtype=np.int64))
        count = np.where(qualifies, prev + 1, 0).astype(np.int64)
        out[acc.column] = count

        per_period = float(acc.add) if _is_number(acc.add) else df[acc.add].astype(float).to_numpy()
        df[acc.column] = np.round(np.where(qualifies, per_period * count, 0.0), 2)
    return df, out


def _is_number(value: str) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def seed_accrual_counters(
    df: pd.DataFrame, accruals: list[Accrual], dpd_column: str | None, period_days: int = 30
) -> dict[str, np.ndarray]:
    """Recover accrual counters for the book from the initial arrears amount.

    Without this the panel has a discontinuity at period 1: a loan that starts
    75 days past due would jump from three payments owed to one. Upstream hit
    exactly this and seeded the counter from the initial days-past-due.
    """
    counters: dict[str, np.ndarray] = {}
    for acc in accruals:
        if acc.column not in df.columns:
            counters[acc.column] = np.zeros(len(df), dtype=np.int64)
            continue
        if dpd_column and dpd_column in df.columns:
            dpd = df[dpd_column].astype(float).to_numpy()
            counters[acc.column] = np.where(
                dpd <= 0, 0, np.maximum(np.ceil(dpd / period_days), 1)
            ).astype(np.int64)
        else:
            per_period = (
                float(acc.add) if _is_number(acc.add) else df[acc.add].astype(float).to_numpy()
            )
            amount = df[acc.column].astype(float).to_numpy()
            with np.errstate(divide="ignore", invalid="ignore"):
                counters[acc.column] = np.where(
                    (per_period > 0) & (amount > 0), np.round(amount / per_period), 0
                ).astype(np.int64)
    return counters
