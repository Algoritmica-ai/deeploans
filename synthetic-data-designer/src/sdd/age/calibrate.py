"""Turn portfolio rates into engine settings, and back again.

A transition matrix is the honest representation of how a pool behaves, and it
is the wrong thing to put in front of someone who wants a 3% default rate. This
module is the translation layer between the two, in both directions:

**Reading** — ``implied_default_rate`` walks the matrix forward a year and reports
what it actually produces, so the UI can show the rate a spec already implies
instead of an empty box.

**Writing** — ``set_default_rate`` finds the stress multiplier that lands on a
requested rate and applies it to the matrix, scaling the probability of getting
worse and renormalising each row. The matrix stays a valid matrix, and it stays
the thing the engine runs; nothing is bolted on beside it.

Prepayment and recovery are simpler and are handled the same way for symmetry:
prepayment is a hazard's annual rate, recovery is a share of the balance booked
when an entity writes off.
"""

from __future__ import annotations

import numpy as np

from sdd.spec.schema import BernoulliHazard, DesignSpec, Recovery

# Names that mark a hazard as the prepayment/redemption one when the spec does
# not make it obvious from the states alone.
PREPAY_NAME_HINTS = ("prepay", "attrition", "redeem", "redemption", "early_repayment")

# How close the solver has to get before it stops.
RATE_TOLERANCE = 1e-4
MAX_ITERATIONS = 60


# ---------------------------------------------------------------------------
# which states mean "gone bad"
# ---------------------------------------------------------------------------


def default_states(spec: DesignSpec) -> list[str]:
    """States that count as a default.

    Absorbing states are the definition — a pool's "Defaulted" bucket is exactly
    the one an entity cannot recover from. Write-off states (terminal, and only
    reachable from an absorbing one) count too, because an entity that has
    already been written off certainly defaulted.
    """
    lc = spec.lifecycle
    if lc is None:
        return []
    states = list(lc.absorbing)
    for hz in lc.hazards:
        if hz.kind == "dwell_time" and hz.from_state in lc.absorbing:
            states.append(hz.to_state)
    return list(dict.fromkeys(states))


def prepayment_hazard(spec: DesignSpec) -> BernoulliHazard | None:
    """The hazard that takes a healthy entity out of the pool early."""
    lc = spec.lifecycle
    if lc is None:
        return None
    bad = set(default_states(spec))
    candidates = [
        hz
        for hz in lc.hazards
        if isinstance(hz, BernoulliHazard) and hz.to_state in lc.terminal and hz.to_state not in bad
    ]
    if not candidates:
        return None
    for hz in candidates:
        if any(hint in hz.name.lower() for hint in PREPAY_NAME_HINTS):
            return hz
    return candidates[0]


# ---------------------------------------------------------------------------
# reading the current rates
# ---------------------------------------------------------------------------


def implied_default_rate(spec: DesignSpec) -> float | None:
    """The annual default rate the matrix already produces.

    Walks a cohort of performing entities forward one year and reports the share
    that ended up in a default state. This is a cumulative first-year rate, which
    is what "default rate" means on a tape — not the one-period transition
    probability, which is roughly twelve times smaller and endlessly confused
    with it.
    """
    lc = spec.lifecycle
    if lc is None or lc.transitions is None:
        return None

    states = lc.resolved_transition_states
    bad = [s for s in default_states(spec) if s in states]
    if not bad:
        return None

    matrix = np.asarray(lc.transitions, dtype=float)
    position = _starting_distribution(spec, states)
    steps = round(spec.entity.calendar.periods_per_year)

    for _ in range(max(steps, 1)):
        position = position @ matrix
    indices = [states.index(s) for s in bad]
    return float(np.clip(position[indices].sum(), 0.0, 1.0))


def _starting_distribution(spec: DesignSpec, states: list[str]) -> np.ndarray:
    """Where a cohort starts: the spec's own opening mix, or all-performing."""
    lc = spec.lifecycle
    assert lc is not None
    position = np.zeros(len(states))
    if lc.initial_distribution:
        for name, share in lc.initial_distribution.items():
            if name in states:
                position[states.index(name)] = share
    if position.sum() <= 0:
        position[0] = 1.0
    return position / position.sum()


def implied_prepayment_rate(spec: DesignSpec) -> float | None:
    hazard = prepayment_hazard(spec)
    if hazard is None:
        return None
    if hazard.annual_rate is not None:
        return float(hazard.annual_rate)
    rate = hazard.rate_per_period(spec.entity.calendar.periods_per_year)
    periods = spec.entity.calendar.periods_per_year
    return float(1.0 - (1.0 - rate) ** periods)


def implied_recovery_rate(spec: DesignSpec) -> float | None:
    return spec.dynamics.recovery.rate if spec.dynamics.recovery else None


def rates(spec: DesignSpec) -> dict[str, float | None]:
    """Every ageing rate a spec currently implies, for the configure form."""
    return {
        "default_rate": implied_default_rate(spec),
        "prepayment_rate": implied_prepayment_rate(spec),
        "recovery_rate": implied_recovery_rate(spec),
    }


# ---------------------------------------------------------------------------
# writing them back
# ---------------------------------------------------------------------------


def scale_worsening(matrix: list[list[float]], multiplier: float) -> list[list[float]]:
    """Scale the probability of moving to a worse state, renormalising each row.

    "Worse" means later in the declared state order, which every delinquency
    ladder already follows. Probability added to the worsening cells is taken
    proportionally from the rest of the row, so each row still sums to 1 and the
    result is still a matrix the loader will accept.
    """
    array = np.asarray(matrix, dtype=float).copy()
    n = array.shape[0]
    for i in range(n):
        worse = np.zeros(n, dtype=bool)
        worse[i + 1 :] = True
        before = array[i, worse].sum()
        if before <= 0:
            continue
        after = float(np.clip(before * multiplier, 0.0, 0.999999))
        array[i, worse] *= after / before
        rest = ~worse
        rest_before = array[i, rest].sum()
        if rest_before > 0:
            array[i, rest] *= (1.0 - after) / rest_before
        array[i] /= array[i].sum()
    return [[float(v) for v in row] for row in array]


def set_default_rate(spec: DesignSpec, target: float) -> tuple[DesignSpec, dict[str, float]]:
    """Rescale the matrix until it produces ``target`` defaults in the first year.

    Solved by bisection on the stress multiplier rather than algebraically: the
    relationship between a multiplier and a cumulative twelve-step default rate
    runs through a matrix power, and a solver is both shorter and harder to get
    subtly wrong than an inversion of it.
    """
    out = spec.model_copy(deep=True)
    lc = out.lifecycle
    if lc is None or lc.transitions is None:
        raise ValueError(
            "this spec has no transition matrix, so there is no default rate to set. "
            "Only panels with a lifecycle can be aged."
        )
    if not 0.0 <= target < 1.0:
        raise ValueError(f"a default rate must be between 0 and 1, got {target}")

    base = list(lc.transitions)
    current = implied_default_rate(out)
    if current is None:
        raise ValueError(
            "this spec declares no absorbing or write-off state, so nothing in it counts "
            "as a default"
        )

    if target <= 0:
        # Remove the worsening flow entirely rather than chasing zero.
        lc.transitions = scale_worsening(base, 0.0)
        return out, {"requested": target, "achieved": implied_default_rate(out) or 0.0}

    low, high = 0.0, 1.0
    # Grow the bracket until the achievable rate straddles the target.
    for _ in range(20):
        lc.transitions = scale_worsening(base, high)
        if (implied_default_rate(out) or 0.0) >= target:
            break
        high *= 2.0
        if high > 1e6:
            break

    for _ in range(MAX_ITERATIONS):
        mid = (low + high) / 2.0
        lc.transitions = scale_worsening(base, mid)
        achieved = implied_default_rate(out) or 0.0
        if abs(achieved - target) < RATE_TOLERANCE:
            break
        if achieved < target:
            low = mid
        else:
            high = mid

    return out, {
        "requested": target,
        "achieved": implied_default_rate(out) or 0.0,
        "multiplier": round((low + high) / 2.0, 6),
    }


def set_prepayment_rate(spec: DesignSpec, target: float) -> tuple[DesignSpec, dict[str, float]]:
    """Set the annual rate of the hazard that redeems entities early."""
    out = spec.model_copy(deep=True)
    hazard = prepayment_hazard(out)
    if hazard is None:
        raise ValueError(
            "this spec has no prepayment hazard — no Bernoulli hazard leads to a terminal "
            "state that is not a write-off, so there is nothing to set"
        )
    hazard.annual_rate = float(np.clip(target, 0.0, 0.99))
    hazard.period_rate = None
    return out, {"requested": target, "achieved": hazard.annual_rate}


def set_recovery_rate(spec: DesignSpec, target: float) -> tuple[DesignSpec, dict[str, float]]:
    """Book ``target`` of the balance back when an entity writes off.

    Creates the recovery column if the spec does not already have one, because a
    number nothing records is not a setting.
    """
    from sdd.spec.schema import Column, ConstantGen

    out = spec.model_copy(deep=True)
    rate = float(np.clip(target, 0.0, 1.0))

    if out.dynamics.recovery is not None:
        out.dynamics.recovery.rate = rate
        return out, {"requested": target, "achieved": rate}

    balance = _balance_column(out)
    if balance is None:
        raise ValueError(
            "recovery is a share of a balance, and this spec has no balance column "
            "(none is named in dynamics.amortisation and none is called *balance*)"
        )
    lc = out.lifecycle
    if lc is None:
        raise ValueError("recovery needs a lifecycle: it is booked when an entity writes off")

    on_states = [s for s in default_states(out) if s in lc.terminal] or list(lc.absorbing)
    if not on_states:
        raise ValueError(
            "this spec has no write-off state, so there is no point at which a recovery "
            "would be booked"
        )

    target_column = "recovery_amount"
    if out.column(target_column) is None:
        out.columns.append(
            Column(
                name=target_column,
                role="dynamic",
                dtype="float",
                generator=ConstantGen(value=0.0),
                description="Amount recovered when the entity writes off. Zero until then.",
                confidence=1.0,
            )
        )
        if out.emit.column_order and target_column not in out.emit.column_order:
            out.emit.column_order.append(target_column)

    out.dynamics.recovery = Recovery(
        rate=rate, balance=balance, target=target_column, on_states=on_states
    )
    return out, {"requested": target, "achieved": rate}


def _balance_column(spec: DesignSpec) -> str | None:
    if spec.dynamics.amortisation:
        return spec.dynamics.amortisation.balance
    for column in spec.columns:
        if "balance" in column.name.lower() and column.dtype in ("int", "float"):
            return column.name
    return None


def apply_rates(
    spec: DesignSpec,
    *,
    default_rate: float | None = None,
    prepayment_rate: float | None = None,
    recovery_rate: float | None = None,
) -> tuple[DesignSpec, list[str]]:
    """Apply whichever rates were given, reporting what each one achieved.

    A rate that cannot be applied is reported and skipped rather than raised: the
    configure form offers all three, and a spec without a write-off state should
    lose the recovery box, not the whole form.
    """
    out = spec
    notes: list[str] = []
    for name, value, setter in (
        ("default rate", default_rate, set_default_rate),
        ("prepayment rate", prepayment_rate, set_prepayment_rate),
        ("recovery rate", recovery_rate, set_recovery_rate),
    ):
        if value is None:
            continue
        try:
            out, result = setter(out, float(value))
        except ValueError as exc:
            notes.append(f"{name} not applied: {exc}")
            continue
        achieved = result.get("achieved", value)
        if abs(achieved - float(value)) > 0.005:
            notes.append(
                f"{name} set as close as the matrix allows: asked for {value:.2%}, "
                f"got {achieved:.2%}."
            )
        else:
            notes.append(f"{name} set to {achieved:.2%}.")
    return out, notes
