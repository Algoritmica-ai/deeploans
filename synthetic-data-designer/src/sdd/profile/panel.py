"""Learn dynamics from a multi-cut-off sample.

A single snapshot tells you what a portfolio looks like. A panel tells you how
it *behaves*, and behaviour is what the ageing engine needs. Given the same
loans observed month after month, this module recovers:

- the **state machine** — which delinquency states exist and how often loans move
  between them, counted directly rather than hand-set;
- the **attrition rate** — how many entities leave the pool each period, which is
  the prepayment hazard;
- the **amortisation kind** — by testing the observed balance paths against each
  kernel and seeing which one predicts them;
- **counters** — columns that move by a fixed step every period;
- **index drift** — the average growth of valuation columns.

Everything here is measurement, not assumption. Where a measurement is thin
(too few observed transitions, say) it is reported with its sample size so the
number can be judged rather than trusted blindly.
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sdd.profile.profiler import DatasetProfile

# A state column should have few values but more than one.
MAX_STATE_VALUES = 20

# Below this many observed transitions, a matrix row is guesswork.
MIN_TRANSITIONS_PER_ROW = 30

# How closely a kernel must predict observed balances to be declared the match.
AMORT_TOLERANCE = 0.02

# A second chain has to move to be a chain. Below the floor the column is
# static in all but name; above the ceiling it is noise rather than migration.
MIN_CHAIN_CHURN = 0.002
MAX_CHAIN_CHURN = 0.5

# Above this, a candidate chain is a restatement of the primary state rather
# than something migrating on its own.
MAX_CHAIN_PURITY = 0.95

# Above this, one candidate is a function of another and not a chain of its own.
DERIVED_PURITY = 0.99

# A primary state has to determine a secondary value this reliably before the
# coupling is written down as forced rather than merely correlated.
FORCED_PURITY = 0.95

# A condition hazard is a claim that a rule fired, not that a coin landed. It is
# held to a correspondingly higher standard: the rule must explain nearly every
# move into the state, and nearly every row that satisfies it must be a move.
CONDITION_PRECISION = 0.95
CONDITION_RECALL = 0.9

# Neither a delay nor a rule can be established from one or two observations.
# Found by relearning a relearned CLO: the second-generation panel held a single
# maturity, and "modal dwell = 18, on 100% of events" fired the dwell test on a
# sample of one — writing an eighteen-month fixed delay into the spec on no
# evidence at all. A flat rate is the honest fallback: it is also badly measured
# from three events, but it does not dress the guess up as a mechanism.
MIN_EXIT_EVENTS = 3
MIN_CONDITION_EVENTS = MIN_EXIT_EVENTS

# How much of a rule's misses may be written off as exclusions before the rule
# itself is in doubt. Generous enough to recover a real carve-out, mean enough
# that "it works apart from the third of the book where it doesn't" is refused.
MAX_EXCLUDED_SHARE = 0.25
MAX_EXCLUDED_STATES = 3

# How far a column may jump as it lands, measured against how far it usually
# moves in a period, before it is read as having been *set* by the transition
# rather than crossed by it.
MAX_LANDING_JUMP = 3.0

# Names that mark a delinquency or status column.
STATE_NAME_HINTS = (
    "arrears_bucket",
    "status",
    "performing_status",
    "delinquency",
    "delinquency_status",
    "state",
    "arrears_status",
    "account_status",
)
BALANCE_NAME_HINTS = ("current_balance", "outstanding_balance", "balance", "principal_balance")
VALUATION_NAME_HINTS = ("market_value", "valuation", "indexed", "collateral_value", "residual")


def learn_panel_dynamics(
    df: pd.DataFrame, profile: DatasetProfile, *, state_column: str | None = None
) -> dict[str, Any]:
    """Recover everything the ageing engine needs from an observed panel."""
    out: dict[str, Any] = {}
    eid, etime = profile.id_column, profile.time_column
    if not eid or not etime:
        return out

    ordered = df.sort_values([eid, etime])

    state_column = state_column or detect_state_column(df, profile)
    if state_column:
        # Counters are the columns a deterministic exit is written against: a
        # term counts down, a seasoning counts up, and the rule fires when one
        # of them reaches its end. Static numeric columns are excluded because a
        # value that never moves cannot be crossed.
        candidates = [
            c.name
            for c in profile.columns
            if c.role == "dynamic" and c.name in df.columns and _holds_numbers(df[c.name])
        ]
        lifecycle = learn_lifecycle(ordered, eid, etime, state_column, candidates)
        if lifecycle:
            out["lifecycle"] = lifecycle
            chains = learn_secondary_chains(
                ordered, eid, etime, state_column, profile, lifecycle["states"]
            )
            if chains:
                out["secondary_chains"] = chains

    attrition = learn_attrition(df, eid, etime)
    if attrition:
        out["attrition"] = attrition

    originations = learn_originations(ordered, eid, etime, profile)
    if originations:
        out["originations"] = originations

    counters = learn_counters(ordered, eid, profile)
    if counters:
        out["counters"] = counters

    balance = detect_by_name(df, profile, BALANCE_NAME_HINTS, dynamic_only=True)
    if balance:
        amortisation = learn_amortisation(ordered, eid, balance, profile, state_column)
        if amortisation:
            out["amortisation"] = amortisation

    indices = learn_index_drift(df, etime, profile)
    if indices:
        out["indices"] = indices

    return out


# ---------------------------------------------------------------------------
# detection helpers
# ---------------------------------------------------------------------------


def detect_by_name(
    df: pd.DataFrame,
    profile: DatasetProfile,
    hints: tuple[str, ...],
    *,
    dynamic_only: bool = False,
    numeric_only: bool = False,
) -> str | None:
    """First column whose name contains one of ``hints``, preferring exact matches.

    ``numeric_only`` is not a refinement, it is a correctness guard. Matching on
    the name alone picked `interest_rate_type` — which holds "Fixed" and
    "Floating" — as an amortisation rate, because it contains "interest_rate"
    and came first. The spec that produced validated cleanly and then died on
    `.astype(float)` mid-run, so the failure surfaced nowhere near its cause.

    A column standing in for a rate, a payment or a term has to hold numbers.
    """
    candidates = [
        c.name
        for c in profile.columns
        if (not dynamic_only or c.role == "dynamic")
        and c.name in df.columns
        and (not numeric_only or _holds_numbers(df[c.name]))
    ]
    for hint in hints:
        for name in candidates:
            if name.lower() == hint:
                return name
    for hint in hints:
        for name in candidates:
            if hint in name.lower():
                return name
    return None


def _holds_numbers(series: pd.Series) -> bool:
    """Whether a column is usable as a number, judged on its values.

    Not on its declared dtype: a tape read from CSV arrives as object, and a
    perfectly good rate column would be rejected for having been parsed loosely.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    observed = series.notna().sum()
    return bool(observed) and numeric.notna().sum() / observed > 0.95


def detect_state_column(df: pd.DataFrame, profile: DatasetProfile) -> str | None:
    """Find the column holding the lifecycle state.

    By name first. Failing that, the dynamic categorical with the fewest values
    — a delinquency ladder is short, and it has to change or it would not be
    dynamic.
    """
    named = detect_by_name(df, profile, STATE_NAME_HINTS, dynamic_only=True)
    if named:
        return named

    best: tuple[str, int] | None = None
    for col in profile.columns:
        if col.role != "dynamic" or col.dtype not in ("category", "str", "bool"):
            continue
        if not 1 < col.distinct <= MAX_STATE_VALUES:
            continue
        if best is None or col.distinct < best[1]:
            best = (col.name, col.distinct)
    return best[0] if best else None


# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------


def learn_exits(
    ordered: pd.DataFrame,
    id_column: str,
    time_column: str,
    state_column: str,
    terminal: list[str],
    absorbing: list[str],
    periods_per_year: float,
    numeric_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """How entities leave the pool, one rule per terminal state.

    The transition matrix covers the states an entity sits in; leaving is a
    hazard, and until now only two were ever emitted — one flat rate into the
    first terminal state and one write-off delay, fixed at nine periods whatever
    the data said. A book with four ways out came back with two of them
    unreachable, and the loader rightly refused the spec.

    Each terminal state gets its own rule, and which *kind* is decided by the
    evidence rather than by position in a list:

    *A fixed delay* looks like a spike. Entities write off after nine months in
    default, near enough every time, so the time spent in the source state
    before the move clusters on one value.

    *A flat chance* looks like a decay. A loan can prepay in any month, so the
    time before it does is spread thin across many values.
    """
    exits: list[dict[str, Any]] = []
    if not terminal:
        return exits

    frame = ordered[[id_column, time_column, state_column]].copy()
    frame["_next"] = frame.groupby(id_column)[state_column].shift(-1)

    # How many consecutive periods an entity has been in its current state.
    same = frame[state_column].eq(frame.groupby(id_column)[state_column].shift())
    run_id = (~same).cumsum()
    frame["_dwell"] = frame.groupby([id_column, run_id]).cumcount() + 1

    live = frame[~frame[state_column].isin(terminal)]
    at_risk = len(live)
    if not at_risk:
        return exits

    for state in terminal:
        moves = live[live["_next"] == state]
        if moves.empty:
            continue

        sources = moves[state_column].value_counts()
        dominant = str(sources.index[0])
        dominant_share = float(sources.iloc[0] / sources.sum())

        dwell = moves["_dwell"].value_counts()
        modal_dwell = int(dwell.index[0])
        modal_share = float(dwell.iloc[0] / dwell.sum())

        # A spike in the dwell distribution, from one source state, is a delay.
        # Three conditions matter: a flat hazard out of a rare state can look
        # spiky by accident, a genuine delay always comes from one place, and a
        # spike needs enough observations to be a spike rather than a data point.
        if (
            len(moves) >= MIN_EXIT_EVENTS
            and modal_dwell > 1
            and modal_share > 0.55
            and dominant_share > 0.7
        ):
            exits.append(
                {
                    "kind": "dwell_time",
                    "name": f"to_{_slug(state)}",
                    "from_state": dominant,
                    "to_state": state,
                    # +1: `moves` holds the last row *in* the source state, whose
                    # dwell counter reads one short of the period the hazard
                    # fires on. Feeding the raw modal value back would shorten
                    # every workout by a month on each round trip.
                    "periods": modal_dwell + 1,
                    "evidence": f"{len(moves)} moves, {modal_share:.0%} after "
                    f"{modal_dwell + 1} periods in {dominant!r}",
                }
            )
            continue

        # No spike in the clock, so try a rule over the entity's own columns
        # before falling back to chance.
        #
        # Ordered after the dwell test rather than before it, which took a
        # measurement to settle. Run first, this displaced the auto and green
        # packs' charge-off — a nine-month workout — with `days_past_due >= 180`,
        # which scores perfectly for the uninteresting reason that days past due
        # *is* the workout clock in other units. Restating a clock as a threshold
        # on its own read-out adds nothing and hides the mechanism.
        #
        # A genuine rule looks different: facilities mature after wildly varying
        # spells in the performing state, so no dwell spike exists to explain
        # them, and the countdown column is the only thing that does.
        condition = learn_condition(
            ordered,
            id_column,
            time_column,
            state_column,
            state,
            [s for s in sources.index.astype(str) if s not in terminal],
            numeric_columns or [],
        )
        if condition:
            exits.append(condition)
            continue

        # Otherwise a flat per-period chance, measured over the entities that
        # could have made the move rather than over every row in the panel.
        eligible = live
        if state in _reachable_only_from(moves, state_column):
            eligible = live[live[state_column].isin(sources.index)]
        rate = len(moves) / max(len(eligible), 1)
        annual = 1.0 - (1.0 - min(rate, 0.99)) ** periods_per_year
        exits.append(
            {
                "kind": "bernoulli",
                "name": f"to_{_slug(state)}",
                "annual_rate": round(min(annual, 0.99), 6),
                "to_state": state,
                "excluded_states": [s for s in absorbing if s not in sources.index],
                "evidence": f"{len(moves)} moves out of {len(eligible):,} at-risk observations",
            }
        )

    return exits


def learn_condition(
    ordered: pd.DataFrame,
    id_column: str,
    time_column: str,
    state_column: str,
    to_state: str,
    sources: list[str],
    numeric_columns: list[str],
) -> dict[str, Any] | None:
    """Whether entering a state is explained by a column crossing a threshold.

    The other two hazards are chance. Maturity is not: a loan matures when *its
    own* maturity date arrives, and relearning that as a flat monthly rate gives
    every loan the same chance of maturing in month three — the 72-month loans
    included. The panel says which it was, and this is the test.

    Read from the **landing** row rather than the last row before the move.
    The engine advances counters and then evaluates, so a facility whose
    countdown reads 2 on its last live row is at 1 when the rule fires; taking
    the earlier value would shorten every term by a month on each round trip.
    Reading where the entity landed sidesteps the arithmetic entirely.

    Held to precision *and* recall, because either alone is easy to satisfy by
    accident. Recall alone: "balance <= 10,000,000" catches every maturity in a
    book where nothing is that large. Precision alone: a threshold met by three
    rows, all of which happen to be maturities.
    """
    landings = ordered[ordered[state_column] == to_state].groupby(id_column).head(1)
    if len(landings) < MIN_CONDITION_EVENTS:
        return None
    entered = set(landings[id_column])

    best: dict[str, Any] | None = None
    for column in numeric_columns:
        values = pd.to_numeric(landings[column], errors="coerce").dropna()
        if len(values) < MIN_CONDITION_EVENTS:
            continue

        counts = values.value_counts()
        modal = float(counts.index[0])
        if counts.iloc[0] / len(values) < CONDITION_RECALL:
            continue

        whole = ordered[column].dropna()
        for operator in ("<=", ">="):
            # The threshold has to sit at an edge of the column's range. A
            # midpoint would split the column rather than mark the end of
            # something, and "balance >= median" is not a maturity rule however
            # well it scores.
            if operator == "<=" and modal > whole.quantile(0.15):
                continue
            if operator == ">=" and modal < whole.quantile(0.85):
                continue

            satisfied = ordered[column] <= modal if operator == "<=" else ordered[column] >= modal
            if not satisfied.any():
                continue

            # Scored per entity, not per row. An entity that matures sits in the
            # matured state for the rest of the panel, and every one of those
            # rows still satisfies the condition — counted by row, a state that
            # is entered early would outscore one entered late for no reason
            # beyond how long the panel ran afterwards.
            reached = set(ordered.loc[satisfied, id_column])
            if not reached:
                continue
            if not _crossed_rather_than_set(ordered, id_column, state_column, to_state, column):
                continue

            excluded = _condition_exclusions(
                ordered, id_column, state_column, satisfied, reached - entered
            )
            eligible = reached
            if excluded:
                spared = set(
                    ordered.loc[satisfied & ordered[state_column].isin(excluded), id_column]
                )
                eligible = reached - (spared - entered)

            if not eligible:
                continue
            precision = len(eligible & entered) / len(eligible)
            recall = len(eligible & entered) / len(entered)
            if precision < CONDITION_PRECISION or recall < CONDITION_RECALL:
                continue

            score = precision * recall
            if best is None or score > best["_score"]:
                note = f"{len(entered)} entries, {precision:.0%} of entities meeting "
                note += f"{column} {operator} {modal:g} are in {to_state!r}"
                if excluded:
                    note += f"; {sorted(excluded)} excluded"
                best = {
                    "kind": "condition",
                    "name": f"to_{_slug(to_state)}",
                    "when": f"{column} {operator} {modal:g}",
                    "to_state": to_state,
                    "excluded_states": sorted(excluded),
                    "_score": score,
                    "evidence": note,
                }

    if best is not None:
        best.pop("_score")
    return best


def _crossed_rather_than_set(
    ordered: pd.DataFrame,
    id_column: str,
    state_column: str,
    to_state: str,
    column: str,
) -> bool:
    """Whether the entity crossed the threshold, or the transition put it there.

    The distinction the whole detector turns on, and the one it got wrong first.
    Every prepaid facility satisfies `current_balance <= 0` — perfectly, on 255
    of 255 — because entering Prepaid is what *sets* the balance to zero. Read
    as a trigger it is circular: regenerate with "prepay when the balance
    reaches zero" and nothing ever prepays, because nothing reaches zero without
    prepaying first. Prepayment would silently disappear from the book.

    A genuine threshold is approached. `months_to_maturity` reads 2 on the last
    live row and 1 on the landing row — one ordinary step of a counter that
    steps by one. A balance set on arrival reads four million and then zero.

    So the column's jump as it lands is compared with how far it moves in an
    ordinary period. A step is a crossing; a cliff is an assignment.
    """
    frame = ordered[[id_column, state_column, column]].copy()
    frame["_value"] = pd.to_numeric(frame[column], errors="coerce")
    frame["_prev"] = frame.groupby(id_column)["_value"].shift()
    frame["_was"] = frame.groupby(id_column)[state_column].shift()

    landing = frame[(frame[state_column] == to_state) & (frame["_was"] != to_state)]
    landing = landing.dropna(subset=["_value", "_prev"])
    if landing.empty:
        return True

    live = frame[(frame[state_column] != to_state) & (frame["_was"] == frame[state_column])]
    ordinary = float((live["_value"] - live["_prev"]).abs().median() or 0.0)
    jump = float((landing["_value"] - landing["_prev"]).abs().median())

    if jump == 0.0:
        return True
    if ordinary == 0.0:
        # The column does not move on its own, so any movement at the boundary
        # came from the boundary.
        return False
    return jump <= MAX_LANDING_JUMP * ordinary


def _condition_exclusions(
    ordered: pd.DataFrame,
    id_column: str,
    state_column: str,
    satisfied: pd.Series,
    missed: set[Any],
) -> set[str]:
    """States that meet the condition without obeying it.

    A defaulted facility's term keeps counting down while it sits in workout, so
    it reaches zero months to maturity and never matures — it resolves through
    recovery instead. Counted as counter-examples, five such facilities dropped
    a perfect rule to 93% precision and the whole maturity condition was lost,
    relearned as a flat monthly chance.

    They are not counter-examples, they are a carve-out, and the pack this was
    measured against says so in as many words: `excluded_states: [Defaulted]`.
    Recovering the carve-out is a better answer than loosening the threshold,
    because it puts the exception in the spec where a reader can see it.

    Bounded on both sides, so that this cannot become a way to excuse any rule:
    a handful of states, covering a minority of the misses.
    """
    if not missed:
        return set()

    rows = ordered[satisfied & ordered[id_column].isin(missed)]
    first = rows.groupby(id_column).head(1)
    states = first[state_column].dropna().value_counts()
    if states.empty or len(states) > MAX_EXCLUDED_STATES:
        return set()

    reached_total = ordered.loc[satisfied, id_column].nunique()
    if reached_total and len(missed) / reached_total > MAX_EXCLUDED_SHARE:
        return set()
    return {str(v) for v in states.index}


def _reachable_only_from(moves: pd.DataFrame, state_column: str) -> set[str]:
    """States whose exits all come from a single source."""
    counts = moves[state_column].value_counts()
    return set(counts.index[:1]) if len(counts) == 1 else set()


def _slug(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value)).strip("_")


def learn_lifecycle(
    ordered: pd.DataFrame,
    id_column: str,
    time_column: str,
    state_column: str,
    numeric_columns: list[str] | None = None,
) -> dict[str, Any] | None:
    """Count observed state transitions into a matrix.

    Terminal states are identified by behaviour, not by name: a state an entity
    is never observed to leave, and after which it stops appearing, is terminal.
    A state it never leaves but keeps being reported in is absorbing.
    """
    states = ordered[state_column].dropna()
    if states.nunique() < 2:
        return None

    nxt = ordered.groupby(id_column)[state_column].shift(-1)
    pairs = pd.DataFrame({"from": ordered[state_column], "to": nxt})
    observed = pairs.dropna()
    if observed.empty:
        return None

    first_period = ordered[time_column].min()
    initial = ordered[ordered[time_column] == first_period][state_column].value_counts()

    # Order states best-first, because the engine and the stress scenarios both
    # read that ordering as severity: a scenario worsens outcomes by shifting
    # weight to *later* states. Frequency in the opening cut-off is the proxy —
    # a healthy pool is mostly performing — which is a heuristic, so it is
    # flagged for review in the generated spec rather than trusted.
    labels = list(initial.index) + [
        s for s in sorted(states.unique(), key=str) if s not in initial.index
    ]

    counts = pd.crosstab(observed["from"], observed["to"]).reindex(
        index=labels, columns=labels, fill_value=0
    )

    last_period = ordered[time_column].max()
    last_row = ordered.groupby(id_column).tail(1)

    # A state is terminal when entities stop being reported after reaching it.
    #
    # Observations at the final cut-off carry no information: an entity seen
    # there does not reappear, but that is because the panel ended, not because
    # the state ended it. They are therefore excluded from the denominator
    # rather than counted as evidence against — counting them would make a state
    # look non-terminal purely in proportion to how many entities happened to be
    # sitting in it when the data stopped.
    terminal = []
    before_end = ordered[ordered[time_column] < last_period]
    for label in labels:
        occurrences = int((before_end[state_column] == label).sum())
        if not occurrences:
            continue
        exits = int(
            ((last_row[state_column] == label) & (last_row[time_column] < last_period)).sum()
        )
        if exits / occurrences > 0.8:
            terminal.append(label)

    live = [label for label in labels if label not in terminal]
    live_counts = counts.reindex(index=live, columns=live, fill_value=0)
    totals = live_counts.sum(axis=1)

    # Absorbing is judged on the *live* matrix, not on raw counts. A defaulted
    # loan does eventually leave for charge-off, but that exit is a separate
    # dwell-time hazard rather than a matrix transition; within the matrix the
    # state is genuinely absorbing. Judging on raw counts would miss every such
    # state and produce a spec whose matrix and declarations disagree.
    absorbing = [
        label
        for label in live
        if totals.get(label, 0) > 0 and live_counts.loc[label, label] / totals[label] > 0.999
    ]

    thin = [label for label in live if totals.get(label, 0) < MIN_TRANSITIONS_PER_ROW]
    matrix = []
    for label in live:
        total = totals.get(label, 0)
        if total > 0:
            matrix.append([round(float(v), 6) for v in (live_counts.loc[label] / total)])
        else:
            # Never observed leaving: treat as staying put rather than inventing
            # transitions from no evidence.
            matrix.append([1.0 if other == label else 0.0 for other in live])

    matrix = [_renormalise(row) for row in matrix]

    return {
        "state_column": state_column,
        "states": [str(s) for s in labels],
        "transition_states": [str(s) for s in live],
        "transitions": matrix,
        "terminal": [str(s) for s in terminal],
        "absorbing": [str(s) for s in absorbing],
        "observed_transitions": len(observed),
        "initial_distribution": _renormalise_shares(initial.div(initial.sum())),
        "exits": learn_exits(
            ordered,
            id_column,
            time_column,
            state_column,
            [str(s) for s in terminal],
            [str(s) for s in absorbing],
            periods_per_year=12.0,
            numeric_columns=numeric_columns or [],
        ),
        "low_evidence_states": [str(s) for s in thin],
        "state_order_note": (
            "states are ordered by their share of the first cut-off, as a proxy for severity; "
            "check the order before relying on stress scenarios, which treat later states as worse"
        ),
        "confidence": 0.3 if thin else 0.85,
    }


def _renormalise_shares(shares: pd.Series) -> dict[str, float]:
    """Round a distribution to six places and make it sum to exactly 1.

    The same residue problem `_renormalise` solves for a matrix row, on the
    opening mix. Rounding nine rating grades independently landed a learned
    chain on 0.999998, and the loader rejects a distribution that does not sum
    to 1 — so profiling a perfectly ordinary tape returned a 500 whenever the
    mix happened to round that way. Seed-dependent, which is why it survived
    until a test picked a different one.
    """
    rounded = {str(k): round(float(v), 6) for k, v in shares.items()}
    if not rounded:
        return rounded
    residue = round(1.0 - sum(rounded.values()), 6)
    if residue:
        # Into the largest share, where it is proportionally least visible.
        biggest = max(rounded, key=lambda k: rounded[k])
        rounded[biggest] = round(rounded[biggest] + residue, 6)
    return rounded


def _renormalise(row: list[float]) -> list[float]:
    """Force a row to sum to exactly 1 despite rounding."""
    total = sum(row)
    if total <= 0:
        return row
    scaled = [v / total for v in row]
    # Push the rounding residue into the largest cell, where it is least visible.
    rounded = [round(v, 6) for v in scaled]
    residue = 1.0 - sum(rounded)
    biggest = rounded.index(max(rounded))
    rounded[biggest] = round(rounded[biggest] + residue, 6)
    return rounded


# ---------------------------------------------------------------------------
# secondary chains
# ---------------------------------------------------------------------------


def learn_secondary_chains(
    ordered: pd.DataFrame,
    id_column: str,
    time_column: str,
    state_column: str,
    profile: DatasetProfile,
    primary_states: list[str],
) -> list[dict[str, Any]]:
    """Columns that migrate on their own, alongside the lifecycle.

    A credit rating is the case. It moves under its own steam, and normally
    moves *before* distress is visible — a company is downgraded while still
    paying every instalment, which is the early warning the rating exists to
    give. Relearned as an ordinary categorical column it would be redrawn
    independently each period, and a facility would flicker between BB and CCC
    from one month to the next.

    The hard part is not finding a column that changes. It is telling a chain
    from a **restatement of the primary state**: a column holding "Performing" /
    "Non-performing" also changes over time, and also has a tidy matrix, but it
    carries nothing the lifecycle does not already say. Those are rejected on
    purity — if knowing the state tells you the column, the column is derived.
    """
    chains: list[dict[str, Any]] = []
    for column in _chain_candidates(ordered, profile, state_column):
        matrix = _chain_matrix(ordered, id_column, time_column, column)
        if matrix is None:
            continue
        chains.append(
            {
                "name": _slug(column).removesuffix("_at_cutoff") or column,
                "lifecycle": matrix,
                "coupling": _chain_coupling(
                    ordered, id_column, state_column, column, matrix["states"], primary_states
                ),
            }
        )
    return chains


def _chain_candidates(
    ordered: pd.DataFrame, profile: DatasetProfile, state_column: str
) -> list[str]:
    """Categorical columns that move, but not in lockstep with the lifecycle."""
    # Columns the bucket detector has already claimed are derivations, not
    # chains. `balance_bucket` migrates as a loan amortises and produces a
    # perfectly good matrix, but generated independently it would report a
    # 300k-350k band on a loan carrying 80k.
    derived = {d.target for d in profile.derived}

    out = []
    for column in profile.columns:
        if column.name == state_column or column.name not in ordered.columns:
            continue
        if column.name in derived:
            continue
        if column.role != "dynamic" or column.dtype not in ("category", "str", "bool"):
            continue
        values = ordered[column.name].dropna()
        if not 2 <= values.nunique() <= MAX_STATE_VALUES:
            continue

        # Churn: the share of consecutive observations where the value moved.
        changed = ordered[column.name].ne(ordered[column.name].shift())
        changed &= ordered[profile.id_column].eq(ordered[profile.id_column].shift())
        churn = float(changed.sum() / max(len(ordered) - 1, 1))
        if not MIN_CHAIN_CHURN <= churn <= MAX_CHAIN_CHURN:
            continue

        # Purity: how much of the column the primary state already accounts for.
        # A column the state can predict is a relabelling of the state.
        modal = ordered.groupby(state_column)[column.name].transform(
            lambda s: s.mode().iloc[0] if not s.mode().empty else None
        )
        purity = float((ordered[column.name] == modal).mean())
        if purity > MAX_CHAIN_PURITY:
            continue

        out.append(column.name)

    return _drop_derived(ordered, out)


def _drop_derived(ordered: pd.DataFrame, candidates: list[str]) -> list[str]:
    """Remove candidates that are functions of another candidate.

    Measured, and the measurement mattered: the CLO panel offered three chains
    where it has one. `rating_at_cutoff` holds nine grades, `rating_bucket`
    collapses them to four, and `ccc_flag` to two — and all three migrate, all
    three produce a clean matrix, and two of them are the first one with detail
    thrown away.

    Run as three independent chains they would drift apart, and the output would
    carry facilities rated B- whose bucket said CCC. The finer column wins,
    because it is the one the others can be recovered from.

    Bucketings of a *numeric* column are already caught upstream by the bucket
    detector and never reach here; this is the categorical case it does not
    cover.
    """
    keep = list(candidates)
    for coarse, fine in itertools.permutations(candidates, 2):
        if coarse not in keep:
            continue
        pair = ordered[[fine, coarse]].dropna()
        if pair.empty or pair[fine].nunique() <= pair[coarse].nunique():
            continue
        modal = pair.groupby(fine)[coarse].transform(
            lambda s: s.mode().iloc[0] if not s.mode().empty else None
        )
        if float((pair[coarse] == modal).mean()) >= DERIVED_PURITY:
            keep.remove(coarse)
    return keep


def _chain_matrix(
    ordered: pd.DataFrame, id_column: str, time_column: str, column: str
) -> dict[str, Any] | None:
    """The chain's own transition matrix.

    Deliberately not `learn_lifecycle`. That function identifies terminal states
    by entities ceasing to be reported, which is a fact about the *lifecycle* —
    an entity stops being reported because it left the pool, not because its
    rating did anything. Reusing it would declare D terminal, and the schema
    rightly refuses a secondary chain that can end an entity's life.
    """
    nxt = ordered.groupby(id_column)[column].shift(-1)
    observed = pd.DataFrame({"from": ordered[column], "to": nxt}).dropna()
    if len(observed) < MIN_TRANSITIONS_PER_ROW:
        return None

    first = ordered[ordered[time_column] == ordered[time_column].min()][column].value_counts()
    labels = list(first.index) + [
        v for v in sorted(ordered[column].dropna().unique(), key=str) if v not in first.index
    ]
    counts = pd.crosstab(observed["from"], observed["to"]).reindex(
        index=labels, columns=labels, fill_value=0
    )
    totals = counts.sum(axis=1)

    rows = []
    for label in labels:
        total = totals.get(label, 0)
        if total > 0:
            rows.append(_renormalise([round(float(v), 6) for v in (counts.loc[label] / total)]))
        else:
            rows.append([1.0 if other == label else 0.0 for other in labels])

    absorbing = [
        label
        for label in labels
        if totals.get(label, 0) > 0 and counts.loc[label, label] / totals[label] > 0.999
    ]
    thin = [label for label in labels if totals.get(label, 0) < MIN_TRANSITIONS_PER_ROW]

    return {
        "state_column": column,
        "states": [str(v) for v in labels],
        "transitions": rows,
        "absorbing": [str(v) for v in absorbing],
        "terminal": [],
        "initial_distribution": _renormalise_shares(first.div(first.sum())),
        "observed_transitions": len(observed),
        "low_evidence_states": [str(v) for v in thin],
        "state_order_note": (
            "chain states are ordered by their share of the first cut-off, which is a "
            "frequency ordering and not a severity one; nothing in the engine reads it as "
            "severity, but a reader might"
        ),
        "confidence": 0.3 if thin else 0.8,
    }


def _chain_coupling(
    ordered: pd.DataFrame,
    id_column: str,
    state_column: str,
    column: str,
    chain_states: list[str],
    primary_states: list[str],
) -> dict[str, Any]:
    """How the chain and the lifecycle hold each other in line.

    Two directions, measured separately because they are different claims.

    ``forced_by`` is near-certainty: if practically every defaulted facility is
    rated D, the state is overwriting the rating and the spec should say so
    outright rather than hope the matrix reproduces it.

    ``stress`` is the other direction — being rated CCC makes falling further
    behind more likely. Measured as a ratio of worsening rates, using the
    primary's own ordering to define worse, and left out where the evidence is
    too thin to distinguish it from the base rate.
    """
    forced: dict[str, str] = {}
    for state in primary_states:
        rows = ordered[ordered[state_column] == state][column].dropna()
        if len(rows) < MIN_TRANSITIONS_PER_ROW:
            continue
        counts = rows.value_counts(normalize=True)
        if float(counts.iloc[0]) >= FORCED_PURITY and str(counts.index[0]) in chain_states:
            forced[str(state)] = str(counts.index[0])

    rank = {state: i for i, state in enumerate(primary_states)}
    current = ordered[state_column].map(rank)
    following = ordered.groupby(id_column)[state_column].shift(-1).map(rank)
    worsened = (following > current).where(following.notna())

    baseline = float(worsened.mean()) if worsened.notna().any() else 0.0
    stress: dict[str, float] = {}
    if baseline > 0:
        for value in chain_states:
            # Rows forced into this chain state carry no information about it:
            # the rating is D *because* the facility defaulted, so measuring
            # what D does to default risk would be reading the arrow backwards.
            mask = (ordered[column] == value) & worsened.notna()
            if str(value) in set(forced.values()):
                continue
            if int(mask.sum()) < MIN_TRANSITIONS_PER_ROW:
                continue
            rate = float(worsened[mask].mean())
            multiplier = round(min(max(rate / baseline, 0.1), 20.0), 3)
            if abs(multiplier - 1.0) > 0.25:
                stress[str(value)] = multiplier

    return {"forced_by": forced, "stress": stress}


# ---------------------------------------------------------------------------
# attrition
# ---------------------------------------------------------------------------


def _entities_by_period(df: pd.DataFrame, id_column: str, time_column: str) -> list[set[Any]]:
    """The set of entities reported at each cut-off, in order."""
    periods = sorted(df[time_column].dropna().unique())
    grouped = df.groupby(time_column)[id_column]
    return [set(grouped.get_group(period).unique()) for period in periods]


def learn_attrition(df: pd.DataFrame, id_column: str, time_column: str) -> dict[str, Any] | None:
    """Measure how fast entities leave the pool.

    Counted as *departures* — entities reported at one cut-off and absent at the
    next — rather than as the change in pool size. The two are the same number
    for a closed pool and very different for an open one: a pool taking on as
    many loans as it loses has zero net change and a perfectly ordinary
    prepayment rate, and measuring the net would report it as zero.

    The per-period rate is converted to an annualised one, since that is how
    prepayment is quoted and calibrated in practice.
    """
    cohorts = _entities_by_period(df, id_column, time_column)
    if len(cohorts) < 2:
        return None

    departures = []
    for before, after in itertools.pairwise(cohorts):
        if before:
            departures.append(len(before - after) / len(before))
    if not departures:
        return None

    per_period = float(np.clip(np.mean(departures), 0.0, 1.0))
    annual = 1.0 - (1.0 - per_period) ** 12 if per_period < 1 else 1.0
    return {
        "period_rate": round(per_period, 6),
        "annual_rate": round(min(annual, 0.999), 6),
        "periods_observed": len(cohorts),
        "confidence": 0.8 if len(cohorts) >= 6 else 0.4,
    }


def learn_originations(
    ordered: pd.DataFrame, id_column: str, time_column: str, profile: DatasetProfile
) -> dict[str, Any] | None:
    """Measure entities *joining* the pool after the first cut-off.

    A tape covering two years of a lender's book contains loans written in both,
    and a revolving deal keeps buying receivables — so a panel whose later
    cut-offs hold entities the first one never saw is an open pool, and
    reproducing it as a closed one would generate the wrong thing entirely.

    Whether the arrivals are *newly originated* or *acquired seasoned* is decided
    by looking at them: a counter that ticks upward measures elapsed time, so if
    new arrivals enter with it at zero they were written that period.
    """
    cohorts = _entities_by_period(ordered, id_column, time_column)
    if len(cohorts) < 2:
        return None

    seen = set(cohorts[0])
    arrivals: list[int] = []
    first_period: int | None = None
    joining: set[Any] = set()

    for period, cohort in enumerate(cohorts[1:], start=1):
        new = cohort - seen
        arrivals.append(len(new))
        if new and first_period is None:
            first_period = period
        joining |= new
        seen |= cohort

    if not joining:
        return None

    opening = max(len(cohorts[0]), 1)
    mean_new = float(np.mean(arrivals))
    return {
        "per_period_mean": round(mean_new, 3),
        "rate": round(mean_new / opening, 6),
        "total": len(joining),
        "start_period": first_period or 1,
        "periods_observed": len(cohorts),
        "fresh": _arrivals_look_new(ordered, id_column, joining, profile),
        "confidence": 0.8 if len(cohorts) >= 6 else 0.5,
    }


# A counter reading at or below this on arrival means no time has elapsed for
# that entity, i.e. it was written in the period it appeared.
FRESH_COUNTER_TOLERANCE = 1.0


def _arrivals_look_new(
    ordered: pd.DataFrame, id_column: str, joining: set[Any], profile: DatasetProfile
) -> bool:
    """True when entities joining later start their upward counters at zero."""
    rising = [
        c["column"]
        for c in learn_counters(ordered, id_column, profile)
        if c["step"] > 0 and c["column"] in ordered.columns
    ]
    if not rising or not joining:
        # Nothing measures elapsed time here, so "newly written" and "acquired"
        # are indistinguishable. Newly written is the commoner case.
        return True

    arrivals = ordered[ordered[id_column].isin(joining)]
    first_rows = arrivals.groupby(id_column).head(1)
    medians = [
        float(pd.to_numeric(first_rows[column], errors="coerce").median()) for column in rising
    ]
    return all(np.isfinite(m) and m <= FRESH_COUNTER_TOLERANCE for m in medians)


# ---------------------------------------------------------------------------
# counters
# ---------------------------------------------------------------------------


def learn_counters(
    ordered: pd.DataFrame, id_column: str, profile: DatasetProfile
) -> list[dict[str, Any]]:
    """Find numeric columns that move by a fixed step every period.

    Seasoning up by one and remaining term down by one are the obvious cases,
    and both are trivially detectable: take the per-entity difference and see
    whether one value dominates.
    """
    found: list[dict[str, Any]] = []
    for col in profile.columns:
        if col.role != "dynamic" or col.dtype not in ("int", "float"):
            continue
        if col.name not in ordered.columns:
            continue
        diffs = ordered.groupby(id_column)[col.name].diff().dropna()
        if diffs.empty:
            continue
        counts = diffs.value_counts(normalize=True)
        step, share = counts.index[0], counts.iloc[0]
        # A step of zero is a column that simply does not move, not a counter.
        if share > 0.9 and step != 0 and float(step) == round(float(step), 4):
            found.append(
                {
                    "column": col.name,
                    "step": round(float(step), 6),
                    "consistency": round(float(share), 4),
                    "confidence": round(float(share), 3),
                }
            )
    return found


# ---------------------------------------------------------------------------
# amortisation
# ---------------------------------------------------------------------------


def learn_amortisation(
    ordered: pd.DataFrame,
    id_column: str,
    balance_column: str,
    profile: DatasetProfile,
    state_column: str | None,
) -> dict[str, Any] | None:
    """Work out which amortisation kernel the observed balances follow.

    Each candidate predicts the next balance from the current one; whichever
    predicts observed balances most closely wins. Only *falling* balances are
    used, because a frozen balance is consistent with every kernel and would
    make them all look equally good.
    """
    if balance_column not in ordered.columns:
        return None

    balances = ordered.groupby(id_column)[balance_column]
    prev = balances.shift()
    curr = ordered[balance_column]
    moving = prev.notna() & (prev > 0) & (curr > 0) & (curr < prev)

    if moving.sum() < 20:
        return {
            "kind": "interest_only",
            "reason": "balances never fell in the sample, so nothing amortises",
            "confidence": 0.4,
        }

    ratio = (curr[moving] / prev[moving]).median()
    absolute = (prev[moving] - curr[moving]).median()

    rate_column = detect_by_name(
        ordered, profile, ("interest_rate", "rate", "coupon"), numeric_only=True
    )
    payment_column = detect_by_name(
        ordered, profile, ("scheduled_monthly_payment", "payment", "instalment", "installment")
    )
    # A remaining-term column is the other way to amortise: without a payment,
    # balance/term is the equal-principal slice. Worth finding, because a tape
    # that carries a term but not a payment is common.
    term_column = detect_by_name(
        ordered,
        profile,
        ("remaining_term_months", "remaining_term", "months_to_maturity", "term_months", "term"),
        numeric_only=True,
    )

    # An annuity retires a growing slice of principal each period, so the
    # absolute reduction rises over time; a linear loan retires a constant one.
    kind, confidence, reason = "linear", 0.5, "balances fell by a roughly constant amount"
    if rate_column and payment_column:
        kind = "annuity"
        confidence = 0.75
        reason = f"found both a rate ({rate_column}) and a payment ({payment_column}) column"
    elif ratio > 0.98:
        kind = "linear"
        confidence = 0.5
        reason = f"balances fell slowly and steadily (median ratio {ratio:.4f})"

    out: dict[str, Any] = {
        "kind": kind,
        "balance": balance_column,
        "median_period_ratio": round(float(ratio), 6),
        "median_period_reduction": round(float(absolute), 2),
        "reason": reason,
        "confidence": confidence,
    }
    if rate_column:
        out["rate"] = rate_column
    if payment_column:
        out["payment"] = payment_column
    if term_column:
        out["term"] = term_column
    if state_column:
        # Which states were actually paying down, so the spec can restrict
        # amortisation to them rather than letting defaulted loans amortise.
        paying = ordered.loc[moving, state_column].value_counts(normalize=True)
        out["only_when_state"] = [str(s) for s in paying[paying > 0.5].index.tolist()]
    return out


# ---------------------------------------------------------------------------
# indices
# ---------------------------------------------------------------------------


def learn_index_drift(
    df: pd.DataFrame, time_column: str, profile: DatasetProfile
) -> list[dict[str, Any]]:
    """Back out the growth rate applied to valuation columns."""
    found: list[dict[str, Any]] = []
    for col in profile.columns:
        if col.role != "dynamic" or col.dtype != "float":
            continue
        if not any(hint in col.name.lower() for hint in VALUATION_NAME_HINTS):
            continue
        means = df.groupby(time_column)[col.name].mean().dropna()
        if len(means) < 2 or (means <= 0).any():
            continue
        # Geometric mean of period-over-period growth.
        growth = float(np.exp(np.diff(np.log(means.to_numpy())).mean()))
        found.append(
            {
                "name": f"{col.name}_index",
                "applies_to": [col.name],
                "kind": "constant_drift",
                "annual": round(growth**12 - 1.0, 6),
                "period_multiplier": round(growth, 8),
                "confidence": 0.7 if len(means) >= 6 else 0.4,
            }
        )
    return found
