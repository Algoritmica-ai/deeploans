"""The generalised lifecycle state machine.

Upstream hardcoded eight mortgage states, a 6x6 matrix, and index constants like
``IDX_CHARGEOFF = 6``. Everything here is driven by the spec instead, so an auto
lease with three states or a credit card with a utilisation ladder needs no code.

Three kinds of state, and the distinction matters:

**ordinary**
    Moves according to the transition matrix.
**absorbing**
    Cannot be left, but the entity stays in the pool and keeps being reported.
    A defaulted loan still appears on every tape while it is being worked out.
**terminal**
    Ends the entity's life. The row is written *once* for the period the entity
    entered the state — a redeemed loan shows a final zero balance — and then
    drops out of the pool.

Each period runs in four passes, matching how these events actually sequence:

0. **Condition hazards**, evaluated against the entity's own columns. These are
   facts, not chances — a loan reaching its maturity date has matured — so they
   settle before anything probabilistic gets a say.
1. **Bernoulli hazards**, evaluated against the *previous* state. Prepayment is
   decided before delinquency, because a borrower who pays the loan off in full
   this month never gets the chance to fall behind in it.
2. **Matrix transition**, for everyone a hazard did not already move.
3. **Dwell-time hazards**, evaluated against the *new* state. Charge-off after
   nine months in default is a consequence of the state just entered, so it is
   counted after the fact.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from sdd.spec.schema import BernoulliHazard, ConditionHazard, DwellTimeHazard, Lifecycle


def _scale_worse(rows: np.ndarray, source: np.ndarray, multipliers: np.ndarray) -> np.ndarray:
    """Scale each row's *worsening* cells, renormalising so it stays a distribution.

    The vectorised form of what a stress scenario does to the whole matrix: take
    probability from staying put and getting better, give it to getting worse,
    and keep the row summing to 1. Applied per entity here, so two facilities in
    the same state can face different odds because something else about them
    differs — their rating, for instance.

    "Worse" is later in the declared state order, which every ladder in this
    project already follows: Performing, Watchlist, Distressed, Defaulted.
    """
    width = rows.shape[1]
    worse = np.arange(width)[None, :] > source[:, None]
    scaled = rows.astype(float, copy=True)

    before = (scaled * worse).sum(axis=1)
    after = np.clip(before * multipliers, 0.0, 0.999999)
    # A row with nothing worse to reach — the last state — is left alone.
    movable = before > 0

    factor = np.ones_like(before)
    factor[movable] = after[movable] / before[movable]
    scaled = np.where(worse, scaled * factor[:, None], scaled)

    rest = ~worse
    rest_before = (scaled * rest).sum(axis=1)
    rest_factor = np.ones_like(rest_before)
    live = movable & (rest_before > 0)
    rest_factor[live] = (1.0 - after[live]) / rest_before[live]
    scaled = np.where(rest, scaled * rest_factor[:, None], scaled)

    totals = scaled.sum(axis=1, keepdims=True)
    return np.divide(scaled, totals, out=scaled, where=totals > 0)


@dataclass
class LifecycleEngine:
    """Compiled, index-based form of a spec's lifecycle section.

    Compiling once and reusing per period matters: at 500k rows x 24 periods,
    looking up state names by string on every step dominates the runtime.
    """

    lc: Lifecycle
    periods_per_year: float

    states: list[str] = field(init=False)
    index: dict[str, int] = field(init=False)
    terminal_idx: np.ndarray = field(init=False)
    absorbing_idx: np.ndarray = field(init=False)
    trans_state_idx: np.ndarray = field(init=False)
    probs: np.ndarray | None = field(init=False)
    cumprobs: np.ndarray | None = field(init=False)
    # position in `trans_state_idx` for each global state index, -1 if absent
    _row_of: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        self.states = list(self.lc.states)
        self.index = {s: i for i, s in enumerate(self.states)}
        self.terminal_idx = np.array([self.index[s] for s in self.lc.terminal], dtype=np.int16)
        self.absorbing_idx = np.array([self.index[s] for s in self.lc.absorbing], dtype=np.int16)

        tstates = self.lc.resolved_transition_states
        self.trans_state_idx = np.array([self.index[s] for s in tstates], dtype=np.int16)

        self._row_of = np.full(len(self.states), -1, dtype=np.int16)
        for row, gidx in enumerate(self.trans_state_idx):
            self._row_of[gidx] = row

        if self.lc.transitions is not None:
            self.probs = np.asarray(self.lc.transitions, dtype=float)
            self.cumprobs = np.cumsum(self.probs, axis=1)
        else:
            self.probs = None
            self.cumprobs = None

    # -- helpers ------------------------------------------------------------

    def to_idx(self, labels: np.ndarray) -> np.ndarray:
        """Map state labels to indices. Unknown labels fall back to state 0."""
        out = np.zeros(len(labels), dtype=np.int16)
        for name, i in self.index.items():
            out[labels == name] = i
        return out

    def to_label(self, idx: np.ndarray) -> np.ndarray:
        return np.asarray(self.states, dtype=object)[idx]

    def is_terminal(self, idx: np.ndarray) -> np.ndarray:
        return np.isin(idx, self.terminal_idx)

    @property
    def dwell_hazards(self) -> list[DwellTimeHazard]:
        return [h for h in self.lc.hazards if isinstance(h, DwellTimeHazard)]

    @property
    def bernoulli_hazards(self) -> list[BernoulliHazard]:
        return [h for h in self.lc.hazards if isinstance(h, BernoulliHazard)]

    @property
    def condition_hazards(self) -> list[ConditionHazard]:
        return [h for h in self.lc.hazards if isinstance(h, ConditionHazard)]

    # -- the step -----------------------------------------------------------

    def step(
        self,
        state_idx: np.ndarray,
        dwell: dict[str, np.ndarray],
        rng: np.random.Generator,
        *,
        hazard_multipliers: dict[str, float] | None = None,
        condition_masks: dict[str, np.ndarray] | None = None,
        row_multipliers: np.ndarray | None = None,
    ) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        """Advance every entity one period.

        Returns the new state indices and the updated dwell-time counters.
        ``hazard_multipliers`` scales named hazard rates, which is how a stress
        scenario doubles prepayment or halves it without editing the spec.

        ``condition_masks`` carries the already-evaluated truth of each condition
        hazard, keyed by name. The engine works in state indices and never sees
        the frame, so the caller — which has both the frame and the expression
        evaluator — does the evaluating and hands the answer in.
        """
        n = len(state_idx)
        new = state_idx.copy()
        moved = np.zeros(n, dtype=bool)
        mult = hazard_multipliers or {}
        masks = condition_masks or {}

        # -- pass 0: condition hazards, which are facts rather than chances.
        # A loan that has reached its maturity date has matured; it must not then
        # be drawn into prepaying or being sold in the same period, so these run
        # before both the Bernoulli pass and the matrix.
        for hz in self.condition_hazards:
            mask = masks.get(hz.name)
            if mask is None:
                continue
            eligible = ~moved & ~self.is_terminal(state_idx) & np.asarray(mask, dtype=bool)
            if hz.from_states:
                allowed = np.array([self.index[s] for s in hz.from_states], dtype=np.int16)
                eligible &= np.isin(state_idx, allowed)
            if hz.excluded_states:
                blocked = np.array([self.index[s] for s in hz.excluded_states], dtype=np.int16)
                eligible &= ~np.isin(state_idx, blocked)
            new[eligible] = self.index[hz.to_state]
            moved |= eligible

        # -- pass 1: Bernoulli hazards on the previous state
        for hz in self.bernoulli_hazards:
            rate = hz.rate_per_period(self.periods_per_year) * mult.get(hz.name, 1.0)
            rate = float(np.clip(rate, 0.0, 1.0))
            if rate <= 0.0:
                continue
            eligible = ~moved & ~self.is_terminal(state_idx)
            if hz.from_states:
                allowed = np.array([self.index[s] for s in hz.from_states], dtype=np.int16)
                eligible &= np.isin(state_idx, allowed)
            if hz.excluded_states:
                blocked = np.array([self.index[s] for s in hz.excluded_states], dtype=np.int16)
                eligible &= ~np.isin(state_idx, blocked)
            fires = eligible & (rng.random(n) < rate)
            new[fires] = self.index[hz.to_state]
            moved |= fires

        # -- pass 2: matrix transition for everyone else
        if self.cumprobs is not None:
            rows = self._row_of[state_idx]
            movable = ~moved & (rows >= 0)
            if movable.any():
                picked = rows[movable]
                if row_multipliers is None:
                    cum = self.cumprobs[picked]
                else:
                    # One matrix row per entity rather than per state, because the
                    # multiplier is a property of the entity — its rating, say —
                    # and not of the state it happens to be in.
                    cum = np.cumsum(
                        _scale_worse(self.probs[picked], picked, row_multipliers[movable]),
                        axis=1,
                    )
                draws = rng.random(int(movable.sum()))[:, None]
                landed = (draws > cum).sum(axis=1)
                new[movable] = self.trans_state_idx[landed]

        # -- pass 3: dwell-time hazards on the new state
        updated_dwell = dict(dwell)
        for hz in self.dwell_hazards:
            here = new == self.index[hz.from_state]
            counter = updated_dwell.get(hz.name, np.zeros(n, dtype=np.int32))
            counter = np.where(here, counter + 1, 0).astype(np.int32)
            fires = here & (counter >= hz.periods)
            new[fires] = self.index[hz.to_state]
            # A fired counter resets; the entity is no longer in `from_state`.
            updated_dwell[hz.name] = np.where(fires, 0, counter).astype(np.int32)

        return new, updated_dwell

    def initial_dwell(self, n: int, state_idx: np.ndarray | None = None) -> dict[str, np.ndarray]:
        """Seed the dwell counters for the period-0 book.

        An entity that is *already* in the watched state at period 0 has been
        observed there for one period, so its counter starts at 1. Starting it at
        0 instead would give those entities an extra free period before the
        hazard fires, and the panel would show two different dwell lengths
        depending on whether an entity started in the state or arrived later.
        """
        out: dict[str, np.ndarray] = {}
        for hz in self.dwell_hazards:
            counter = np.zeros(n, dtype=np.int32)
            if state_idx is not None:
                counter[state_idx == self.index[hz.from_state]] = 1
            out[hz.name] = counter
        return out
