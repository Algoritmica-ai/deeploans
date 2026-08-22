"""Parent records several entities share.

The entity stays the unit of the panel. A group is the thing behind several of
them — the obligor behind three facilities, the household behind a mortgage and
a buy-to-let, the dealer behind a month of car loans.

What makes this more than a category column is that a group carries its *own*
attributes, generated once and identical for every member. Three facilities lent
to the same company must agree about that company's industry and revenue.
Generated per facility they would disagree, and any analysis by obligor becomes
meaningless.

The state has to outlive a single book. A pool that reinvests builds later
cohorts from the same spec, and a facility acquired in month twenty may belong to
an obligor created in month one — so the group table is carried through ageing
rather than rebuilt, and later cohorts either attach to a group that exists or
mint a new one.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sdd.spec.schema import DesignSpec, Group

# Below this many rows the Iman-Conover reordering is mostly noise: measured at
# a target of 0.90, five rows land 0.85 with a standard deviation of 0.24, while
# fifty land 0.88 with 0.045. Reinvestment cohorts mint a median of four
# obligors at a time, so without a floor the correlation holds on the opening
# book and washes out across everything bought afterwards - 0.99 among the
# opening obligors against 0.80 across the whole book, in a measured run.
MIN_CORRELATED_ROWS = 60


class GroupError(ValueError):
    """A group could not be built or attached."""


def _member_weights(group: Group, n_groups: int, rng: np.random.Generator) -> np.ndarray:
    """Relative likelihood of each group taking the next member.

    Zipf by default, because real books are lumpy: a few borrowers carry several
    facilities and most carry one. Uniform spreads members evenly, which is
    tidier and less true — and a portfolio with no lumpiness is one no
    concentration limit would ever bite on.
    """
    size = group.size
    if size.kind == "uniform":
        return np.ones(n_groups, dtype=float)
    if size.kind == "fixed":
        return np.ones(n_groups, dtype=float)
    ranks = np.arange(1, n_groups + 1, dtype=float)
    weights = 1.0 / np.power(ranks, size.concentration)
    # Shuffle so the largest group is not always the first identifier.
    rng.shuffle(weights)
    return weights


def build_group_table(
    spec: DesignSpec,
    group: Group,
    n_groups: int,
    rng: np.random.Generator,
    *,
    id_offset: int = 0,
) -> pd.DataFrame:
    """Generate ``n_groups`` parent records, each with its own attributes."""
    from sdd.generate.samplers import sample

    # A correlated table is drawn larger than asked for and then cut back. The
    # reordering needs rows to work with, and a cohort that wants four obligors
    # would otherwise get four rows of noise.
    #
    # Truncating afterwards is safe, and that is not obvious. The reordering
    # gives every row a *joint* draw, so the first `n_groups` rows are an
    # ordinary sample of that joint distribution: both the marginals and the
    # correlation survive in expectation. The cost is generating some rows that
    # are thrown away, which is a few hundred numbers.
    correlated = group.correlation_target is not None
    drawn = max(n_groups, MIN_CORRELATED_ROWS) if correlated else n_groups

    frame = pd.DataFrame(index=pd.RangeIndex(drawn))
    frame[group.key] = [
        group.id_format.format(seq=id_offset + i + 1, name=group.name) for i in range(drawn)
    ]
    for column in group.columns:
        if column.generator is None:
            raise GroupError(
                f"group {group.name!r} column {column.name!r} has no generator; a group "
                "attribute is generated, not derived"
            )
        try:
            frame[column.name] = sample(column.generator, drawn, rng, frame, id_offset)
        except Exception as exc:
            raise GroupError(
                f"group {group.name!r} column {column.name!r} failed to sample: {exc}"
            ) from exc

    # The attributes were drawn one generator at a time, so they are independent
    # until this puts the joint structure back. Revenue and leverage move
    # together on a real book, and a portfolio where the most indebted borrowers
    # are neither larger nor smaller than anyone else is one nobody has seen.
    #
    # Reordering cannot change any column's own distribution — it only permutes
    # values already drawn — so the marginals the pack declares survive exactly.
    #
    # Scaled by the same `generation.correlation` slider the entity columns use.
    # Two knobs for one idea would be a worse interface than one that reaches
    # both, and a user turning correlation down expects the whole book to loosen.
    if correlated:
        from sdd.generate.randomness import reorder_to_correlation

        reorder_to_correlation(
            frame, group.correlation_target, float(spec.generation.correlation), rng
        )
        frame = frame.head(n_groups).reset_index(drop=True)

    return frame


MEMBER_COUNT = "__members"


def assign_members(
    group: Group,
    table: pd.DataFrame,
    n_entities: int,
    rng: np.random.Generator,
    *,
    eligible: np.ndarray | None = None,
) -> np.ndarray:
    """Pick a group for each entity, honouring the size distribution and any cap.

    ``eligible`` restricts the draw to a subset of rows — used when a later
    cohort may only attach to groups that already existed.

    The cap counts a group's members over the whole run, not within one call.
    Counted per call it is not a cap: a borrower filled to its limit in the
    opening book would quietly take more every time the pool reinvested. Running
    totals live in the group table itself, so they travel with it.
    """
    if table.empty:
        raise GroupError(f"group {group.name!r} has no rows to assign members to")

    index = np.arange(len(table)) if eligible is None else np.asarray(eligible)
    if index.size == 0:
        raise GroupError(f"group {group.name!r} has no eligible parents")

    weights = _member_weights(group, len(index), rng)
    weights = weights / weights.sum()

    # Concatenating a new cohort's groups onto existing ones leaves the count
    # missing for the new rows, and a missing count is not a full group.
    if MEMBER_COUNT not in table.columns:
        table[MEMBER_COUNT] = 0
    else:
        table[MEMBER_COUNT] = table[MEMBER_COUNT].fillna(0).astype(int)

    keys = table[group.key].to_numpy()
    counts = table[MEMBER_COUNT].to_numpy(copy=True)
    cap = group.size.max_members

    if cap is None:
        picks = rng.choice(index, size=n_entities, replace=True, p=weights)
        np.add.at(counts, picks, 1)
        table[MEMBER_COUNT] = counts
        return keys[picks]

    # With a cap, draw one at a time and retire a parent once it is full. Slower,
    # and the only way a cap is a cap rather than a suggestion.
    already = counts[index]
    remaining = np.maximum(cap - already, 0)
    if remaining.sum() < n_entities:
        raise GroupError(
            f"group {group.name!r} caps members at {cap}, leaving room for "
            f"{int(remaining.sum())} more across {len(index)} groups but asked to place "
            f"{n_entities}. Raise `max_members`, lower `ratio`, or raise `new_group_rate`."
        )

    chosen = np.empty(n_entities, dtype=object)
    for i in range(n_entities):
        live = weights * (remaining > 0)
        live = live / live.sum()
        pick = rng.choice(len(index), p=live)
        remaining[pick] -= 1
        counts[index[pick]] += 1
        chosen[i] = keys[index[pick]]
    table[MEMBER_COUNT] = counts
    return chosen


def attach_groups(
    spec: DesignSpec,
    frame: pd.DataFrame,
    rng: np.random.Generator,
    *,
    state: dict[str, pd.DataFrame] | None = None,
    fresh_cohort: bool = False,
) -> pd.DataFrame:
    """Give every entity a group, and join that group's attributes onto it.

    ``state`` carries group tables between cohorts. Passed in populated, existing
    groups are reused and only ``new_group_rate`` of the cohort mints new ones —
    which is what makes an acquisition in month twenty able to belong to an
    obligor created in month one.
    """
    if not spec.groups:
        return frame

    n = len(frame)
    for group in spec.groups:
        existing = None if state is None else state.get(group.name)

        if existing is None or existing.empty:
            wanted = group.group_count(n)
            table = build_group_table(spec, group, wanted, rng)
            keys = assign_members(group, table, n, rng)
        elif not fresh_cohort:
            table = existing
            keys = assign_members(group, table, n, rng)
        else:
            # A cohort joining an open pool: some entities belong to borrowers
            # the book already has, the rest bring new ones.
            new_count = round(n * group.new_group_rate)
            minted = pd.DataFrame()
            if new_count:
                wanted = max(1, group.group_count(new_count))
                minted = build_group_table(spec, group, wanted, rng, id_offset=len(existing))
            table = pd.concat([existing, minted], ignore_index=True) if new_count else existing

            keys = np.empty(n, dtype=object)
            takes_new = rng.random(n) < group.new_group_rate
            if takes_new.any() and not minted.empty:
                new_rows = np.arange(len(existing), len(table))
                keys[takes_new] = assign_members(
                    group, table, int(takes_new.sum()), rng, eligible=new_rows
                )
            old_mask = ~takes_new if not minted.empty else np.ones(n, dtype=bool)
            if old_mask.any():
                old_rows = np.arange(len(existing))
                keys[old_mask] = assign_members(
                    group, table, int(old_mask.sum()), rng, eligible=old_rows
                )

        if state is not None:
            state[group.name] = table

        frame[group.key] = keys
        attributes = table.set_index(group.key)
        for column in group.columns:
            frame[column.name] = frame[group.key].map(attributes[column.name])

    return frame
