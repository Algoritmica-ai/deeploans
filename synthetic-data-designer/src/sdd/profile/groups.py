"""Recover the parent records several entities share.

The entity is the unit of the panel — a facility, a loan, an account. A *group*
is the thing behind several of them: the obligor behind three facilities, the
household behind a mortgage and a buy-to-let, the dealer behind a month of car
loans. What makes it more than a category column is that a group carries its own
attributes, identical for every member: three facilities lent to the same company
must agree about that company's industry, country and revenue.

Read back as ordinary entity columns, that structure is lost. Every facility gets
its own draw, so the same obligor comes out in four industries at once, and every
concentration figure the book is measured on — largest obligor, top ten, single
industry — becomes meaningless. A relearned CLO had one company per facility.

The detection turns on one question: **is knowing this column enough to know the
others?** A key with attributes constant inside it is a parent record. A category
column is not — two obligors in Healthcare share an industry and agree about
nothing else.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from sdd.profile.profiler import DatasetProfile

# A group holding roughly one member is a relabelled entity id, and it is the
# degenerate case that breaks naive detection: partition a book into 400 groups
# of one and *every* column is constant within its group, so a near-unique
# column scores as the perfect parent record. Measured, the first pass picked
# `ebitda_eur` — a float, near-unique, explaining all nine other static columns
# vacuously. Real sharing is the whole claim, so it is required up front.
MIN_MEMBERS_PER_GROUP = 1.5
MIN_GROUPS = 2

# The other end of the same problem, and the one genuinely judgement-based
# threshold here. A column with four values and three hundred members apiece is
# classifying entities, not identifying a parent: three hundred households that
# all own their homes are not the same household. Measured, `occupancy` was
# offered as the green pack's group structure on exactly that basis — it does
# explain `property_usage` and `buy_to_let_flag`, because those are correlated
# with it, and it is still a category.
#
# The line is drawn where a real parent record stops being plausible. A company
# carries a handful of facilities and a household a couple of mortgages; past
# roughly two dozen members the reading is a classification.
MAX_MEMBERS_PER_GROUP = 25

# A key must explain at least this many other columns to count as a parent
# record.
#
# Two rather than one, and the difference is a false positive that survived
# every other guard. `economic_region_nuts3` explains `province` — perfectly,
# because a region rolls up into a province — and passes as a parent record
# holding thirty-seven mortgages. It is a geographic hierarchy: two mortgages in
# one region are not the same household, and generating "the region's attributes
# once, shared by every member" would be inventing a borrower that does not
# exist.
#
# One explained column is too easily a coarsening of the key itself. Two is the
# claim that the key carries a *bundle* of facts about a thing — an industry, a
# country, a revenue — which is what a parent record is.
#
# The cost is real and worth stating: a genuine group whose parent has exactly
# one attribute is missed. That trade is deliberate. A missed group is visible
# in the spec and can be added by hand; a spurious one silently corrupts every
# concentration figure the book is measured on, and looks plausible doing it.
MIN_SHARED_COLUMNS = 2

# How constant an attribute has to be inside a group. Not 1.0: a real tape
# carries typos and restatements, and refusing the whole structure over one
# disagreeing row would be the wrong trade.
ATTRIBUTE_PURITY = 0.98

# Concentration values tried when fitting the member-size distribution.
CONCENTRATION_GRID = tuple(round(1.1 + 0.1 * i, 2) for i in range(25))

# How many candidate group counts to try between the floor and the ceiling.
CREATED_GRID_POINTS = 14


def learn_groups(df: pd.DataFrame, profile: DatasetProfile) -> list[dict[str, Any]]:
    """The group structure behind a panel, or nothing if there is none."""
    if not profile.id_column or profile.id_column not in df.columns:
        return []

    # Measured on the opening cohort, not on every entity the panel ever met.
    # A book that reinvests keeps acquiring assets, and those later arrivals
    # attach to groups under a different rule — `new_group_rate`, learned
    # separately below. Pooling the two would fit one shape to two mechanisms.
    first = df.groupby(profile.id_column, sort=False).head(1)
    if profile.time_column and profile.time_column in df.columns:
        opening = df[df[profile.time_column] == df[profile.time_column].min()]
        if len(opening) >= MIN_GROUPS:
            first = opening.groupby(profile.id_column, sort=False).head(1)
    entities = len(first)
    if entities < 2:
        return []

    static = [
        c.name
        for c in profile.columns
        if c.role == "static" and c.name in first.columns and c.name != profile.id_column
    ]

    # A key identifies; it does not measure. Floats are excluded as candidates
    # for that reason and not as a heuristic — an obligor is named, and a column
    # of revenues that happens to repeat is a coincidence rather than a parent
    # record. They remain eligible as group *attributes*, which is what revenue
    # actually is.
    keyable = {c.name for c in profile.columns if c.dtype in ("category", "str", "int", "bool")}

    best: dict[str, Any] | None = None
    for key in static:
        if key not in keyable:
            continue
        members = first[key].dropna()
        n_groups = members.nunique()
        if n_groups < MIN_GROUPS or n_groups >= entities:
            continue
        members_each = entities / n_groups
        if not MIN_MEMBERS_PER_GROUP <= members_each <= MAX_MEMBERS_PER_GROUP:
            continue

        shared = _shared_columns(first, key, [c for c in static if c != key])
        if len(shared) < MIN_SHARED_COLUMNS:
            continue

        # More explained columns wins; a tie goes to the finer key. A coarse
        # column that happens to explain the same attributes is the same
        # structure described with information thrown away.
        rank = (len(shared), n_groups)
        if best is None or rank > best["_rank"]:
            best = {"key": key, "shared": shared, "n_groups": n_groups, "_rank": rank}

    if best is None:
        return []

    sizes = first[best["key"]].value_counts()
    shape = _fit_shape(sizes, entities)
    learned = {
        "name": _group_name(best["key"]),
        "key": best["key"],
        "ratio": shape["ratio"],
        "id_format": _id_format(first[best["key"]]),
        "columns": best["shared"],
        "size": shape["size"],
        "new_group_rate": _new_group_rate(df, profile, best["key"]),
        "evidence": (
            f"{best['n_groups']:,} groups over {entities:,} entities, "
            f"largest holding {int(sizes.iloc[0])}; "
            f"{len(best['shared'])} attributes constant within the group"
        ),
    }
    return [learned]


def _shared_columns(first: pd.DataFrame, key: str, others: list[str]) -> list[str]:
    """Columns that hold one value per group.

    The whole test, and the reason a category column does not pass it. Every
    facility of one obligor reports that obligor's industry, so `industry` is
    constant within `obligor_id`. Nothing is constant within `industry` except
    itself.

    Columns that are globally constant are excluded: they are trivially constant
    within every group and would let any key look like a parent record.
    """
    shared = []
    for column in others:
        if first[column].nunique(dropna=False) <= 1:
            continue
        per_group = first.groupby(key, sort=False)[column].nunique(dropna=False)
        if float((per_group <= 1).mean()) >= ATTRIBUTE_PURITY:
            shared.append(column)
    return shared


def _fit_shape(sizes: pd.Series, entities: int) -> dict[str, Any]:
    """How many groups the book was built with, and how lumpy they are.

    Both at once, because they cannot be read off separately.

    **The ratio is not what the tape shows.** `ratio` says how many groups were
    *created*; a tape shows how many ended up with at least one member, and Zipf
    weights leave a tail of groups holding nobody. The CLO creates 180 obligors
    per 400 facilities and 127 of them appear. Copying the visible 0.32 back into
    the spec would create 127 next time, of which ~90 would appear, and the book
    would lose obligors on every round trip — a ratchet that ends with one
    facility per obligor, which is the structure this whole feature exists to
    preserve.

    **The exponent does not act alone either.** Members are drawn against Zipf
    weights subject to a cap, and the cap bends the distribution in a way no
    closed form covers.

    So both are fitted by running the generator's own allocator across a grid and
    keeping the pair whose output looks most like what was observed — matched on
    the size histogram (what share of groups hold one member, two, three: the
    shape a concentration limit reads) and on how many groups end up occupied.
    Simulating rather than solving also means there is only one model of the
    allocation, so the fit cannot drift away from the thing it is fitting.
    """
    from sdd.generate.groups import GroupError, assign_members
    from sdd.spec.schema import Group, GroupSize

    occupied = len(sizes)
    max_members = int(sizes.max())
    observed = sizes.value_counts(normalize=True)

    if len(observed) < 2:
        return {
            "ratio": round(occupied / entities, 4),
            "size": {"kind": "fixed", "max_members": max_members},
        }

    # Created groups can only outnumber occupied ones, and are bounded below by
    # what the cap allows: too few groups and the members do not fit.
    floor = max(occupied, -(-entities // max_members))
    ceiling = min(entities, max(floor + 1, occupied * 3))
    counts = sorted({round(c) for c in np.linspace(floor, ceiling, CREATED_GRID_POINTS)})

    best: tuple[float, int, float] | None = None
    for concentration in CONCENTRATION_GRID:
        for created in counts:
            table = pd.DataFrame({"_key": [f"G{i}" for i in range(created)]})
            group = Group(
                name="fit",
                key="_key",
                count=created,
                size=GroupSize(kind="zipf", concentration=concentration, max_members=max_members),
            )
            try:
                drawn = pd.Series(
                    assign_members(group, table, entities, np.random.default_rng(0))
                ).value_counts()
            except GroupError:
                # The cap and this group count cannot hold the members. Not a
                # failure — just a combination the book cannot have had.
                continue

            shape = drawn.value_counts(normalize=True)
            error = sum(
                abs(float(shape.get(k, 0.0)) - float(observed.get(k, 0.0)))
                for k in set(shape.index) | set(observed.index)
            )
            error += abs(len(drawn) - occupied) / occupied
            if best is None or error < best[0]:
                best = (error, created, concentration)

    if best is None:
        return {
            "ratio": round(occupied / entities, 4),
            "size": {"kind": "zipf", "max_members": max_members},
        }

    error, created, concentration = best
    return {
        "ratio": round(created / entities, 4),
        "size": {
            "kind": "zipf",
            "concentration": concentration,
            "max_members": max_members,
            "fit_error": round(error, 4),
        },
    }


def _new_group_rate(df: pd.DataFrame, profile: DatasetProfile, key: str) -> float:
    """How often an entity joining later brings a group the book has not met.

    A lender that lends again to a borrower it already has produces a low rate;
    one whose every new loan is to a new name produces a high one.

    Measured **cohort by cohort**, with the roll of known groups updated only at
    the cut-off boundary. Measured entity by entity instead it read 0.29 against
    a declared 0.60, because the second facility of a newly arrived obligor
    counted as attaching to an existing group — the obligor having become
    "known" moments earlier, within the same cohort. The generator mints its new
    groups per cohort, so the measurement has to be taken per cohort too.
    """
    if not profile.time_column or profile.time_column not in df.columns:
        return 0.5

    first_seen = df.groupby(profile.id_column, sort=False)[profile.time_column].min()
    entity_key = df.groupby(profile.id_column, sort=False)[key].first()
    cutoffs = sorted(df[profile.time_column].dropna().unique())
    if len(cutoffs) < 2:
        return 0.5

    known = set(entity_key[first_seen == cutoffs[0]].dropna())
    arrived = fresh = 0
    for cutoff in cutoffs[1:]:
        cohort = entity_key[first_seen == cutoff].dropna()
        if cohort.empty:
            continue
        arrived += len(cohort)
        fresh += int((~cohort.isin(known)).sum())
        known.update(cohort)

    if arrived < 10:
        return 0.5
    return round(min(max(fresh / arrived, 0.0), 1.0), 4)


def _group_name(key: str) -> str:
    """A readable name for the group, taken from its key column."""
    name = re.sub(r"_(id|key|code|no|number)$", "", key.strip().lower())
    return name or key


def _id_format(values: pd.Series) -> str:
    """Reproduce the shape of the observed identifiers.

    Keeps a relearned tape recognisable: obligors that arrived as OBL00042 come
    back as OBL00042 rather than G000042. Falls back to the schema default where
    the identifiers follow no pattern, since inventing one from a sample of
    UUIDs would be worse than admitting the shape was not recovered.
    """
    sample = values.dropna().astype(str)
    if sample.empty:
        return "G{seq:06d}"

    matches = sample.str.extract(r"^([^0-9]*)(\d+)$")
    if matches.isna().any().any():
        return "G{seq:06d}"

    prefixes = matches[0].unique()
    if len(prefixes) != 1:
        return "G{seq:06d}"

    widths = matches[1].str.len().unique()
    width = int(widths[0]) if len(widths) == 1 else 0
    return f"{prefixes[0]}{{seq:0{width}d}}" if width else f"{prefixes[0]}{{seq}}"
