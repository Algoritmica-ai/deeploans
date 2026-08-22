"""Structure the profiler used to throw away.

Three things the designer can express and the profiler could not read back:
maturity as a rule rather than a chance, a second state machine running beside
the lifecycle, and the parent record several entities share. A relearned CLO had
flat-rate maturity, no rating migration, and one company per facility.

The CLO pack is the fixture throughout because it is the only pack that declares
all three. The other two packs are just as important, in the opposite direction:
they declare none of it, so anything found in them is a false positive — and
false positives here are worse than misses. A missed group is visible in the
spec and can be added by hand; a spurious one silently corrupts every
concentration figure the book is measured on, and looks plausible doing it.
"""

from __future__ import annotations

import pathlib
import tempfile

import pandas as pd
import pytest

from sdd import api
from sdd.profile import build_spec

CLO = "clo_eu_leveraged_loans"
PLAIN = ["rmbs_nl_green_lion", "auto_abs_esma_annex5"]
ENTITIES = 400

# Long enough for 60-96 month facilities to actually mature. At the pack's own
# 24 periods a 400-facility book matures four of them, and a rule cannot be
# learned from a panel that does not contain the event — see the evidence test
# at the bottom, which pins that limit rather than hiding it.
LONG_PANEL = 72


def _relearn(pack: str, periods: int | None = None):
    """Run a pack, profile the output, and hand back both spec and panel."""
    base = api.load(pack)
    dumped = base.model_dump(mode="json", exclude_none=True, by_alias=True)
    if periods:
        dumped["entity"]["calendar"]["periods"] = periods

    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(dumped, ENTITIES, tmp, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    learned, profile = build_spec(
        panel,
        name=f"relearned_{pack}",
        id_column=base.entity.id_column,
        time_column=base.entity.time_column,
        state_column=base.lifecycle.state_column,
    )
    return {"base": base, "panel": panel, "spec": learned, "profile": profile}


@pytest.fixture(scope="module")
def clo():
    return _relearn(CLO)


@pytest.fixture(scope="module")
def clo_long():
    return _relearn(CLO, periods=LONG_PANEL)


@pytest.fixture(scope="module")
def plain():
    return {pack: _relearn(pack) for pack in PLAIN}


# ---------------------------------------------------------------------------
# item 4 — conditions
# ---------------------------------------------------------------------------


def test_maturity_is_a_rule_not_a_rate(clo):
    """The expression comes back, not just the fact that facilities mature.

    A flat rate makes the state reachable and the spec runnable, which is why
    this went unnoticed: it is a *different rule*, giving a 96-month facility the
    same chance of maturing in month three as a 60-month one.
    """
    hazard = _hazard_to(clo["spec"], "Matured")
    original = _hazard_to(clo["base"], "Matured")

    assert hazard.kind == "condition"
    assert hazard.when == original.when


@pytest.mark.parametrize("state", ["Prepaid", "Sold"])
def test_chance_stays_chance(clo, state):
    """Prepayment is a coin, and must not be relearned as a rule.

    The trap is specific and it fired: every prepaid facility satisfies
    `current_balance <= 0`, on 255 of 255, because entering Prepaid is what
    *sets* the balance to zero. Read as a trigger it is circular — regenerate
    with "prepay when the balance reaches zero" and nothing ever prepays,
    because nothing reaches zero without prepaying first.
    """
    assert _hazard_to(clo["spec"], state).kind == "bernoulli"


def test_a_workout_stays_a_delay(plain):
    """A nine-month workout must not be relearned as a threshold on days past due.

    `days_past_due >= 180` scores perfectly against a charge-off, for the
    uninteresting reason that days past due *is* the workout clock in other
    units. Restating a clock as a threshold on its own read-out adds nothing and
    hides the mechanism, so the dwell test is tried first.
    """
    for pack, case in plain.items():
        learned = _hazard_to(case["spec"], "Charged-Off")
        original = _hazard_to(case["base"], "Charged-Off")
        assert learned.kind == "dwell_time", f"{pack}: {learned.kind}"
        assert learned.periods == original.periods


@pytest.mark.local_only(
    "needs a 72-period panel so the facilities actually mature; the instance ages up to 60"
)
def test_the_carve_out_comes_back(clo_long):
    """A defaulted facility does not mature, and the spec should say so.

    Its term keeps counting down through the workout, so it reaches zero months
    to maturity and never matures. Counted as counter-examples, five such
    facilities dropped a perfect rule to 93% precision and the whole condition
    was lost. They are a carve-out, and the pack says so in as many words.
    """
    hazard = _hazard_to(clo_long["spec"], "Matured")
    assert hazard.kind == "condition"
    assert "Defaulted" in hazard.excluded_states


def test_one_event_establishes_nothing(clo):
    """A rule needs evidence, and the fallback must be the honest one.

    Found by relearning a relearned CLO: the second-generation panel held a
    single maturity, and "modal dwell = 18, on 100% of events" wrote an
    eighteen-month fixed delay into the spec on a sample of one.
    """
    from sdd.profile.panel import MIN_EXIT_EVENTS, learn_exits

    panel = clo["panel"].sort_values(["facility_id", "reporting_date"])
    thin = panel[panel["credit_state"].isin(["Performing", "Sold"])]
    exits = learn_exits(thin, "facility_id", "reporting_date", "credit_state", ["Sold"], [], 12.0)
    moves = int((thin["credit_state"] == "Sold").sum())
    if moves < MIN_EXIT_EVENTS:
        assert all(e["kind"] != "dwell_time" for e in exits)


# ---------------------------------------------------------------------------
# item 5 — secondary chains
# ---------------------------------------------------------------------------


def test_the_rating_chain_comes_back(clo):
    """Column, states and the coupling that keeps it honest."""
    chains = clo["spec"].secondary_chains
    assert len(chains) == 1

    learned, original = chains[0], clo["base"].secondary_chains[0]
    assert learned.lifecycle.state_column == original.lifecycle.state_column
    assert set(learned.lifecycle.states) == set(original.lifecycle.states)
    assert learned.lifecycle.terminal == [], "only the lifecycle may end a life"

    # A defaulted facility *is* rated D. Uncoupled, the two run independently and
    # the output carries BB-rated facilities sitting in default.
    assert learned.coupling.forced_by == original.coupling.forced_by


def test_the_chain_is_not_a_restatement_of_the_state(clo):
    """Three chains were offered where the pack has one.

    `rating_at_cutoff` holds nine grades, `rating_bucket` collapses them to four
    and `ccc_flag` to two. All three migrate and all three produce a clean
    matrix; two of them are the first with detail thrown away. Run
    independently they would drift apart, and the output would carry facilities
    rated B- whose bucket said CCC.
    """
    columns = {c.lifecycle.state_column for c in clo["spec"].secondary_chains}
    assert "rating_bucket" not in columns
    assert "ccc_flag" not in columns


def test_a_binned_column_is_not_a_chain(plain):
    """`balance_bucket` migrates as a loan amortises. It is still not a chain.

    Generated independently it would report a 300k-350k band on a loan carrying
    80k. These are derivations, and the derivation pass now runs first so every
    later pass sees them marked.
    """
    for pack, case in plain.items():
        assert case["spec"].secondary_chains == [], f"{pack} invented a chain"


def test_the_chain_survives_regeneration(clo, tmp_path):
    """A chain in the spec is worth nothing if the run does not honour it."""
    dumped = clo["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    result = api.run(dumped, ENTITIES, tmp_path / "chain", seed=7, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    column = clo["spec"].secondary_chains[0].lifecycle.state_column
    assert panel[column].nunique() > 1

    for state, forced in clo["spec"].secondary_chains[0].coupling.forced_by.items():
        rows = panel[panel["credit_state"] == state]
        if not rows.empty:
            assert (rows[column] == forced).all(), f"{state} should force {forced}"


# ---------------------------------------------------------------------------
# item 6 — groups
# ---------------------------------------------------------------------------


def test_the_obligor_comes_back(clo):
    """The key, its shape, and the attributes that hang off it."""
    groups = clo["spec"].groups
    assert len(groups) == 1

    learned, original = groups[0], clo["base"].groups[0]
    assert learned.key == original.key
    assert learned.id_format == original.id_format
    assert learned.size.max_members == original.size.max_members
    original_names = {c.name for c in original.columns}
    assert original_names <= {c.name for c in learned.columns}


def test_group_attributes_leave_the_entity(clo):
    """Moved, not copied — and the move is the point.

    Noting the key while leaving the attributes where they are would change
    nothing: the industry would still be drawn per facility, and the same
    obligor would still come out in four industries at once.
    """
    entity_columns = {c.name for c in clo["spec"].columns}
    for column in clo["spec"].groups[0].columns:
        assert column.name not in entity_columns
    assert clo["spec"].groups[0].key not in entity_columns


def test_members_of_a_group_agree(clo, tmp_path):
    """The whole reason groups exist, checked on regenerated data."""
    dumped = clo["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    result = api.run(dumped, ENTITIES, tmp_path / "groups", seed=11, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    group = clo["spec"].groups[0]
    sizes = panel.groupby("facility_id").head(1)[group.key].value_counts()
    assert sizes.max() > 1, "every group has one member: the structure is gone"
    assert sizes.max() <= group.size.max_members

    for column in group.columns:
        disagreeing = panel.groupby(group.key)[column.name].nunique().gt(1).sum()
        assert disagreeing == 0, f"{column.name} differs within {disagreeing} groups"


def test_no_columns_are_lost_in_the_move(clo, tmp_path):
    """Provenance changes; the output does not.

    Group attributes reach the panel like any other column. This also pins a
    loader gap the move exposed: `emit.column_order` counted group columns as
    produced by nothing, because the one pack with groups declares no column
    order and nothing had exercised the pair together.
    """
    dumped = clo["spec"].model_dump(mode="json", exclude_none=True, by_alias=True)
    assert api.check(dumped)["valid"]

    result = api.run(dumped, ENTITIES, tmp_path / "cols", seed=13, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    assert set(clo["panel"].columns) <= set(panel.columns)


def test_a_category_is_not_a_group(plain):
    """The failure mode that survived every other guard.

    `occupancy` explains `property_usage` and `buy_to_let_flag`, and passes as a
    parent record holding 353 mortgages. `economic_region_nuts3` explains
    `province`, perfectly, because a region rolls up into one. Neither is a
    parent: 353 households that all own their homes are not the same household.
    """
    for pack, case in plain.items():
        assert case["spec"].groups == [], f"{pack} invented {[g.key for g in case['spec'].groups]}"


def test_a_near_unique_column_is_not_a_group(clo):
    """The degenerate case that breaks naive detection.

    Partition a book into 400 groups of one and *every* column is constant
    within its group, so a near-unique column scores as the perfect parent
    record. Measured, the first pass picked `ebitda_eur` — a float, near-unique,
    explaining all nine other static columns vacuously.
    """
    key = clo["spec"].groups[0].key
    assert key == "obligor_id"

    first = clo["panel"].groupby("facility_id").head(1)
    assert len(first) / first[key].nunique() > 1.5


def test_the_group_count_is_not_the_visible_one(clo):
    """`ratio` says how many groups were created, not how many are occupied.

    Zipf weights leave a tail of groups holding nobody: the CLO creates 180
    obligors per 400 facilities and 127 of them appear. Copying the visible 0.32
    back would create 127 next time, of which ~90 would appear, and the book
    would lose obligors on every round trip — ending at one facility per
    obligor, which is the structure this feature exists to preserve.
    """
    first = clo["panel"].groupby("facility_id").head(1)
    occupied = first[clo["spec"].groups[0].key].nunique() / len(first)
    assert clo["spec"].groups[0].ratio > occupied


# ---------------------------------------------------------------------------
# the round trip, end to end
# ---------------------------------------------------------------------------


@pytest.mark.local_only("same 72-period panel, for the same reason")
def test_structure_survives_repeated_relearning(clo_long, tmp_path):
    """Relearn a relearned spec, twice, and the structure must still be there.

    A single round trip can hide a ratchet. Each generation is profiled from the
    previous generation's output, so anything that shrinks a little each time
    shows up as a trend rather than as noise.
    """
    spec = clo_long["spec"]
    for generation in range(2):
        dumped = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
        assert api.check(dumped)["valid"]
        dumped["entity"]["calendar"]["periods"] = LONG_PANEL

        result = api.run(
            dumped,
            ENTITIES,
            tmp_path / f"gen{generation}",
            seed=20 + generation,
            validate_output=False,
        )
        panel = pd.read_parquet(result["panel"])
        spec, _ = build_spec(
            panel,
            name=f"gen{generation}",
            id_column="facility_id",
            time_column="reporting_date",
            state_column="credit_state",
        )

        assert len(spec.groups) == 1, f"generation {generation} lost the obligor"
        assert len(spec.secondary_chains) == 1, f"generation {generation} lost the rating"
        assert _hazard_to(spec, "Matured").kind == "condition"

        first = panel.groupby("facility_id").head(1)
        sizes = first[spec.groups[0].key].value_counts()
        assert sizes.max() > 1, f"generation {generation} collapsed to one member per group"


def test_a_rule_cannot_be_learned_from_a_panel_without_the_event(clo):
    """The honest limit, pinned so it is not mistaken for a bug later.

    At the CLO pack's own 24 periods, a book of 60-96 month facilities matures a
    handful. Relearn that and relearn it again and the maturities run out
    entirely — there is no rule to find, because the data no longer contains
    one. Nothing here fixes that; the panel has to be long enough to hold the
    event.
    """
    matured = clo["panel"][clo["panel"]["credit_state"] == "Matured"]
    assert matured["facility_id"].nunique() < ENTITIES * 0.05


def test_a_path_finds_the_same_structure_as_a_frame(clo, tmp_path):
    """The route a user actually takes must not lose the group.

    Every test above hands `build_spec` a DataFrame. The web UI and the CLI hand
    it a `Path`, and group detection — alone among the passes, because it counts
    rows rather than summarising columns — checked for a frame and silently did
    nothing when it got a path. Silently, because a spec with no groups is a
    perfectly valid spec.

    Caught on the deployed Space against a tape the detector handles correctly
    in memory, which is the only reason it was caught at all.
    """
    csv = tmp_path / "tape.csv"
    clo["panel"].to_csv(csv, index=False)

    from_path, _ = build_spec(
        csv,
        name="from_path",
        id_column="facility_id",
        time_column="reporting_date",
        state_column="credit_state",
    )

    assert [g.key for g in from_path.groups] == [g.key for g in clo["spec"].groups]
    assert [c.name for c in from_path.groups[0].columns] == [
        c.name for c in clo["spec"].groups[0].columns
    ]


def _hazard_to(spec, state: str):
    """The hazard that sends entities to a state."""
    for hazard in spec.lifecycle.hazards:
        if getattr(hazard, "to_state", None) == state:
            return hazard
    raise AssertionError(f"no hazard reaches {state!r}")
