"""Groups: several entities sharing one parent record.

Generic by design. The CLO pack uses it for the obligor behind several
facilities, but the same shape is a household holding a mortgage and a
buy-to-let, a dealer behind a month of car loans, or a company behind several
SME facilities.

What makes a group more than a category column is that it carries its *own*
attributes, generated once and identical for every member. That is what
`group_columns_stable` exists to protect: if three facilities lent to the same
company disagree about that company's industry, every figure computed by obligor
is wrong while looking entirely plausible.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd import api
from sdd.generate.groups import GroupError, assign_members, build_group_table
from sdd.spec import SpecError
from sdd.spec.schema import Group

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    result = api.run(PACK, 500, tmp_path_factory.mktemp("groups"), seed=42)
    return result, pd.read_parquet(result["panel"])


# ---------------------------------------------------------------------------
# the shape
# ---------------------------------------------------------------------------


def test_entities_share_parents(run):
    _, panel = run
    facilities = panel["facility_id"].nunique()
    obligors = panel["obligor_id"].nunique()

    assert obligors < facilities, "every facility has its own obligor; nothing is grouped"
    per_obligor = panel.groupby("obligor_id")["facility_id"].nunique()
    assert per_obligor.max() > 1
    assert per_obligor.min() == 1, "no obligor holds a single facility; the book is not lumpy"


def test_group_attributes_agree_across_members(run):
    """The property the whole feature exists to provide."""
    _, panel = run
    for column in ("industry", "obligor_country", "revenue_eur", "leverage_ratio"):
        spread = panel.groupby("obligor_id")[column].nunique()
        assert spread.max() == 1, (
            f"{column} differs between facilities of the same obligor; the attributes "
            "were generated per entity rather than per group"
        )


def test_a_facility_keeps_its_obligor_for_life(run):
    _, panel = run
    assert panel.groupby("facility_id")["obligor_id"].nunique().max() == 1


def test_the_group_key_reaches_the_output(run):
    """Without the identifier a reader cannot tell which rows share a parent."""
    _, panel = run
    assert "obligor_id" in panel.columns
    assert panel["obligor_id"].notna().all()


def test_bookkeeping_does_not_leak(run):
    _, panel = run
    assert not [c for c in panel.columns if c.startswith("__")]


# ---------------------------------------------------------------------------
# size and caps
# ---------------------------------------------------------------------------


def test_the_member_cap_holds_across_the_whole_run(run):
    """Counted per call it is not a cap.

    A borrower filled to its limit in the opening book would quietly take more
    every time the pool reinvested — measured at 11 members against a cap of 6
    before the running total was carried in the group table.
    """
    _, panel = run
    cap = api.load(PACK).groups[0].size.max_members
    assert cap is not None
    per_obligor = panel.groupby("obligor_id")["facility_id"].nunique()
    assert per_obligor.max() <= cap, f"an obligor holds {per_obligor.max()} against a cap of {cap}"


def test_members_are_lumpy_not_uniform(run):
    """A book where every obligor holds the same number is one no concentration
    limit would ever bite on."""
    _, panel = run
    per_obligor = panel.groupby("obligor_id")["facility_id"].nunique()
    assert per_obligor.nunique() >= 4, "member counts are too uniform to be a real book"


def test_a_cap_that_cannot_hold_the_book_is_refused():
    group = Group(name="g", key="k", count=5, size={"kind": "zipf", "max_members": 2})
    table = build_group_table(api.load(PACK), group, 5, np.random.default_rng(0))
    with pytest.raises(GroupError, match="caps members"):
        assign_members(group, table, 50, np.random.default_rng(0))


# ---------------------------------------------------------------------------
# reinvestment
# ---------------------------------------------------------------------------


def test_later_cohorts_reuse_existing_parents(run):
    """A facility acquired in month twenty may belong to a month-one obligor.

    This is why the group table is carried through ageing rather than rebuilt.
    """
    _, panel = run
    first_seen_facility = panel.groupby("facility_id")["reporting_date"].min()
    first_seen_obligor = panel.groupby("obligor_id")["reporting_date"].min()
    opening = panel["reporting_date"].min()

    acquired = first_seen_facility[first_seen_facility > opening].index
    assert len(acquired) > 50, "too few acquisitions to test"

    their_obligors = panel[panel["facility_id"].isin(acquired)]["obligor_id"].unique()
    reused = [o for o in their_obligors if first_seen_obligor[o] == opening]
    assert reused, (
        "every acquired facility brought a brand-new obligor; the group table is "
        "being rebuilt per cohort instead of carried"
    )


def test_new_parents_also_appear(run):
    _, panel = run
    opening = panel["reporting_date"].min()
    first_seen_obligor = panel.groupby("obligor_id")["reporting_date"].min()
    assert (first_seen_obligor > opening).any(), "no new obligor ever joined"


# ---------------------------------------------------------------------------
# concentration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 7, 42, 99])
def test_the_opening_portfolio_respects_the_concentration_limit(tmp_path_factory, seed):
    """A manager would not buy a book already in breach."""
    result = api.run(
        PACK, 500, tmp_path_factory.mktemp(f"c{seed}"), seed=seed, validate_output=False
    )
    panel = pd.read_parquet(result["panel"])
    opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]

    by_obligor = opening.groupby("obligor_id")["current_par"].sum()
    share = by_obligor / by_obligor.sum()
    assert share.max() <= 0.025, f"largest obligor is {share.max():.2%} of the opening par"


def test_concentration_drifts_up_as_the_pool_runs_down(tmp_path):
    """Deliberately asserting the *opposite* of a limit.

    Concentration rising as a CLO amortises is realistic — the pool shrinks while
    survivors keep their par — and it is what an indenture limit exists to catch.
    Asserting it could never happen would model the world backwards and fail most
    runs. If this test ever fails, the pack has started preventing something it
    should be reporting.
    """
    result = api.run(PACK, 500, tmp_path, seed=42, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    by_period = panel.groupby(["reporting_date", "obligor_id"])["current_par"].sum()
    totals = panel.groupby("reporting_date")["current_par"].sum()
    share = (by_period / totals).groupby("reporting_date").max()

    facilities = panel.groupby("reporting_date")["facility_id"].nunique()
    assert facilities.iloc[-1] < facilities.iloc[0], "the pool never ran down"
    assert share.max() > share.iloc[0], (
        "concentration never rose above its opening level, which means the pool "
        "is not behaving like an amortising portfolio"
    )


# ---------------------------------------------------------------------------
# spec validation
# ---------------------------------------------------------------------------


def test_a_group_needs_exactly_one_of_count_or_ratio():
    with pytest.raises(ValueError, match="exactly one"):
        Group(name="g", key="k", count=10, ratio=0.5)
    with pytest.raises(ValueError, match="exactly one"):
        Group(name="g", key="k")


def test_declaring_a_group_attribute_as_a_column_is_refused():
    """It looks harmless and is not: the entity generator runs first and the join
    overwrites it, so the spec would describe one thing and the data contain
    another."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["columns"].append(
        {"name": "industry", "dtype": "str", "generator": {"kind": "constant", "value": "X"}}
    )
    with pytest.raises(SpecError, match="industry"):
        api.load(spec)


def test_declaring_the_group_key_as_a_column_is_refused():
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["columns"].append(
        {"name": "obligor_id", "dtype": "str", "generator": {"kind": "constant", "value": "X"}}
    )
    with pytest.raises(SpecError, match="obligor_id"):
        api.load(spec)


def test_a_group_cannot_list_its_own_key_as_an_attribute():
    with pytest.raises(ValueError, match="key"):
        Group(
            name="g",
            key="k",
            count=5,
            columns=[
                {"name": "k", "dtype": "str", "generator": {"kind": "constant", "value": "x"}}
            ],
        )


# ---------------------------------------------------------------------------
# genericity
# ---------------------------------------------------------------------------


def test_the_ungrouped_packs_are_untouched(tmp_path):
    """Two packs declare no groups and must behave exactly as before."""
    for pack in ("auto_abs_esma_annex5", "rmbs_nl_green_lion"):
        spec = api.load(pack)
        assert spec.groups == []
        a = api.run(pack, 200, tmp_path / f"{pack}_a", seed=5, validate_output=False)
        b = api.run(pack, 200, tmp_path / f"{pack}_b", seed=5, validate_output=False)
        assert pd.read_parquet(a["panel"]).equals(pd.read_parquet(b["panel"]))


def test_grouping_works_on_a_mortgage_pack(tmp_path):
    """The shape is not CLO-specific.

    Here a household holds one or more mortgages and the household's own
    attributes — its region and income — are shared by every loan it holds.
    """
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 6
    spec["groups"] = [
        {
            "name": "household",
            "key": "household_id",
            "ratio": 0.7,
            "id_format": "HH{seq:06d}",
            "size": {"kind": "zipf", "concentration": 1.5, "max_members": 3},
            "columns": [
                {
                    "name": "household_income_eur",
                    "dtype": "float",
                    "generator": {
                        "kind": "scipy",
                        "dist": "lognorm",
                        "params": {"s": 0.4, "loc": 0.0, "scale": 62000.0},
                        "decimals": 0,
                    },
                },
                {
                    "name": "joint_application_flag",
                    "dtype": "category",
                    "domain": ["Y", "N"],
                    "generator": {
                        "kind": "categorical",
                        "values": ["Y", "N"],
                        "weights": [0.55, 0.45],
                    },
                },
            ],
        }
    ]
    spec["emit"]["column_order"] = None

    result = api.run(spec, 300, tmp_path, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    assert "household_id" in panel.columns
    assert (
        panel["household_id"].nunique()
        < panel[api.load("rmbs_nl_green_lion").entity.id_column].nunique()
    )
    for column in ("household_income_eur", "joint_application_flag"):
        assert panel.groupby("household_id")[column].nunique().max() == 1


def test_a_pack_survives_the_web_api_round_trip():
    """The wizard hands a pack to the browser and takes it back to run it.

    `GET /api/packs/{name}` dumps with `exclude_none=True`. A required field
    holding None is dropped by that and comes back missing, so a pack using
    `constant: null` — the way a column is seeded empty for a later per-period
    derivation to stamp — could be loaded in the wizard and then refused when
    run. Every pack must survive the round trip, not just parse from disk.
    """
    for pack in api.list_packs():
        dumped = api.load(pack).model_dump(mode="json", exclude_none=True, by_alias=True)
        result = api.check(dumped)
        assert result["valid"], f"{pack} does not survive the round trip: {result['problems'][:3]}"
