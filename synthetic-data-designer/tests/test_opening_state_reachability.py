"""A state every entity starts in is reachable, even if nothing transitions into it.

Whole families of lending products begin in a phase they never return to:

    interest-only  -> repayment        (mortgages, CRE)
    deferment      -> repayment        (student loans)
    promotional    -> standard rate    (cards, BNPL)
    revolving      -> amortising       (many ABS structures)

The reachability check counted transition-matrix states and hazard targets, and
not the opening distribution — so it called the phase every loan begins in dead
and refused the spec outright. Nothing could express a product with a first
phase.
"""

from __future__ import annotations

import pytest

from sdd import api
from sdd.spec import SpecError

PACK = "clo_eu_leveraged_loans"


def _with_opening_phase(*, reachable_by_hazard: bool) -> dict:
    """Add a phase every facility starts in and leaves after six periods."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    lc = spec["lifecycle"]
    lc["states"] = ["Ramp", *lc["states"]]
    lc["transition_states"] = [s for s in lc["states"] if s not in lc["terminal"] and s != "Ramp"]
    lc["initial_distribution"] = {"Ramp": 1.0}
    if reachable_by_hazard:
        lc["hazards"] = [
            *lc["hazards"],
            {
                "kind": "condition",
                "name": "ramp_ends",
                "when": "period >= 6",
                "to_state": "Performing",
                "from_states": ["Ramp"],
            },
        ]
    for column in spec["columns"]:
        if column["name"] == lc["state_column"]:
            column["domain"] = ["Ramp", *column["domain"]]
    return spec


def test_a_state_only_the_opening_distribution_reaches_is_accepted():
    result = api.check(_with_opening_phase(reachable_by_hazard=True))
    assert result["valid"], result["problems"]


def test_a_state_nothing_reaches_at_all_is_still_refused():
    """The check must still catch a genuinely dead state.

    Without this the fix would have widened into 'anything named in the
    lifecycle is fine', which is the opposite of useful.
    """
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    lc = spec["lifecycle"]
    lc["states"] = [*lc["states"], "Orphan"]
    for column in spec["columns"]:
        if column["name"] == lc["state_column"]:
            column["domain"] = [*column["domain"], "Orphan"]

    with pytest.raises(SpecError, match="Orphan"):
        api.load(spec)


def test_a_zero_weighted_opening_state_does_not_count():
    """Listed at zero probability, no entity ever starts there."""
    spec = _with_opening_phase(reachable_by_hazard=True)
    lc = spec["lifecycle"]
    lc["initial_distribution"] = {"Ramp": 0.0, "Performing": 1.0}
    lc["hazards"] = [h for h in lc["hazards"] if h.get("name") != "ramp_ends"]

    result = api.check(spec)
    assert not result["valid"]
    assert any("Ramp" in p for p in result["problems"])


def test_the_shipped_packs_are_unaffected():
    for pack in api.list_packs():
        assert api.check(pack)["valid"]


def test_the_opening_mix_actually_sets_the_opening_states(tmp_path):
    """`initial_distribution` must reach the data, not only the rate maths.

    It was documented as "the state mix at period 0" and set nothing: the state
    column's own generator supplied the opening states, and this field was read
    only by the rate calibration. Both shipped packs carried identical numbers in
    the two places, so nothing looked wrong — while a spec whose two disagreed
    got the generator's mix in its data and had its implied default and
    prepayment rates computed against the other one.

    That mattered more once the reachability check above started accepting specs
    whose opening phase exists only in this field.
    """
    import pandas as pd

    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 2
    state_column = spec["lifecycle"]["state_column"]

    for column in spec["columns"]:
        if column["name"] == state_column:
            column["generator"] = {
                "kind": "categorical",
                "values": ["Performing", "Watchlist", "Distressed"],
                "weights": [0.95, 0.03, 0.02],
            }
    spec["lifecycle"]["initial_distribution"] = {
        "Performing": 0.05,
        "Watchlist": 0.05,
        "Distressed": 0.90,
    }

    result = api.run(spec, 2000, tmp_path, seed=1, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]
    share = opening[state_column].value_counts(normalize=True)

    assert share["Distressed"] > 0.85, (
        f"the opening mix declared 90% Distressed and produced "
        f"{share.get('Distressed', 0):.1%}; the column generator is still winning"
    )


def test_the_shipped_packs_opening_mixes_are_unchanged(tmp_path):
    """The two places agreed in every pack, so applying the field changes nothing."""
    import pandas as pd

    for pack in api.list_packs():
        spec = api.load(pack)
        if not (spec.lifecycle and spec.lifecycle.initial_distribution):
            continue
        result = api.run(pack, 400, tmp_path / pack, seed=3, validate_output=False)
        panel = pd.read_parquet(result["panel"])
        opening = panel[panel[spec.entity.time_column] == panel[spec.entity.time_column].min()]
        share = opening[spec.lifecycle.state_column].value_counts(normalize=True)
        for state, declared in spec.lifecycle.initial_distribution.items():
            assert abs(share.get(state, 0.0) - declared) < 0.05, (
                f"{pack}: {state} declared {declared:.1%}, produced {share.get(state, 0):.1%}"
            )
