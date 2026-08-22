"""Spec validation, mostly through negative controls.

A validator that cannot fail proves nothing, so every check here breaks exactly
one thing in a spec that is otherwise valid and asserts the failure names it.
"""

from __future__ import annotations

import copy

import pytest

from sdd.spec import SpecError, dump_spec, load_spec, load_spec_dict


def test_minimal_spec_is_valid(minimal_spec_dict):
    spec = load_spec_dict(minimal_spec_dict)
    assert spec.meta.name == "mini"
    assert spec.output_columns()[0] == "loan_id"


def test_round_trips_through_yaml(minimal_spec_dict, tmp_path):
    spec = load_spec_dict(minimal_spec_dict)
    path = dump_spec(spec, tmp_path / "spec.yaml")
    again = load_spec(path)
    assert again.model_dump() == spec.model_dump()


def _broken(base: dict, mutate) -> dict:
    out = copy.deepcopy(base)
    mutate(out)
    return out


# ---------------------------------------------------------------------------
# section-level validation
# ---------------------------------------------------------------------------


def test_transition_row_must_sum_to_one(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["lifecycle"].__setitem__("transitions", [[0.95, 0.15], [0.5, 0.5]]),
    )
    with pytest.raises(SpecError, match=r"sums to 1\.100000"):
        load_spec_dict(bad)


def test_transition_matrix_must_be_square_over_the_live_states(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["lifecycle"].__setitem__("transitions", [[1.0], [1.0]]),
    )
    with pytest.raises(SpecError, match="has 1 entries, expected 2"):
        load_spec_dict(bad)


def test_absorbing_state_must_actually_absorb(minimal_spec_dict):
    def mutate(s):
        s["lifecycle"]["absorbing"] = ["Late"]
        s["lifecycle"]["transitions"] = [[0.95, 0.05], [0.5, 0.5]]

    with pytest.raises(SpecError, match="declared absorbing"):
        load_spec_dict(_broken(minimal_spec_dict, mutate))


def test_state_cannot_be_both_absorbing_and_terminal(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict, lambda s: s["lifecycle"].__setitem__("absorbing", ["Redeemed"])
    )
    with pytest.raises(SpecError, match="both absorbing and terminal"):
        load_spec_dict(bad)


def test_bucket_label_count_must_match_edges(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["buckets"]["b"].__setitem__("labels", ["only-one"]),
    )
    with pytest.raises(SpecError, match="needs 2 labels"):
        load_spec_dict(bad)


def test_bucket_edges_must_increase(minimal_spec_dict):
    bad = _broken(minimal_spec_dict, lambda s: s["buckets"]["b"].__setitem__("bins", [0, 500, 100]))
    with pytest.raises(SpecError, match="strictly increasing"):
        load_spec_dict(bad)


def test_categorical_weights_must_match_values(minimal_spec_dict):
    def mutate(s):
        s["columns"][3]["generator"]["weights"] = [0.5]

    with pytest.raises(SpecError, match=r"[23] values but 1 weights"):
        load_spec_dict(_broken(minimal_spec_dict, mutate))


def test_unknown_key_is_rejected(minimal_spec_dict):
    bad = _broken(minimal_spec_dict, lambda s: s["meta"].__setitem__("typo_here", 1))
    with pytest.raises(SpecError):
        load_spec_dict(bad)


def test_hazard_needs_exactly_one_rate(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["lifecycle"]["hazards"][0].__setitem__("period_rate", 0.01),
    )
    with pytest.raises(SpecError, match="exactly one of"):
        load_spec_dict(bad)


# ---------------------------------------------------------------------------
# whole-spec cross-referencing
# ---------------------------------------------------------------------------


def test_unknown_column_in_a_derivation_is_caught(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["derivations"].append({"target": "x", "expr": "no_such_column * 2"}),
    )
    with pytest.raises(SpecError, match="unknown name 'no_such_column'"):
        load_spec_dict(bad)


def test_self_referential_book_derivation_is_caught(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["derivations"].append({"target": "loop", "expr": "loop + 1"}),
    )
    with pytest.raises(SpecError, match="defined in terms of itself"):
        load_spec_dict(bad)


def test_out_of_order_derivation_is_caught(minimal_spec_dict):
    """Using a value produced by a *later* derivation must fail, not silently
    read a stale column."""

    def mutate(s):
        s["derivations"].insert(0, {"target": "early", "expr": "later * 2"})
        s["derivations"].append({"target": "later", "expr": "balance"})

    with pytest.raises(SpecError, match="before they are available"):
        load_spec_dict(_broken(minimal_spec_dict, mutate))


def test_undefined_bucket_reference_is_caught(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["derivations"][1].__setitem__("bucket", "nope"),
    )
    with pytest.raises(SpecError, match="not defined under `buckets`"):
        load_spec_dict(bad)


def test_amortisation_pointing_at_a_missing_column_is_caught(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["dynamics"]["amortisation"].__setitem__("balance", "ghost"),
    )
    with pytest.raises(SpecError, match="unknown column 'ghost'"):
        load_spec_dict(bad)


def test_amortisation_state_must_exist(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["dynamics"]["amortisation"].__setitem__("only_when_state", "Blessed"),
    )
    with pytest.raises(SpecError, match="unknown states"):
        load_spec_dict(bad)


def test_unreachable_state_is_caught(minimal_spec_dict):
    def mutate(s):
        s["lifecycle"]["states"].append("Ghost")
        s["lifecycle"]["terminal"].append("Ghost")

    with pytest.raises(SpecError, match="can never be reached"):
        load_spec_dict(_broken(minimal_spec_dict, mutate))


def test_emit_column_order_must_be_producible(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s["emit"]["column_order"].append("phantom"),
    )
    with pytest.raises(SpecError, match="nothing produces"):
        load_spec_dict(bad)


def test_scenario_shifting_an_unknown_index_is_caught(minimal_spec_dict):
    bad = _broken(
        minimal_spec_dict,
        lambda s: s.__setitem__(
            "scenarios", {"bad": {"name": "bad", "index_shift": {"no_such_index": -0.1}}}
        ),
    )
    with pytest.raises(SpecError, match="unknown index"):
        load_spec_dict(bad)


def test_all_problems_are_reported_at_once(minimal_spec_dict):
    """One run should surface every issue, not make the author fix them singly."""

    def mutate(s):
        s["derivations"].append({"target": "a", "expr": "ghost_one * 2"})
        s["derivations"].append({"target": "b", "expr": "ghost_two * 2"})

    with pytest.raises(SpecError) as exc:
        load_spec_dict(_broken(minimal_spec_dict, mutate))
    assert "ghost_one" in str(exc.value)
    assert "ghost_two" in str(exc.value)


def test_lifecycle_without_transitions_or_hazards_is_caught(minimal_spec_dict):
    def mutate(s):
        del s["lifecycle"]["transitions"]
        s["lifecycle"]["hazards"] = []

    with pytest.raises(SpecError, match="nothing would ever change state"):
        load_spec_dict(_broken(minimal_spec_dict, mutate))


def test_unquoted_yaml_date_is_accepted(tmp_path, minimal_spec_dict):
    """YAML parses a bare 2024-01-31 as a date object, not a string."""
    import yaml

    raw = copy.deepcopy(minimal_spec_dict)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw).replace("start: '2024-01-31'", "start: 2024-01-31"))
    assert load_spec(path).entity.calendar.start == "2024-01-31"


def test_a_counter_on_the_amortising_balance_is_refused(minimal_spec_dict):
    """Two rules writing one column each period is an override, not a merge.

    The ageing loop applies counters before amortisation, so amortisation wins
    and the counter's declared step becomes a claim the validator then reports
    against the engine's own output. Better to refuse the spec.
    """
    raw = copy.deepcopy(minimal_spec_dict)
    raw["dynamics"] = {
        "amortisation": {"kind": "linear", "balance": "balance", "payment": "balance"},
        "counters": [{"column": "balance", "step": -100}],
    }
    with pytest.raises(SpecError, match="already moves that column"):
        load_spec_dict(raw)


def test_a_derivation_may_rewrite_a_column_that_already_has_a_value(minimal_spec_dict):
    """`x = x + 1` is a cycle only when nothing else gives x a value.

    Where x is a sampled column, it is an ordinary rewrite — and it is exactly
    what a stress scenario emits to shift a rate column that was sampled a
    moment earlier. Rejecting it made `scenarios.rate_shift` unusable.
    """
    raw = copy.deepcopy(minimal_spec_dict)
    raw["derivations"].append({"target": "balance", "expr": "balance + 100"})
    spec = load_spec_dict(raw)
    assert spec.derivations[-1].target == "balance"
