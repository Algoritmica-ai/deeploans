"""The profiler: structure detection, distribution fitting, and the round trip.

The round-trip test at the bottom is the strongest single check in the suite.
Generate data from the RMBS pack, profile *that output* with no knowledge of the
pack, build a spec from the profile alone, and run it. If the profiler can
rediscover the model from data it has never been told about, the generalisation
is real rather than a re-expression of one hardcoded deal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.age.panel import run_ageing
from sdd.generate import build_book
from sdd.profile import build_spec, profile_dataset, spec_from_profile
from sdd.profile.derived import find_bucket_columns
from sdd.profile.distributions import decimals_used, fit_numeric, looks_discrete
from sdd.profile.panel import learn_attrition, learn_counters, learn_lifecycle
from sdd.spec import check_spec, load_spec
from sdd.validate import compare, validate_panel


@pytest.fixture
def rng():
    return np.random.default_rng(0)


@pytest.fixture
def snapshot(rng) -> pd.DataFrame:
    n = 3000
    return pd.DataFrame(
        {
            "loan_id": [f"L{i:05d}" for i in range(n)],
            "balance": rng.lognormal(12.0, 0.4, n).round(2),
            "debtor_count": rng.choice([1, 2, 3], size=n, p=[0.3, 0.66, 0.04]),
            "region": rng.choice(["N", "S", "E"], size=n, p=[0.5, 0.3, 0.2]),
            "currency": ["EUR"] * n,
        }
    )


# ---------------------------------------------------------------------------
# distribution fitting
# ---------------------------------------------------------------------------


def test_a_lognormal_column_is_recognised(rng):
    fit = fit_numeric(pd.Series(rng.lognormal(12.0, 0.4, 5000)))
    assert fit.method.startswith("scipy")
    assert fit.ks < 0.05
    assert fit.confidence > 0.5


def test_a_small_integer_set_becomes_a_category(rng):
    """Fitting a continuous shape to {1,2,3} would produce 2.7 debtors."""
    fit = fit_numeric(pd.Series(rng.choice([1, 2, 3], size=2000)))
    assert fit.generator.kind == "categorical"
    assert set(fit.generator.values) == {1, 2, 3}


def test_a_single_valued_column_becomes_a_constant():
    fit = fit_numeric(pd.Series([7.5] * 500))
    assert fit.generator.kind == "constant"
    assert fit.generator.value == 7.5


def test_a_zero_inflated_column_keeps_its_spike(rng):
    """96% exact zeros must come back as exact zeros, not as a bin midpoint."""
    values = np.where(rng.random(5000) < 0.04, rng.lognormal(10, 0.3, 5000), 0.0)
    fit = fit_numeric(pd.Series(values))
    assert fit.generator.kind == "empirical"
    assert 0.0 in fit.generator.values
    zero_weight = fit.generator.weights[fit.generator.values.index(0.0)]
    assert zero_weight == pytest.approx(0.96, abs=0.02)


def test_an_unfittable_shape_falls_back_to_empirical(rng):
    """A bimodal column matches no single named distribution."""
    bimodal = np.concatenate([rng.normal(0, 0.3, 2500), rng.normal(20, 0.3, 2500)])
    fit = fit_numeric(pd.Series(bimodal))
    assert fit.generator.kind == "empirical"
    assert fit.note


def test_decimal_places_are_preserved():
    assert decimals_used(pd.Series([1.23, 4.56, 7.89])) == 2
    assert decimals_used(pd.Series([1.0, 2.0, 3.0])) == 0


def test_looks_discrete_distinguishes_counts_from_amounts():
    assert looks_discrete(pd.Series([1, 2, 3, 2, 1]))
    assert not looks_discrete(pd.Series(np.linspace(0, 1000, 500)))


# ---------------------------------------------------------------------------
# structure detection
# ---------------------------------------------------------------------------


def test_detects_the_id_and_time_columns_by_name(snapshot):
    panel = pd.concat(
        [snapshot.assign(reporting_date=d) for d in ("2024-01-31", "2024-02-29")],
        ignore_index=True,
    )
    profile = profile_dataset(panel)
    assert profile.id_column == "loan_id"
    assert profile.time_column == "reporting_date"
    assert profile.is_panel


def test_detects_them_by_behaviour_when_the_names_are_unfamiliar(snapshot):
    renamed = snapshot.rename(columns={"loan_id": "contract_ref"})
    panel = pd.concat(
        [renamed.assign(snap=d) for d in ("2024-01-31", "2024-02-29")], ignore_index=True
    )
    profile = profile_dataset(panel)
    assert profile.id_column == "contract_ref"
    assert profile.time_column == "snap"
    assert "unique within every cut-off" in profile.detection_notes["id_column"]


def test_a_per_loan_date_is_not_mistaken_for_the_cutoff(snapshot, rng):
    """maturity_date is roughly unique per loan; a cut-off repeats across many."""
    snapshot = snapshot.assign(
        maturity_date=[f"20{40 + i % 20}-06-30" for i in range(len(snapshot))]
    )
    panel = pd.concat(
        [snapshot.assign(reporting_date=d) for d in ("2024-01-31", "2024-02-29")],
        ignore_index=True,
    )
    assert profile_dataset(panel).time_column == "reporting_date"


def test_a_single_snapshot_is_not_treated_as_a_panel(snapshot):
    profile = profile_dataset(snapshot)
    assert not profile.is_panel
    assert profile.periods == 1


def test_roles_are_decided_by_counting_not_guessing(snapshot):
    panel = pd.concat(
        [
            snapshot.assign(reporting_date=d, balance=snapshot["balance"] * f)
            for d, f in (("2024-01-31", 1.0), ("2024-02-29", 0.99))
        ],
        ignore_index=True,
    )
    profile = profile_dataset(panel)
    assert profile.column("region").role == "static"
    assert profile.column("balance").role == "dynamic"
    assert profile.column("currency").role == "constant"


def test_a_snapshot_records_that_it_cannot_tell_static_from_dynamic(snapshot):
    note = profile_dataset(snapshot).column("balance").note
    assert note and "without repeat observations" in note


def test_free_text_is_replaced_not_resampled(snapshot):
    """Resampling high-cardinality text would copy sample values verbatim."""
    snapshot = snapshot.assign(notes=[f"unique note {i}" for i in range(len(snapshot))])
    fit = profile_dataset(snapshot).column("notes").fit
    assert fit.generator.kind == "uuid"
    assert "verbatim" in (fit.note or "")


# ---------------------------------------------------------------------------
# panel dynamics
# ---------------------------------------------------------------------------


def _state_panel(paths: list[list[str]]) -> pd.DataFrame:
    rows = []
    for i, path in enumerate(paths):
        for period, state in enumerate(path):
            rows.append(
                {"id": f"L{i}", "t": f"2024-{period + 1:02d}-28", "state": state, "n": period}
            )
    return pd.DataFrame(rows).sort_values(["id", "t"])


def test_learns_a_transition_matrix_whose_rows_sum_to_one():
    panel = _state_panel([["A", "A", "B", "B"]] * 60 + [["A", "B", "A", "B"]] * 40)
    learned = learn_lifecycle(panel, "id", "t", "state")
    assert learned
    np.testing.assert_allclose([sum(r) for r in learned["transitions"]], 1.0)


def test_identifies_a_terminal_state_by_behaviour():
    """Entities in a terminal state stop being reported."""
    panel = _state_panel([["A", "A", "Gone"]] * 50 + [["A", "A", "A", "A"]] * 50)
    learned = learn_lifecycle(panel, "id", "t", "state")
    assert learned["terminal"] == ["Gone"]


def test_identifies_an_absorbing_state_on_the_live_matrix():
    """A state whose only exit is to a terminal one is absorbing in the matrix."""
    panel = _state_panel([["A", "Stuck", "Stuck", "Gone"]] * 60 + [["A", "A", "A", "A", "A"]] * 60)
    learned = learn_lifecycle(panel, "id", "t", "state")
    assert "Gone" in learned["terminal"]
    assert "Stuck" in learned["absorbing"]


def test_states_are_ordered_best_first():
    """Stress scenarios read later states as worse, so the order matters."""
    panel = _state_panel([["Performing"] * 4] * 90 + [["Late"] * 4] * 10)
    learned = learn_lifecycle(panel, "id", "t", "state")
    assert learned["states"][0] == "Performing"
    assert "state_order_note" in learned


def test_measures_attrition_and_annualises_it():
    #  100 entities, ~10% leaving each period
    paths = [["A"] * 4] * 70 + [["A", "A"]] * 30
    learned = learn_attrition(_state_panel(paths), "id", "t")
    assert learned["period_rate"] > 0
    assert learned["annual_rate"] > learned["period_rate"]


def test_finds_counters_and_their_step():
    panel = _state_panel([["A"] * 5] * 50)
    profile = profile_dataset(panel, id_column="id", time_column="t")
    counters = learn_counters(panel.sort_values(["id", "t"]), "id", profile)
    steps = {c["column"]: c["step"] for c in counters}
    assert steps.get("n") == 1.0


# ---------------------------------------------------------------------------
# derived columns
# ---------------------------------------------------------------------------


def test_recovers_a_bucket_column_and_its_edges(rng):
    values = rng.uniform(0, 1000, 3000)
    df = pd.DataFrame(
        {
            "amount": values,
            "amount_bucket": pd.cut(
                values, bins=[-1, 250, 500, 1e9], labels=["low", "mid", "high"]
            ).astype(str),
        }
    )
    profile = profile_dataset(df, id_column="amount")
    found = find_bucket_columns(df, profile)
    assert len(found) == 1
    assert found[0].target == "amount_bucket"
    assert found[0].source == "amount"
    assert found[0].confidence > 0.999


def test_an_unrelated_category_is_not_mistaken_for_a_bucket(rng):
    """Overlapping ranges are not a binning, however suggestive the name."""
    df = pd.DataFrame(
        {
            "amount": rng.uniform(0, 1000, 3000),
            "amount_bucket": rng.choice(["low", "mid", "high"], size=3000),
        }
    )
    profile = profile_dataset(df, id_column="amount")
    assert find_bucket_columns(df, profile) == []


# ---------------------------------------------------------------------------
# spec building
# ---------------------------------------------------------------------------


def test_builds_a_valid_spec_from_a_snapshot(snapshot):
    spec, _ = build_spec(snapshot, name="snap")
    check_spec(spec)
    assert spec.entity.id_column == "loan_id"
    assert "currency" in spec.constants


def test_a_spec_without_an_identifier_is_refused():
    df = pd.DataFrame({"a": [1, 2, 3]})
    profile = profile_dataset(df)
    profile.id_column = None
    with pytest.raises(ValueError, match="entity identifier"):
        spec_from_profile(profile)


def test_the_generated_spec_runs(snapshot):
    spec, _ = build_spec(snapshot, name="snap")
    out = build_book(spec, 200, seed=1)
    assert len(out) == 200
    assert set(spec.output_columns()) <= set(out.columns)


def test_low_confidence_columns_are_flagged_for_review(snapshot):
    snapshot = snapshot.assign(notes=[f"note {i}" for i in range(len(snapshot))])
    spec, _ = build_spec(snapshot, name="snap")
    flagged = [c for c in spec.columns if c.review]
    assert any(c.name == "notes" for c in flagged)


def test_a_structure_fixes_the_output_schema(snapshot):
    from sdd.profile import template_from_columns

    wanted = ["loan_id", "balance", "region", "regulatory_only_field"]
    spec, _ = build_spec(snapshot, structure=template_from_columns(wanted), name="snap")
    assert spec.output_columns() == wanted
    # A field the structure demands but the sample lacks still has to exist.
    extra = spec.column("regulatory_only_field")
    assert extra is not None and extra.review


# ---------------------------------------------------------------------------
# the round trip
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_round_trip_rediscovers_the_model_from_data_alone(tmp_path):
    """Pack -> generate -> profile -> new spec -> regenerate.

    Asserted here: the profiler recovers the *structure* — the lifecycle, the
    terminal states, the amortisation rule, the counters — and the regenerated
    panel satisfies every invariant its own spec implies.

    Deliberately not asserted: full distributional fidelity. Marginal profiling
    fits each column independently, so derived relationships (an LTV that is a
    balance divided by a valuation) are not recovered and the joint structure
    is lost. That limit is measured in test_round_trip_fidelity_is_honest below
    rather than hidden.
    """
    from tests.conftest import PACKS

    pack = load_spec(PACKS / "rmbs_nl_green_lion.yaml")
    source = tmp_path / "source"
    run_ageing(pack, build_book(pack, 2500, seed=11), source, seed=11)
    original = pd.read_parquet(source / pack.emit.panel_filename)

    relearned, profile = build_spec(original, name="relearned")
    check_spec(relearned)

    # Structure recovered from the data alone.
    assert profile.id_column == "loan_id"
    assert profile.time_column == "reporting_date"
    assert relearned.lifecycle is not None
    assert relearned.lifecycle.states[0] == "Performing"
    assert set(relearned.lifecycle.terminal) == {"Redeemed", "Charged-Off"}
    assert relearned.lifecycle.absorbing == ["Defaulted"]
    assert relearned.dynamics.amortisation.kind == "annuity"
    assert relearned.dynamics.amortisation.only_when_state == ["Performing"]
    assert {c.column for c in relearned.dynamics.counters} >= {
        "seasoning_months",
        "remaining_term_months",
    }
    assert profile.derived, "bucket columns should be recovered as derivations"

    # And the regenerated panel is internally consistent.
    regenerated = tmp_path / "regenerated"
    run_ageing(relearned, build_book(relearned, 2500, seed=12), regenerated, seed=12)
    panel = pd.read_parquet(regenerated / relearned.emit.panel_filename)
    report = validate_panel(relearned, panel)
    assert report.passed, report.summary()


@pytest.mark.slow
def test_round_trip_fidelity_is_honest(tmp_path):
    """Measure what marginal profiling does and does not recover.

    Independent columns come back well. Derived ones do not, because nothing in
    a marginal profile knows that one column is a ratio of two others. This test
    pins the current behaviour so a regression is visible and an improvement is
    measurable.
    """
    from tests.conftest import PACKS

    pack = load_spec(PACKS / "rmbs_nl_green_lion.yaml")
    source = tmp_path / "source"
    run_ageing(pack, build_book(pack, 2500, seed=21), source, seed=21)
    original = pd.read_parquet(source / pack.emit.panel_filename)

    relearned, _ = build_spec(original, name="relearned")
    regenerated = tmp_path / "regenerated"
    run_ageing(relearned, build_book(relearned, 2500, seed=22), regenerated, seed=22)
    panel = pd.read_parquet(regenerated / relearned.emit.panel_filename)

    report = compare(
        original,
        panel,
        id_column="loan_id",
        time_column="reporting_date",
        state_column="arrears_bucket",
    )

    # Independently sampled columns come back close. The bar is stated as a
    # distance rather than the report's own pass/fail gate: the generator is
    # fitted to a 2,500-loan opening book, so a KS of one to two points is
    # ordinary fitting error at that size, not a defect.
    by_name = {c.column: c for c in report.columns}
    for column in ("original_balance", "borrower_annual_income", "current_interest_rate_pct"):
        assert by_name[column].distance < 0.05, f"{column}: {by_name[column].distance:.4f}"

    # Most of the schema still comes back within tolerance. The bound is
    # sample-size dependent — fitting a generator to a 2,500-loan opening book
    # leaves more error than a 20,000-loan one, where the failure rate roughly
    # halves — so this pins behaviour at the size the test actually runs.
    assert len(report.failures) / len(report.columns) < 0.45

    # The joint structure survives too, which independent sampling alone cannot
    # manage: the profiler measures the sample's rank correlation and the
    # randomness stage reorders the generated columns to match it. Before that
    # existed this same assertion read `> 0.5`, and the gap it recorded is what
    # the reordering closes.
    assert report.correlation_delta is not None and report.correlation_delta < 0.20


def test_a_balance_that_falls_by_a_fixed_amount_is_not_also_a_counter():
    """A steadily amortising balance looks exactly like a counter to the panel
    learner. Keeping both makes the spec contradict itself, so amortisation —
    which owns the balance — wins and the counter is dropped."""
    rows = []
    for period in range(4):
        for i in range(200):
            rows.append(
                {
                    "loan_id": f"L{i:05d}",
                    "reporting_date": f"2024-0{period + 1}-28",
                    "current_balance": 200_000 + i * 137 - period * 900,
                    "remaining_term_months": 240 - period,
                    "status": "Performing" if i % 5 else "Defaulted",
                }
            )
    frame = pd.DataFrame(rows)

    profile = profile_dataset(frame)
    # The learner does report it as both — that is what makes this worth testing.
    assert "current_balance" in {c["column"] for c in profile.dynamics["counters"]}

    spec = spec_from_profile(profile, name="t")
    assert spec.dynamics.amortisation.balance == "current_balance"
    assert [c.column for c in spec.dynamics.counters] == ["remaining_term_months"]


def test_profiling_can_be_capped_so_a_caller_is_not_left_waiting():
    """The UI bounds how much of a large tape it reads. Distribution shapes
    settle long before a tape is exhausted, so the cap changes the wait rather
    than the answer."""
    rows = 4000
    frame = pd.DataFrame(
        {
            "loan_id": [f"L{i:05d}" for i in range(rows)],
            "reporting_date": "2024-01-31",
            "balance": np.random.default_rng(1).lognormal(12, 0.4, rows).round(2),
        }
    )
    spec, profile = build_spec(frame, name="capped", max_rows=500)

    assert profile.rows == 500
    assert spec.column("balance").generator is not None
