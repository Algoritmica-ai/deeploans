"""Group attributes that move together.

A group's attributes are drawn one generator at a time — a revenue here, a
margin there — so without something to put the joint structure back, a company's
revenue and its leverage vary independently. A book where the most indebted
borrowers are neither larger nor smaller nor more profitable than anyone else is
one nobody has seen.

The gap opened when the attributes moved onto the group. That move is what makes
an obligor's columns agree *across its facilities*, which is the point; it also
took them out of the entity-level correlation target, where they had been.
"""

from __future__ import annotations

import pathlib
import tempfile

import numpy as np
import pandas as pd
import pytest

from sdd import api
from sdd.profile import build_spec

PACK = "clo_eu_leveraged_loans"
KEY = "obligor_id"
DECLARED = {
    ("revenue_eur", "ebitda_margin_pct"): -0.15,
    ("revenue_eur", "leverage_ratio"): -0.10,
    ("ebitda_margin_pct", "leverage_ratio"): 0.35,
}


def _per_group(panel: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """One row per group, which is the only level this means anything at.

    Measured per entity instead, an obligor with six facilities contributes six
    identical rows — and the correlation ends up weighted by how much each
    company happened to borrow.
    """
    return panel.groupby(KEY, sort=False)[columns].first()


@pytest.fixture(scope="module")
def generated():
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, 800, tmp, seed=3, validate_output=False)
    return pd.read_parquet(result["panel"])


def test_the_pack_generates_correlated_obligors(generated):
    """The declared relationships show up in the output."""
    columns = list({c for pair in DECLARED for c in pair})
    observed = _per_group(generated, columns).corr(method="spearman")

    for (left, right), declared in DECLARED.items():
        got = float(observed.loc[left, right])
        assert got == pytest.approx(declared, abs=0.12), (
            f"{left} x {right}: declared {declared:+.2f}, generated {got:+.3f}"
        )


def test_reordering_does_not_disturb_the_marginals(generated):
    """The property that makes this safe at all.

    Iman-Conover permutes values already drawn, so every range, clip and shape
    the pack declares survives exactly. If correlation were imposed by *changing*
    values instead, the clips below would be the first thing to break.
    """
    opening = generated[generated["reporting_date"] == generated["reporting_date"].min()]
    per_group = _per_group(opening, ["revenue_eur", "ebitda_margin_pct", "leverage_ratio"])

    # The clips declared on the pack's group generators.
    assert per_group["revenue_eur"].between(25_000_000, 12_000_000_000).all()
    assert per_group["ebitda_margin_pct"].between(3.0, 45.0).all()
    assert per_group["leverage_ratio"].between(1.5, 11.0).all()


def test_members_of_a_group_still_agree(generated):
    """Correlation must not have cost the consistency it sits on top of."""
    for column in ("revenue_eur", "ebitda_margin_pct", "leverage_ratio", "industry"):
        assert generated.groupby(KEY)[column].nunique().max() == 1


def test_it_holds_across_reinvestment_cohorts(generated):
    """The bug that made the first version half a feature.

    Reinvestment mints a median of four obligors at a time, and the reordering
    on a table that small is mostly noise — measured at a target of 0.90, five
    rows land 0.85 with a standard deviation of 0.24. The correlation held on
    the opening book and washed out across everything bought afterwards: 0.99
    among opening obligors against 0.80 across the whole book.

    Correlated tables are now drawn in a batch and cut back, so the whole book
    carries the structure and not just the part of it issued on day one.
    """
    columns = ["ebitda_margin_pct", "leverage_ratio"]
    opening_ids = set(
        generated[generated["reporting_date"] == generated["reporting_date"].min()][KEY]
    )
    everything = _per_group(generated, columns)
    later = everything.loc[[i for i in everything.index if i not in opening_ids]]

    if len(later) < 30:
        pytest.skip(f"only {len(later)} obligors arrived later; too few to measure")

    whole = float(everything.corr(method="spearman").iloc[0, 1])
    assert whole == pytest.approx(DECLARED[("ebitda_margin_pct", "leverage_ratio")], abs=0.15)


def test_a_small_table_would_have_been_noise():
    """The measurement behind the batch floor, so the constant is not a guess."""
    from sdd.generate.groups import MIN_CORRELATED_ROWS
    from sdd.generate.randomness import reorder_to_correlation
    from sdd.spec.schema import CorrelationTarget

    target = CorrelationTarget(columns=["a", "b"], matrix=[[1.0, 0.9], [0.9, 1.0]])

    def spread(n: int) -> float:
        got = []
        for seed in range(30):
            rng = np.random.default_rng(seed)
            frame = pd.DataFrame({"a": rng.normal(size=n), "b": rng.normal(size=n)})
            reorder_to_correlation(frame, target, 1.0, rng)
            got.append(frame.corr(method="spearman").iloc[0, 1])
        return float(np.std(got))

    assert spread(5) > 0.15, "five rows should be noisy; if not, the floor is unnecessary"
    assert spread(MIN_CORRELATED_ROWS) < 0.06, "the floor is not high enough to be worth having"


def test_the_slider_still_reaches_it(tmp_path):
    """Group correlation obeys `generation.correlation`, like everything else.

    Two knobs for one idea would be a worse interface than one that reaches
    both: a user turning correlation down expects the whole book to loosen, not
    the entity columns only.
    """
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["generation"]["correlation"] = 0.0

    result = api.run(spec, 600, tmp_path, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    observed = _per_group(panel, ["ebitda_margin_pct", "leverage_ratio"]).corr(method="spearman")
    assert abs(float(observed.iloc[0, 1])) < 0.15


def test_the_profiler_learns_it_back(tmp_path):
    """Round trip: a correlated book must relearn as a correlated spec.

    Without this the structure survives exactly one generation — profile the
    output and the group attributes come back independent, which is where this
    started.
    """
    source = api.run(PACK, 600, tmp_path / "src", seed=3, validate_output=False)
    panel = pd.read_parquet(source["panel"])

    spec, _ = build_spec(
        panel,
        name="relearned",
        id_column="facility_id",
        time_column="reporting_date",
        state_column="credit_state",
    )
    learned = spec.groups[0].correlation_target
    assert learned is not None, "the group came back with no correlation at all"
    assert "leverage_ratio" in learned.columns

    dumped = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    assert api.check(dumped)["valid"]

    regenerated = pd.read_parquet(
        api.run(dumped, 600, tmp_path / "out", seed=9, validate_output=False)["panel"]
    )

    columns = learned.columns
    before = _per_group(panel, columns).corr(method="spearman")
    after = _per_group(regenerated, columns).corr(method="spearman")

    worst = 0.0
    for i, left in enumerate(columns):
        for right in columns[i + 1 :]:
            worst = max(worst, abs(before.loc[left, right] - after.loc[left, right]))
    assert worst < 0.15, f"the strongest relationship moved by {worst:.3f} on the round trip"


def test_the_correlation_is_measured_across_groups_not_entities(tmp_path):
    """The distinction the learner depends on.

    An obligor with six facilities would otherwise contribute six identical
    rows, and the measured correlation would be weighted by how much each
    company happened to borrow rather than describing companies.
    """
    source = api.run(PACK, 600, tmp_path / "src", seed=5, validate_output=False)
    panel = pd.read_parquet(source["panel"])

    spec, _ = build_spec(
        panel,
        name="relearned",
        id_column="facility_id",
        time_column="reporting_date",
        state_column="credit_state",
    )
    columns = spec.groups[0].correlation_target.columns
    learned = pd.DataFrame(spec.groups[0].correlation_target.matrix, index=columns, columns=columns)

    per_group = _per_group(panel, columns).corr(method="spearman")
    per_row = panel[columns].corr(method="spearman")

    left, right = columns[0], columns[-1]
    assert (
        abs(learned.loc[left, right] - per_group.loc[left, right])
        < abs(learned.loc[left, right] - per_row.loc[left, right]) + 1e-9
    )
