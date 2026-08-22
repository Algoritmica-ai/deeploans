"""Columns that are empty at the first cut-off and fill in later.

Generators are fitted to the first cut-off, because what they produce is the
opening book. A column that is blank on day one therefore arrives as an empty
sample — and every fitting branch assumed it would not be. `fit_categorical`
refused to build from nothing and the whole profile run died on the column.

Event dates are the ordinary case: a loan has no default date on day one and
acquires one the month it defaults. Any pack using that pattern — every pack the
CLO work introduced — broke the profiler outright.
"""

from __future__ import annotations

import pandas as pd
import pytest

from sdd import api
from sdd.profile import build_spec, profile_dataset


def _panel_with_a_late_filling_column() -> pd.DataFrame:
    """Two cut-offs; `event_date` is blank in the first and set in the second."""
    rows = []
    for period, date in enumerate(("2026-01-31", "2026-02-28")):
        for i in range(200):
            rows.append(
                {
                    "loan_id": f"L{i:04d}",
                    "as_of": date,
                    "balance": 100000 - i * 10 - period * 500,
                    "status": "Current" if i % 7 else "Late",
                    "event_date": None if period == 0 else (date if i % 5 == 0 else None),
                }
            )
    return pd.DataFrame(rows)


def test_profiling_survives_a_column_blank_at_the_first_cut_off():
    frame = _panel_with_a_late_filling_column()
    assert frame[frame["as_of"] == "2026-01-31"]["event_date"].notna().sum() == 0
    assert frame["event_date"].notna().sum() > 0

    profile = profile_dataset(frame, id_column="loan_id", time_column="as_of")
    event = next(c for c in profile.columns if c.name == "event_date")
    assert event.fit is not None


def test_such_a_column_generates_blank_which_is_right_for_day_one():
    """The opening book genuinely has no value there."""
    frame = _panel_with_a_late_filling_column()
    profile = profile_dataset(frame, id_column="loan_id", time_column="as_of")
    event = next(c for c in profile.columns if c.name == "event_date")

    assert event.fit.generator.kind == "constant"
    assert event.fit.generator.value is None


def test_the_profile_says_a_rule_is_missing():
    """What is lost is that something fills it later, and period 0 cannot say what.

    Confidence is deliberately low and the note explicit, because a silently
    always-blank column is the failure mode this replaces a crash with.
    """
    frame = _panel_with_a_late_filling_column()
    profile = profile_dataset(frame, id_column="loan_id", time_column="as_of")
    event = next(c for c in profile.columns if c.name == "event_date")

    assert event.fit.confidence < 0.5
    assert "filled in later" in (event.fit.note or "")


def test_the_domain_comes_from_the_whole_panel():
    """Otherwise the validator would reject the values that do arrive."""
    frame = _panel_with_a_late_filling_column()
    profile = profile_dataset(frame, id_column="loan_id", time_column="as_of")
    event = next(c for c in profile.columns if c.name == "event_date")
    assert event.domain, "no domain was recorded, so later values have nothing to validate against"


def test_an_entirely_empty_column_is_left_alone():
    """Blank everywhere is a different case and must not take this path."""
    frame = _panel_with_a_late_filling_column()
    frame["never_set"] = None
    profile = profile_dataset(frame, id_column="loan_id", time_column="as_of")
    never = next(c for c in profile.columns if c.name == "never_set")
    assert never.fit is not None


@pytest.mark.parametrize("pack", ["clo_eu_leveraged_loans", "rmbs_nl_green_lion"])
def test_a_generated_panel_can_be_profiled_back(tmp_path, pack):
    """The round trip the whole spec-driven design rests on.

    The CLO pack carries four columns blank at the first cut-off — `default_date`,
    `recovery_date`, `sale_date`, `par_at_default` — and profiling it raised
    ValidationError before this.
    """
    result = api.run(pack, 300, tmp_path / pack, seed=3, validate_output=False)
    panel = pd.read_parquet(result["panel"])
    spec = api.load(pack)

    learned, _ = build_spec(
        panel,
        name="relearned",
        id_column=spec.entity.id_column,
        time_column=spec.entity.time_column,
        state_column=spec.lifecycle.state_column,
    )
    assert len(learned.columns) > 20
