"""The row ceiling a shared deployment runs behind.

Entities are not rows. A row is one entity at one cut-off, so an entity count is
separated from the row count by the number of periods — and by `originations`,
which add entities as the pool ages and live in the spec the browser posts. A
deployment that caps entities has therefore capped nothing in particular, which
is what these tests exist to stop anyone assuming again.

The ceiling is enforced inside the ageing loop against the running count, not
projected from the request, because a projection would have to model
originations, terminal-state exits and the scenario overlay — and a projection
that is wrong is worse than no limit at all, since it reads as one.
"""

from __future__ import annotations

import pytest

from sdd import api
from sdd.age.panel import RowLimitExceeded

PACK = "rmbs_nl_green_lion"


def _spec(periods: int = 6, **originations) -> dict:
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = periods
    if originations:
        spec["originations"] = originations
    return spec


def test_entities_alone_do_not_bound_the_row_count(tmp_path):
    """The premise. Same entity count, two orders of magnitude apart in rows."""
    closed = api.run(_spec(periods=6), 200, tmp_path / "closed", seed=3)
    open_pool = api.run(_spec(periods=24, rate=0.5, fresh=True), 200, tmp_path / "open", seed=3)

    assert open_pool["total_rows"] > 10 * closed["total_rows"], (
        "an identical 200-entity request produced wildly different panels, which is "
        "why the ceiling cannot be expressed in entities"
    )


@pytest.mark.local_only(
    "a caller-supplied `max_rows` governs this process; the deployment enforces its own "
    "ceiling from its own environment, so there is no way to ask it for this one"
)
def test_the_ceiling_stops_a_run_that_would_exceed_it(tmp_path):
    with pytest.raises(RowLimitExceeded) as caught:
        api.run(_spec(periods=6), 500, tmp_path, seed=3, max_rows=1_000)

    message = str(caught.value)
    assert "1,000-row ceiling" in message
    assert "period" in message, "the message says how far it got"


@pytest.mark.local_only(
    "same reason: the ceiling under test is the one passed into `api.run`, and a remote "
    "instance uses the ceiling it was deployed with"
)
def test_originations_cannot_grow_past_the_ceiling(tmp_path):
    """The case an entity cap misses entirely.

    A request for 100 entities looks small. Doubling the pool every period for
    30 of them does not stay small, and the ceiling is what notices.
    """
    with pytest.raises(RowLimitExceeded):
        api.run(
            _spec(periods=30, rate=1.0, fresh=True),
            100,
            tmp_path,
            seed=3,
            max_rows=5_000,
        )


def test_a_run_inside_the_ceiling_is_untouched(tmp_path):
    """The limit must not perturb a run that never approaches it."""
    free = api.run(_spec(periods=6), 200, tmp_path / "free", seed=3)
    capped = api.run(_spec(periods=6), 200, tmp_path / "capped", seed=3, max_rows=10_000_000)

    assert capped["total_rows"] == free["total_rows"]
    assert capped["mix"] == free["mix"]
    assert capped["validation"]["passed"] == free["validation"]["passed"]


def test_no_ceiling_by_default(tmp_path):
    """Local use is uncapped: nobody generating on their own laptop is policed."""
    result = api.run(_spec(periods=6), 200, tmp_path, seed=3)
    assert result["total_rows"] > 0


def test_entities_times_periods_overstates_a_closed_pool(tmp_path):
    """Why the ceiling cannot be pre-checked as `entities x periods`.

    Entities reaching a terminal state are dropped and stop producing rows, so
    the product is an upper bound on a closed pool. Treating it as a lower one
    refuses runs that would have fitted.
    """
    result = api.run(_spec(periods=24), 500, tmp_path, seed=3)

    assert result["total_rows"] < 500 * 24
    assert result["surviving_entities"] < 500


def test_the_count_is_rows_written_not_surviving_entities(tmp_path):
    """`total_rows` is what the ceiling is compared against, so pin its meaning."""
    result = api.run(_spec(periods=6), 300, tmp_path, seed=3)

    assert result["total_rows"] == sum(period["rows"] for period in result["mix"])
    assert result["total_rows"] > result["surviving_entities"]
