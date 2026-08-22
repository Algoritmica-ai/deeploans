"""§21 plausibility: does the book look like the asset class it claims to be?

The invariants ask whether the panel is internally consistent. Every one of them
passes on a portfolio of four-thousand-euro loans to companies in one country,
all rated the same, none of them ever repaying — consistent, and obviously not a
CLO.

§21 asks for **broad plausibility rather than replication**, and says so for a
reason: the bundled pack has no reference tape to score against, and inventing
one would mean shipping vendor-derived parameters, which the same section
forbids. So a band is a declared range with a stated reason, not a distance.

The tests that matter here are the negative controls. A band fitted to whatever
the generator happens to produce can never fail and quietly encodes the pack's
own quirks as "plausible", which is worse than having no band at all.
"""

from __future__ import annotations

import pathlib
import tempfile

import pytest

from sdd import api

PACK = "clo_eu_leveraged_loans"

# The ten characteristics §21 names, plus the CCC share at close, which is the
# one an indenture actually caps.
EXPECTED = {
    "facility_size",
    "spread",
    "market_price",
    "maturity",
    "leverage",
    "rating_mix",
    "ccc_at_close",
    "country_spread",
    "industry_spread",
    "seniority",
    "covenant_lite",
}


@pytest.fixture(scope="module")
def report():
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, 800, tmp, seed=42, validate_output=True)
    return {c["name"]: c for c in result["validation"]["checks"]}


def _bands(report: dict) -> dict:
    return {k.split("::", 1)[1]: v for k, v in report.items() if k.startswith("plausibility::")}


def test_every_characteristic_the_specification_names_is_covered(report):
    """§21 lists ten. Missing one would be invisible otherwise — a band that is
    not declared simply does not appear, and nothing complains."""
    assert set(_bands(report)) == EXPECTED


def test_the_shipped_pack_is_plausible(report):
    """All eleven bands, on the default run."""
    failing = {name: check for name, check in _bands(report).items() if not check["passed"]}
    assert not failing, {
        name: check.get("sample") or check.get("error") for name, check in failing.items()
    }


def test_each_band_reports_the_number_and_the_range(report):
    """A bare pass/fail would be useless here.

    An invariant fails with a row count, which is the right answer for a row
    check. A band is one number against a range, so it carries the number — how
    far outside it landed is the whole content of the finding.
    """
    for name, check in _bands(report).items():
        sample = check.get("sample")
        assert sample, f"{name} reported no measurement"
        measured = sample[0]
        assert "observed" in measured and "expected_between" in measured
        low, high = measured["expected_between"]
        assert low <= measured["observed"] <= high


def test_every_band_says_why(report):
    """A range with no stated reason is a number nobody can challenge."""
    for name, check in _bands(report).items():
        assert len(check["description"].strip()) > 40, f"{name} has no real justification"


# ---------------------------------------------------------------------------
# negative controls — the part that decides whether any of this is worth having
# ---------------------------------------------------------------------------


def test_a_book_of_the_wrong_size_is_caught(tmp_path):
    """Shrink the deal and the facilities stop looking like leveraged loans.

    Nothing else in the suite catches this. Every invariant passes on a book of
    hundred-thousand-euro positions: the balances still tie, the states still
    move, the totals still reconcile.
    """
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["targets"][0]["total"] /= 8

    result = api.run(spec, 800, tmp_path, seed=42, validate_output=True)
    band = next(
        c for c in result["validation"]["checks"] if c["name"] == "plausibility::facility_size"
    )
    assert not band["passed"]
    assert band["sample"][0]["observed"] < 400_000


def test_a_single_country_book_is_caught(tmp_path):
    """A European CLO spans jurisdictions by construction."""
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    column = next(c for g in spec["groups"] for c in g["columns"] if c["name"] == "obligor_country")
    column["generator"] = {"kind": "constant", "value": "France"}
    column["domain"] = ["France"]

    result = api.run(spec, 400, tmp_path, seed=42, validate_output=True)
    band = next(
        c for c in result["validation"]["checks"] if c["name"] == "plausibility::country_spread"
    )
    assert not band["passed"]
    assert band["sample"][0]["observed"] == 1
    assert not result["validation"]["passed"], "a failing band must fail the run's validation"


def test_a_missing_column_is_reported_not_skipped(tmp_path):
    """A band naming a column that is gone must be loud.

    Silently skipping would be the worst outcome: the report would show fewer
    checks, all passing, and nobody counts the checks.
    """
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["validation"]["plausibility"].append(
        {
            "name": "invented",
            "column": "no_such_column",
            "statistic": "median",
            "between": [0.0, 1.0],
            "note": "a band pointed at a column that does not exist in the panel",
        }
    )

    result = api.run(spec, 200, tmp_path, seed=42, validate_output=True)
    band = next(c for c in result["validation"]["checks"] if c["name"] == "plausibility::invented")
    assert not band["passed"]
    assert "no_such_column" in (band["error"] or "")


# ---------------------------------------------------------------------------
# the schema's own guards
# ---------------------------------------------------------------------------


def test_a_share_band_needs_something_to_share_on():
    from sdd.spec.schema import PlausibilityBand

    with pytest.raises(ValueError, match=r"no numerator|names no `where`"):
        PlausibilityBand(name="x", column="c", statistic="share", between=(0.0, 1.0), note="n" * 50)


def test_a_share_band_is_bounded_by_zero_and_one():
    from sdd.spec.schema import PlausibilityBand

    with pytest.raises(ValueError, match=r"bounds belong in \[0, 1\]"):
        PlausibilityBand(
            name="x",
            column="c",
            statistic="share",
            where="c = 'Y'",
            between=(0.0, 40.0),
            note="n" * 50,
        )


def test_an_inverted_band_is_refused():
    from sdd.spec.schema import PlausibilityBand

    with pytest.raises(ValueError, match="lower bound above its upper"):
        PlausibilityBand(name="x", column="c", between=(10.0, 1.0), note="n" * 50)


def test_bands_are_measured_where_the_pack_says(tmp_path):
    """`at_first_cutoff` is not a detail.

    Origination facts belong on the opening book: a facility's size is decided
    once, and pooling it across cut-offs weights it by how long each facility
    happened to survive — so a book whose largest loans prepay first would read
    as smaller than it was written.
    """
    import pandas as pd

    spec = api.load(PACK)
    origination_bands = {b.name for b in spec.validation.plausibility if b.at_first_cutoff}
    assert {"facility_size", "spread", "maturity", "leverage"} <= origination_bands

    result = api.run(PACK, 400, tmp_path, seed=42, validate_output=True)
    panel = pd.read_parquet(result["panel"])
    opening = panel[panel["reporting_date"] == panel["reporting_date"].min()]

    band = next(
        c for c in result["validation"]["checks"] if c["name"] == "plausibility::facility_size"
    )
    assert band["sample"][0]["observed"] == pytest.approx(
        float(opening["original_facility_amount"].median()), rel=1e-6
    )
