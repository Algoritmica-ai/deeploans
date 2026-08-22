"""What a pack calls one row of its opening book.

The interface counts entities in several places — "How many …", the outcome
line, the results summary — and it used to guess the word by matching substrings
of `asset_class` in the browser. Anything the list did not recognise was a
"loan", so a buy-now-pay-later plan and a trade receivable were both loans and
no pack author could do anything about it.

The pack knows and the browser does not, so the pack says.
"""

from __future__ import annotations

import pytest

from sdd import api
from sdd.spec.schema import plural_of


@pytest.mark.parametrize(
    ("singular", "expected"),
    [
        ("facility", "facilities"),
        ("plan", "plans"),
        ("invoice", "invoices"),
        ("loan", "loans"),
        ("lease", "leases"),
        ("account", "accounts"),
        ("box", "boxes"),
        ("batch", "batches"),
        # -ay/-ey/-oy keep the y: "policies" is right, "attornies" is not.
        ("policy", "policies"),
        ("facility", "facilities"),
    ],
)
def test_the_pluraliser_covers_the_nouns_lending_uses(singular, expected):
    assert plural_of(singular) == expected


def test_each_pack_names_its_own_rows():
    expected = {
        "clo_eu_leveraged_loans": "facilities",
        "rmbs_nl_green_lion": "loans",
        "auto_abs_esma_annex5": "contracts",
    }
    for pack, plural in expected.items():
        summary = api.check(pack)["spec"]
        assert summary["entity_noun_plural"] == plural, pack


def test_an_explicit_plural_wins_over_the_rule():
    """For the nouns the rule gets wrong, and there will be some."""
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["meta"]["entity_noun"] = "person"
    spec["meta"]["entity_noun_plural"] = "people"
    assert api.check(spec)["spec"]["entity_noun_plural"] == "people"


def test_a_spec_without_a_noun_says_nothing_rather_than_guessing():
    """A tape profiled into a spec has an asset class and no vocabulary.

    The API returns None and lets the interface fall back, rather than inventing
    a word the pack never chose.
    """
    spec = api.load("rmbs_nl_green_lion").model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["meta"].pop("entity_noun", None)
    spec["meta"].pop("entity_noun_plural", None)
    assert api.check(spec)["spec"]["entity_noun_plural"] is None
