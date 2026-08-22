"""A running server older than the pack files it reads.

Python imports the spec model once at start-up and re-reads the pack YAML on
every request. Pull a change that adds a field and a *running* server rejects
its own bundled packs — the files have something the model in memory does not
know about.

This happened twice in one afternoon. The first time the picker listed a pack,
clicking it returned a 500, and nothing said why; the second time the same trap
hid a feature that was working, because the endpoint had been imported before
the code that fills it. Both times the fix was `restart the server`, and both
times the message on offer talked about pydantic.
"""

from __future__ import annotations

import pathlib
import shutil
import tempfile

import pytest
import yaml
from fastapi.testclient import TestClient

from sdd import api
from sdd.web import app as web


@pytest.fixture
def stale(monkeypatch, tmp_path):
    """A packs directory carrying a field this build does not know."""
    staging = tmp_path / "packs"
    staging.mkdir()
    shutil.copy(api.packs_dir() / "clo_eu_leveraged_loans.yaml", staging)

    path = staging / "clo_eu_leveraged_loans.yaml"
    document = yaml.safe_load(path.read_text())
    document["meta"]["a_field_added_after_this_process_booted"] = "hello"
    path.write_text(yaml.safe_dump(document, sort_keys=False))

    monkeypatch.setattr(api, "packs_dir", lambda: staging)
    monkeypatch.setattr(web, "WORKSPACE", pathlib.Path(tempfile.mkdtemp()))
    return TestClient(web.app, raise_server_exceptions=False)


def test_a_healthy_build_reports_nothing():
    assert api.pack_problems() == {}


def test_the_unloadable_pack_is_named(stale):
    problems = stale.get("/api/meta").json()["pack_problems"]
    assert "clo_eu_leveraged_loans" in problems


def test_the_reason_names_the_field_and_the_fix(stale):
    reasons = stale.get("/api/meta").json()["pack_problems"]["clo_eu_leveraged_loans"]
    joined = " ".join(reasons)
    assert "a_field_added_after_this_process_booted" in joined
    assert "Restart it." in joined, "the message explains pydantic and not the fix"


def test_the_endpoint_says_it_rather_than_raising(stale):
    """A 500 with a stack trace is what made this hard to place."""
    response = stale.get("/api/packs/clo_eu_leveraged_loans")
    assert response.status_code == 400
    assert "Restart it." in response.json()["detail"]


def test_an_ordinary_broken_pack_does_not_get_the_restart_hint(monkeypatch, tmp_path):
    """The hint is for one specific cause and must not be advice on everything.

    A transition matrix whose rows do not sum to 1 is a spec someone wrote
    wrongly, and telling them to restart the server would send them looking in
    the wrong place entirely.
    """
    staging = tmp_path / "packs"
    staging.mkdir()
    shutil.copy(api.packs_dir() / "clo_eu_leveraged_loans.yaml", staging)

    path = staging / "clo_eu_leveraged_loans.yaml"
    document = yaml.safe_load(path.read_text())
    document["lifecycle"]["transitions"][0][0] = 0.5
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    monkeypatch.setattr(api, "packs_dir", lambda: staging)

    reasons = api.pack_problems()["clo_eu_leveraged_loans"]
    assert any("sums to" in r for r in reasons)
    assert not any("Restart it." in r for r in reasons)


def test_the_picker_still_lists_it(stale):
    """Shown as unavailable rather than hidden. The picker is where you would go
    to find out something is broken, and an absence explains nothing."""
    meta = stale.get("/api/meta").json()
    assert "clo_eu_leveraged_loans" in meta["packs"]
    assert "clo_eu_leveraged_loans" in meta["pack_problems"]


def test_the_healthy_packs_are_unaffected(stale):
    """One broken pack must not take the others down."""
    meta = stale.get("/api/meta").json()
    healthy = [p for p in meta["packs"] if p not in meta["pack_problems"]]
    for name in healthy:
        assert stale.get(f"/api/packs/{name}").status_code == 200
