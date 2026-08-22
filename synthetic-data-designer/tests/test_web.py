"""The web layer.

Endpoints are thin, so most of these check the *contract* the browser relies on:
a bad spec comes back as a 200 with readable problems rather than a 500, runs are
asynchronous and pollable, and a filename arriving from a browser cannot reach
outside the workspace.
"""

from __future__ import annotations

import io
import json
import time

import pandas as pd
import pytest

pytest.importorskip("fastapi", reason="the web UI needs the [web] extra")

from fastapi.testclient import TestClient

from sdd import api
from sdd.web import app as web

PACK = "rmbs_nl_green_lion"


@pytest.fixture
def client(tmp_path, monkeypatch):
    """A client whose workspace is a throwaway directory."""
    monkeypatch.setattr(web, "WORKSPACE", tmp_path / "workspace")
    return TestClient(web.app)


def wait_for(client, job_id: str, timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/api/run/{job_id}").json()
        if job["status"] in ("done", "error"):
            return job
        time.sleep(0.15)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


# ---------------------------------------------------------------------------
# static and metadata
# ---------------------------------------------------------------------------


def test_the_page_is_served(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Synthetic Data Designer" in response.text


def test_meta_lists_the_packs(client):
    payload = client.get("/api/meta").json()
    assert PACK in payload["packs"]
    assert payload["version"]


def test_a_pack_comes_back_as_editable_json(client):
    payload = client.get(f"/api/packs/{PACK}").json()
    assert payload["spec"]["lifecycle"]["state_column"] == "arrears_bucket"
    assert payload["summary"]["columns"] == 71
    # The editor binds directly to this, so it has to survive a JSON round trip.
    assert json.loads(json.dumps(payload["spec"])) == payload["spec"]


def test_an_unknown_pack_is_a_404(client):
    assert client.get("/api/packs/nope").status_code == 404


# ---------------------------------------------------------------------------
# spec editing
# ---------------------------------------------------------------------------


def test_check_accepts_a_good_spec(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    payload = client.post("/api/check", json=spec).json()
    assert payload["valid"]
    assert payload["problems"] == []


def test_check_returns_readable_problems_not_a_500(client):
    """The UI shows problems[0] as a headline, so it has to be the real message."""
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    spec["lifecycle"]["transitions"][0][0] = 0.85

    response = client.post("/api/check", json=spec)
    assert response.status_code == 200
    payload = response.json()
    assert not payload["valid"]
    assert "sums to 0.857500" in payload["problems"][0]
    # None of pydantic's plumbing should reach the browser.
    joined = " ".join(payload["problems"])
    assert "[type=" not in joined
    assert "validation error for" not in joined
    assert "pydantic.dev" not in joined


def test_yaml_round_trips_through_the_escape_hatch(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    text = client.post("/api/spec/yaml", json=spec).json()["yaml"]
    assert "lifecycle:" in text

    parsed = client.post("/api/spec/parse", json={"yaml": text}).json()
    assert parsed["valid"], parsed["problems"]
    assert parsed["parsed"]["meta"]["name"] == spec["meta"]["name"]


def test_malformed_yaml_is_reported_not_raised(client):
    payload = client.post("/api/spec/parse", json={"yaml": "key: [unclosed"}).json()
    assert not payload["valid"]
    assert "YAML did not parse" in payload["problems"][0]


def test_yaml_that_is_not_a_mapping_is_rejected(client):
    payload = client.post("/api/spec/parse", json={"yaml": "- just\n- a list"}).json()
    assert not payload["valid"]
    assert "not a mapping" in payload["problems"][0]


# ---------------------------------------------------------------------------
# upload and analysis
# ---------------------------------------------------------------------------


def _tape_csv(rows: int = 400) -> bytes:
    frame = pd.concat(
        [
            pd.DataFrame(
                {
                    "loan_id": [f"L{i:05d}" for i in range(rows)],
                    "reporting_date": date,
                    "balance": [100000 + i * 7 for i in range(rows)],
                    "region": ["N", "S"] * (rows // 2),
                    "status": ["Current"] * rows,
                }
            )
            for date in ("2024-01-31", "2024-02-29", "2024-03-31")
        ]
    )
    return frame.to_csv(index=False).encode()


def _upload(client, name: str, body: bytes, kind: str = "sample") -> dict:
    response = client.post(
        "/api/upload",
        files={"file": (name, io.BytesIO(body), "application/octet-stream")},
        data={"kind": kind},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_upload_then_analyse_produces_a_runnable_spec(client):
    stored = _upload(client, "tape.csv", _tape_csv())
    assert "loan_id" in stored["columns"]

    analysed = client.post(
        "/api/analyse", json={"sample_file": stored["file"], "name": "uploaded"}
    ).json()
    assert analysed["profile"]["id_column"] == "loan_id"
    assert analysed["profile"]["time_column"] == "reporting_date"
    assert client.post("/api/check", json=analysed["spec"]).json()["valid"]
    # The wizard's next step binds to this, so it comes back with the analysis
    # rather than needing a second round trip.
    assert analysed["schema"]["primary_key"] == "loan_id"
    assert analysed["source"]


def test_a_schema_alone_is_enough_to_build_a_configuration(client):
    """The schema is the only required upload, so it has to work on its own."""
    header = b"contract_id,as_of_date,exposure,rating\n"
    stored = _upload(client, "schema.csv", header, kind="schema")
    assert stored["columns"] == ["contract_id", "as_of_date", "exposure", "rating"]

    analysed = client.post("/api/analyse", json={"schema_file": stored["file"]}).json()
    assert analysed["profile"] is None
    assert client.post("/api/check", json=analysed["spec"]).json()["valid"]
    # Nothing was measured, so every column says so rather than pretending.
    assert len(analysed["needs_review"]) == 4


def test_analysing_nothing_is_refused(client):
    response = client.post("/api/analyse", json={})
    assert response.status_code == 400
    assert "upload a schema" in response.json()["detail"]


def test_excel_and_json_uploads_are_accepted(client, tmp_path):
    """All four advertised formats reach the profiler, not just the two native ones."""
    frame = pd.read_csv(io.BytesIO(_tape_csv()))

    excel = tmp_path / "tape.xlsx"
    frame.to_excel(excel, index=False)
    assert _upload(client, "tape.xlsx", excel.read_bytes())["columns"][0] == "loan_id"

    body = frame.to_json(orient="records").encode()
    assert "balance" in _upload(client, "tape.json", body)["columns"]


def test_an_unsupported_extension_is_refused(client):
    response = client.post(
        "/api/upload",
        files={"file": ("notes.txt", io.BytesIO(b"hello"), "text/plain")},
        data={"kind": "sample"},
    )
    assert response.status_code == 400
    assert "csv" in response.json()["detail"]


def test_an_unreadable_file_is_refused_and_not_kept(client):
    response = client.post(
        "/api/upload",
        files={"file": ("broken.parquet", io.BytesIO(b"not parquet"), "application/octet-stream")},
    )
    assert response.status_code == 400
    uploads = web.WORKSPACE / "uploads"
    assert not uploads.exists() or not list(uploads.iterdir())


# ---------------------------------------------------------------------------
# schema review and configuration
# ---------------------------------------------------------------------------


def test_the_schema_review_reports_what_was_detected(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    review = client.post("/api/schema", json={"spec": spec}).json()

    assert review["primary_key"] == "loan_id"
    assert review["time_column"] == "reporting_date"
    assert "reporting_date" in review["date_columns"]
    assert review["counts"]["columns"] == len(spec["columns"])
    assert {"name", "dtype", "role", "required", "primary_key"} <= set(review["columns"][0])


def test_renaming_a_column_rewrites_every_reference_to_it(client):
    """A rename that only touched the column definition would break the spec."""
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    result = client.post(
        "/api/schema/edit",
        json={"spec": spec, "edits": [{"original": "current_balance", "rename": "outstanding"}]},
    ).json()

    assert result["valid"], result["problems"]
    edited = result["spec"]
    assert edited["dynamics"]["amortisation"]["balance"] == "outstanding"
    assert "outstanding" in edited["emit"]["column_order"]
    assert "current_balance" not in json.dumps(edited)


def test_marking_a_column_optional_lets_missing_values_reach_it(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    before = client.post(
        "/api/configure", json={"spec": spec, "missing": 0.2, "from_base": False}
    ).json()
    assert before["optional_columns"] == []
    assert any("every column is marked required" in n for n in before["notes"])

    edited = client.post(
        "/api/schema/edit",
        json={"spec": spec, "edits": [{"original": "borrower_annual_income", "required": False}]},
    ).json()["spec"]
    after = client.post(
        "/api/configure", json={"spec": edited, "missing": 0.2, "from_base": False}
    ).json()
    assert after["optional_columns"] == ["borrower_annual_income"]


def test_configuring_rates_rewrites_the_matrix_and_says_so(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    result = client.post(
        "/api/configure",
        json={"spec": spec, "default_rate": 0.05, "recovery_rate": 0.4, "from_base": False},
    ).json()

    assert result["valid"], result["problems"]
    assert abs(result["rates"]["default_rate"] - 0.05) < 0.005
    assert result["rates"]["recovery_rate"] == 0.4
    # Recovery has to be recorded somewhere for the number to mean anything.
    assert result["spec"]["dynamics"]["recovery"]["target"] == "recovery_amount"
    assert result["spec"]["lifecycle"]["transitions"] != spec["lifecycle"]["transitions"]


def test_a_method_change_is_applied_to_the_spec_itself(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    result = client.post(
        "/api/configure", json={"spec": spec, "method": "rule_based", "from_base": False}
    ).json()

    assert result["valid"], result["problems"]
    assert result["spec"]["generation"]["method"] == "rule_based"
    kinds = {c["generator"]["kind"] for c in result["spec"]["columns"] if c.get("generator")}
    assert "scipy" not in kinds
    assert any("rewritten as rule based" in n for n in result["notes"])


def test_new_loans_can_be_switched_on_from_the_configure_form(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    result = client.post(
        "/api/configure",
        json={"spec": spec, "periods": 12, "origination_rate": 0.03, "from_base": False},
    ).json()

    assert result["valid"], result["problems"]
    assert result["spec"]["originations"]["rate"] == 0.03
    assert result["capabilities"]["origination_rate"] == 0.03
    assert any("new loans arrive" in n.lower() for n in result["notes"])


def test_an_open_pool_run_reports_what_arrived(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    spec["originations"] = {"rate": 0.05}
    started = client.post("/api/run", json={"spec": spec, "num_records": 200, "periods": 4})
    job = wait_for(client, started.json()["job"])

    assert job["status"] == "done", job.get("error")
    result = job["result"]
    assert result["originated"] == 30, "10 per period across three later cut-offs"
    assert result["total_entities"] == 230
    assert result["validation"]["passed"]
    assert result["mix"][0]["originated"] == 0


def test_a_deep_method_without_a_sample_is_refused_before_the_run(client):
    """The engine would raise minutes in; the UI should refuse in milliseconds."""
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    spec["generation"] = {"method": "ctgan"}
    response = client.post("/api/run", json={"spec": spec, "num_records": 50})

    assert response.status_code == 400
    assert "learns from real data" in response.json()["error"]


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


def test_a_run_is_queued_polled_and_completed(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post(
        "/api/run", json={"spec": spec, "num_records": 300, "seed": 5, "periods": 4}
    )
    assert started.status_code == 200
    job = wait_for(client, started.json()["job"])

    assert job["status"] == "done", job.get("error")
    result = job["result"]
    assert result["periods"] == 4
    assert len(result["files"]) == 4
    assert result["validation"]["passed"]
    # Paths handed to the browser are workspace-relative, never absolute.
    assert not any(path.startswith("/") for path in result["files"])


def test_a_run_with_an_invalid_spec_is_refused_up_front(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    spec["lifecycle"]["transitions"][0][0] = 0.5
    response = client.post("/api/run", json={"spec": spec, "num_records": 100})
    assert response.status_code == 400
    assert "sums to" in " ".join(response.json()["problems"])


def test_an_unknown_scenario_surfaces_as_a_job_error(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post(
        "/api/run",
        json={"spec": spec, "num_records": 100, "periods": 2, "scenario": "apocalypse"},
    )
    job = wait_for(client, started.json()["job"])
    assert job["status"] == "error"
    assert "no scenario" in job["error"]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("num_records", 0, "num_records must be at least 1"),
        ("periods", -1, "periods must be at least 1"),
        ("seed", -1, "seed must be at least 0"),
        ("num_records", 1.5, "num_records must be an integer"),
    ],
)
def test_a_run_rejects_invalid_numeric_inputs(client, field, value, message):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    response = client.post("/api/run", json={"spec": spec, field: value})

    assert response.status_code == 400
    assert response.json()["detail"] == message


@pytest.mark.parametrize("field", ["num_records", "periods", "seed"])
@pytest.mark.parametrize("literal", ["Infinity", "-Infinity", "NaN"])
def test_a_run_rejects_the_non_finite_json_literals(client, field, literal):
    """`Infinity` and `NaN` are not JSON, and `json.loads` accepts them anyway.

    They have to be posted as raw text because no JSON encoder will emit them.
    `int(nan)` raises ValueError and `int(inf)` raises OverflowError — an
    ArithmeticError, which slips past a `(TypeError, ValueError)` clause. So the
    two arrive together and only one of them used to be caught; the other was a
    500 from a public endpoint.
    """
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    body = json.dumps({"spec": spec})[:-1] + f', "{field}": {literal}}}'

    response = client.post("/api/run", content=body, headers={"Content-Type": "application/json"})

    assert response.status_code == 400, f"{field}={literal} should not be a 500"
    assert response.json()["detail"] == f"{field} must be an integer"


def test_polling_an_unknown_job_is_a_404(client):
    assert client.get("/api/run/deadbeef").status_code == 404


def test_progress_advances_and_names_a_stage(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post("/api/run", json={"spec": spec, "num_records": 200, "periods": 3})
    job = wait_for(client, started.json()["job"])
    assert job["progress"] == 1.0
    assert job["stage"]


def test_progress_names_one_of_the_seven_stages(client):
    """The progress view lists fixed stages, so the engine's must map onto them."""
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post("/api/run", json={"spec": spec, "num_records": 200, "periods": 3})
    assert [s["key"] for s in started.json()["stages"]] == [k for k, _ in web.STAGES]

    job = wait_for(client, started.json()["job"])
    assert job["step"] in {k for k, _ in web.STAGES}


# ---------------------------------------------------------------------------
# results: charts and the data table
# ---------------------------------------------------------------------------


@pytest.fixture
def finished(client):
    """One completed run, reused by everything that inspects a result."""
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post("/api/run", json={"spec": spec, "num_records": 300, "periods": 6})
    job = wait_for(client, started.json()["job"])
    assert job["status"] == "done", job.get("error")
    return started.json()["job"], job["result"]


def test_the_four_charts_come_back_aggregated(client, finished):
    job_id, _ = finished
    charts = client.get(f"/api/charts/{job_id}").json()

    assert charts["unavailable"] == {}, "this pack supports every chart"
    assert charts["pool_balance"]["column"] == "current_balance"
    assert charts["pool_balance"]["factor"][0] == 1.0
    # A pool that amortises ends smaller than it started.
    assert charts["pool_balance"]["balance"][-1] < charts["pool_balance"]["balance"][0]
    assert charts["delinquency"]["series"]
    assert len(charts["ltv"]["edges"]) == 31
    # No sample was uploaded, so there is nothing to compare against and the
    # chart says so rather than inventing a second series.
    assert charts["has_reference"] is False
    assert charts["distribution"][0]["reference"] is None


def test_the_data_table_searches_sorts_and_pages(client, finished):
    job_id, _ = finished

    first = client.get(f"/api/table/{job_id}", params={"limit": 5}).json()
    assert len(first["rows"]) == 5
    assert first["total"] > 5

    sorted_desc = client.get(
        f"/api/table/{job_id}", params={"limit": 5, "sort": "current_balance", "descending": True}
    ).json()
    balances = [row[sorted_desc["columns"].index("current_balance")] for row in sorted_desc["rows"]]
    assert balances == sorted(balances, reverse=True)

    found = client.get(f"/api/table/{job_id}", params={"search": "Performing"}).json()
    assert 0 < found["total"] <= first["total"]

    page_two = client.get(f"/api/table/{job_id}", params={"limit": 5, "offset": 5}).json()
    assert page_two["rows"] != first["rows"]


def test_charts_and_tables_refuse_a_run_that_has_not_finished(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post("/api/run", json={"spec": spec, "num_records": 100, "periods": 2}).json()
    # Whatever state it is in, asking for results of an unknown job is a 404 and
    # of an unfinished one is a 409 — never a traceback.
    assert client.get("/api/charts/deadbeef").status_code == 404
    assert client.get(f"/api/charts/{started['job']}").status_code in (200, 409)
    wait_for(client, started["job"])


# ---------------------------------------------------------------------------
# step 6: the five downloads
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fmt,marker",
    [
        ("csv", b"loan_id,"),
        ("parquet", b"PAR1"),
        ("xlsx", b"PK"),
        ("yaml", b"meta:"),
        ("report", b"<!doctype html>"),
    ],
)
def test_every_download_format_is_produced(client, finished, fmt, marker):
    job_id, _ = finished
    response = client.get(f"/api/export/{job_id}", params={"format": fmt})
    assert response.status_code == 200, response.text
    assert marker in response.content[:4096] or marker in response.content


def test_the_validation_report_states_the_verdict_and_the_checks(client, finished):
    job_id, _ = finished
    html = client.get(f"/api/export/{job_id}", params={"format": "report"}).text

    assert "PASSED" in html
    assert "ids_unique_per_period" in html
    # Self-contained: an emailed report with a broken CDN link is worthless.
    assert "http://" not in html.replace("http://www.w3.org", "")
    assert "<script" not in html


def test_an_unknown_export_format_is_refused(client, finished):
    job_id, _ = finished
    response = client.get(f"/api/export/{job_id}", params={"format": "docx"})
    assert response.status_code == 400
    assert "unknown format" in response.json()["detail"]


# ---------------------------------------------------------------------------
# download and preview
# ---------------------------------------------------------------------------


def test_output_can_be_previewed_and_downloaded(client):
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    started = client.post("/api/run", json={"spec": spec, "num_records": 200, "periods": 3})
    result = wait_for(client, started.json()["job"])["result"]

    preview = client.get("/api/preview", params={"path": result["panel"], "rows": 5}).json()
    assert len(preview["columns"]) == 71
    assert len(preview["rows"]) == 5

    download = client.get("/api/download", params={"path": result["files"][0]})
    assert download.status_code == 200
    assert download.text.splitlines()[0].startswith("loan_id,")


@pytest.mark.parametrize(
    "path",
    ["../../../etc/passwd", "/etc/passwd", "runs/../../secrets.txt"],
)
def test_a_path_cannot_escape_the_workspace(client, path):
    """A filename from a browser is untrusted input."""
    response = client.get("/api/download", params={"path": path})
    assert response.status_code in (400, 404)


def test_previewing_a_missing_file_is_a_404(client):
    assert client.get("/api/preview", params={"path": "runs/nope/panel.parquet"}).status_code == 404


# ---------------------------------------------------------------------------
# the error cleaner, directly
# ---------------------------------------------------------------------------


def test_explain_problems_keeps_the_message_and_drops_the_plumbing():
    raw = (
        "invalid design spec:\n"
        "1 validation error for DesignSpec\n"
        "lifecycle\n"
        "  Value error, transition matrix row 0 sums to 0.85, expected 1.0 "
        "[type=value_error, input_value={'a': [1, 2]}, input_type=dict]\n"
        "    For further information visit https://errors.pydantic.dev/2.12/v/value_error"
    )
    problems = api.explain_problems(ValueError(raw))
    assert problems == ["lifecycle: transition matrix row 0 sums to 0.85, expected 1.0"]


def test_explain_problems_passes_our_own_bullets_through():
    raw = "spec 'x' has 2 problem(s):\n  - first thing is wrong\n  - second thing is wrong"
    assert api.explain_problems(ValueError(raw)) == [
        "first thing is wrong",
        "second thing is wrong",
    ]


def test_explain_problems_never_returns_nothing():
    assert api.explain_problems(ValueError("")) == ["the spec could not be validated"]


def test_static_assets_must_be_revalidated(client):
    """A browser left to invent its own freshness lifetime will serve a stale
    stylesheet for hours, and the page then silently disagrees with the code on
    disk. `no-cache` means "ask me first", not "do not cache" — the ETag still
    turns an unchanged file into a 304."""
    for asset in ("/", "/styles.css", "/app.js"):
        response = client.get(asset)
        assert response.status_code == 200, asset
        assert "no-cache" in response.headers.get("cache-control", ""), asset


# ---------------------------------------------------------------------------
# hosting this for other people
# ---------------------------------------------------------------------------


def test_a_local_instance_claims_nothing_and_limits_nothing(client):
    """The defaults are the local ones: no ceilings, and the page is free to go
    on saying nothing leaves this machine."""
    meta = client.get("/api/meta").json()
    assert meta["shared"] is False
    assert meta["limits"] == {
        "records": None,
        "periods": None,
        "rows": None,
        "upload_mb": None,
    }


def test_a_shared_instance_says_so(client, monkeypatch):
    """The front end swaps its privacy copy for a warning on the strength of
    this flag, so it has to reach the browser."""
    monkeypatch.setattr(web, "SHARED", True)
    monkeypatch.setattr(web, "MAX_RECORDS", 50_000)
    meta = client.get("/api/meta").json()

    assert meta["shared"] is True
    assert meta["limits"]["records"] == 50_000


def test_a_run_over_the_entity_ceiling_is_refused(client, monkeypatch):
    monkeypatch.setattr(web, "MAX_RECORDS", 1_000)
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]

    response = client.post("/api/run", json={"spec": spec, "num_records": 5_000, "periods": 2})
    assert response.status_code == 400
    problems = response.json()["problems"]
    # Entities, said as entities. This message called them rows until a run of
    # 3,000 produced 33,788 of them.
    assert "up to 1,000 entities" in problems[0]
    # It says where to go for more, rather than only saying no.
    assert "locally" in problems[0]


def test_the_row_ceiling_does_not_refuse_on_arithmetic(client, monkeypatch):
    """`entities x periods` must not be used to reject a run up front.

    Entities that reach a terminal state stop producing rows, so that product
    over-states a closed pool rather than under-stating it — 50,000 entities
    over 60 periods is 2,033,298 rows, not 3,000,000. Refusing on it turns
    runs away that would have fitted, and the person asking has no way to
    argue: they cannot know the survival curve. The loop counts instead.
    """
    monkeypatch.setattr(web, "MAX_RECORDS", 10_000)
    monkeypatch.setattr(web, "MAX_ROWS", 50_000)
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]

    # 500 x 24 = 12,000 by arithmetic, under the ceiling either way, and the
    # point is that the front door does no arithmetic at all.
    response = client.post("/api/run", json={"spec": spec, "num_records": 500, "periods": 24})
    assert response.status_code == 200

    job = wait_for(client, response.json()["job"])
    assert job["status"] == "done", job.get("error")
    assert job["result"]["total_rows"] <= 500 * 24, (
        "the product is an upper bound for a closed pool, which is why it "
        "cannot be used as a lower one"
    )


@pytest.mark.local_only(
    "this drives the in-process web app and monkeypatches its ceiling; the run underneath "
    "would go to the deployment, where that patched ceiling means nothing"
)
def test_the_row_ceiling_holds_when_originations_hide_the_size(client, monkeypatch):
    """The request that arithmetic on the payload cannot catch.

    100 entities over 12 periods is 1,200 rows by the front door. Doubling the
    pool every period is not, and the ceiling inside the ageing loop is the only
    thing standing between that spec and the machine.
    """
    monkeypatch.setattr(web, "MAX_RECORDS", 10_000)
    monkeypatch.setattr(web, "MAX_ROWS", 5_000)
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]
    spec["originations"] = {"rate": 1.0, "fresh": True}

    started = client.post("/api/run", json={"spec": spec, "num_records": 100, "periods": 12})
    assert started.status_code == 200, "it passes the pre-check, as expected"

    job = wait_for(client, started.json()["job"])
    assert job["status"] == "error"
    assert "5,000-row ceiling" in job["error"]
    # A limit that stops the run but leaves its output behind is half a limit.
    assert not (web._workspace() / "runs" / started.json()["job"]).exists()


def test_a_run_over_the_period_ceiling_is_refused(client, monkeypatch):
    monkeypatch.setattr(web, "MAX_PERIODS", 6)
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]

    response = client.post("/api/run", json={"spec": spec, "num_records": 50, "periods": 24})
    assert response.status_code == 400
    assert "up to 6 periods" in response.json()["problems"][0]


def test_the_ceiling_reads_the_spec_when_periods_are_not_overridden(client, monkeypatch):
    """A 24-period pack run without an explicit override is still 24 periods."""
    monkeypatch.setattr(web, "MAX_PERIODS", 6)
    spec = client.get(f"/api/packs/{PACK}").json()["spec"]

    response = client.post("/api/run", json={"spec": spec, "num_records": 50})
    assert response.status_code == 400


def test_an_oversized_upload_is_refused_and_not_kept(client, monkeypatch):
    """Checked as it streams, so a stranger cannot fill a shared disk by
    declaring one size and sending another."""
    monkeypatch.setattr(web, "MAX_UPLOAD_BYTES", 4096)

    response = client.post(
        "/api/upload",
        files={"file": ("big.csv", io.BytesIO(_tape_csv(2000)), "text/csv")},
        data={"kind": "sample"},
    )
    assert response.status_code == 413
    assert "MB" in response.json()["detail"]

    uploads = web.WORKSPACE / "uploads"
    assert not uploads.exists() or not list(uploads.iterdir())


def test_an_upload_inside_the_ceiling_still_works(client, monkeypatch):
    monkeypatch.setattr(web, "MAX_UPLOAD_BYTES", 10 * 1024 * 1024)
    stored = _upload(client, "tape.csv", _tape_csv())
    assert "loan_id" in stored["columns"]
