"""What the results and download steps hand back.

Charts are aggregations the browser never performs, so their shape is a contract:
a front end binds to these keys directly. The exports are the artefacts people
actually leave with, and the validation report is the one that ends up attached
to a model-risk review — so it has to stand on its own.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from sdd import api
from sdd.export import EXCEL_ROW_LIMIT, ExportError, export, report_html
from sdd.validate.charts import (
    balance_column,
    build_charts,
    delinquency_curve,
    distressed_states,
    ltv_column,
)

PACK = "rmbs_nl_green_lion"


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """One aged panel, reused by every test in the module."""
    out = tmp_path_factory.mktemp("run")
    result = api.run(PACK, 400, out, seed=9, periods=6)
    return api.load(PACK), result, pd.read_parquet(result["panel"])


# ---------------------------------------------------------------------------
# picking the right column
# ---------------------------------------------------------------------------


def test_the_balance_column_comes_from_the_spec_not_from_its_name(run):
    spec, _, panel = run
    assert balance_column(spec, panel) == spec.dynamics.amortisation.balance


def test_a_current_ltv_is_preferred_over_an_original_one(run):
    """An original LTV is fixed at origination, so charting it twice is pointless."""
    spec, _, panel = run
    assert ltv_column(spec, panel) == "cltomv_current"


def test_redeeming_early_does_not_count_as_distress(run):
    spec, _, _ = run
    states = distressed_states(spec)
    assert "Redeemed" not in states
    assert "Defaulted" in states
    assert "Performing" not in states


# ---------------------------------------------------------------------------
# the charts
# ---------------------------------------------------------------------------


def test_every_chart_is_produced_for_a_pack_that_supports_them(run):
    spec, _, panel = run
    charts = build_charts(spec, panel)

    assert charts["unavailable"] == {}
    assert set(charts) >= {"distribution", "delinquency", "ltv", "pool_balance"}


def test_the_distribution_chart_bins_both_series_on_the_same_edges(run):
    spec, _, panel = run
    reference = panel.sample(200, random_state=1)
    charts = build_charts(spec, panel, reference)

    entry = charts["distribution"][0]
    assert len(entry["edges"]) == 31
    assert len(entry["synthetic"]) == len(entry["reference"]) == 30
    # Densities, so both are comparable however many rows each holds.
    assert sum(entry["synthetic"]) == pytest.approx(1.0, abs=0.02)
    assert sum(entry["reference"]) == pytest.approx(1.0, abs=0.02)


def test_the_delinquency_curve_is_a_share_of_the_surviving_pool(run):
    spec, _, panel = run
    curve = delinquency_curve(spec, panel)

    assert len(curve["periods"]) == 6
    for series in curve["series"]:
        assert all(0.0 <= v <= 1.0 for v in series["values"])
    assert all(0.0 <= v <= 1.0 for v in curve["total_delinquent"])


def test_the_pool_balance_falls_and_its_factor_starts_at_one(run):
    spec, _, panel = run
    chart = build_charts(spec, panel)["pool_balance"]

    assert chart["factor"][0] == 1.0
    assert chart["balance"][-1] < chart["balance"][0]
    assert chart["loans"][-1] <= chart["loans"][0]


def test_a_chart_that_cannot_be_drawn_explains_why(run):
    """A spec with no lifecycle should say so, not return a broken chart."""
    spec, _, panel = run
    flat = spec.model_copy(deep=True)
    flat.lifecycle = None

    charts = build_charts(flat, panel.drop(columns=["cltomv_current", "cltimv_current"]))
    assert charts["delinquency"] is None
    assert "lifecycle" in charts["unavailable"]["delinquency"]


# ---------------------------------------------------------------------------
# the exports
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", ["csv", "xlsx", "yaml", "report"])
def test_each_export_produces_a_file(run, tmp_path, fmt):
    spec, result, _ = run
    payload = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    path = export(fmt, panel=result["panel"], spec=payload, result=result, out_dir=tmp_path)

    assert path.exists()
    assert path.stat().st_size > 0


def test_parquet_is_served_as_written_rather_than_re_encoded(run, tmp_path):
    _, result, _ = run
    path = export("parquet", panel=result["panel"], out_dir=tmp_path)
    assert str(path) == str(result["panel"])


def test_csv_holds_every_row_of_the_panel(run, tmp_path):
    _, result, panel = run
    path = export("csv", panel=result["panel"], result=result, out_dir=tmp_path)
    assert len(pd.read_csv(path)) == len(panel)


def test_the_excel_workbook_carries_its_own_provenance(run, tmp_path):
    _, result, _ = run
    path = export("xlsx", panel=result["panel"], result=result, out_dir=tmp_path)
    about = pd.read_excel(path, sheet_name="about")

    assert dict(zip(about["Field"], about["Value"], strict=False))["Spec hash"] == str(
        result["spec_hash"]
    )
    assert EXCEL_ROW_LIMIT == 1_000_000


def test_the_yaml_export_can_be_run_again(run, tmp_path):
    spec, result, _ = run
    payload = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    path = export("yaml", spec=payload, result=result, out_dir=tmp_path)

    reloaded = api.check(str(path))
    assert reloaded["valid"], reloaded["problems"]
    assert reloaded["spec"]["hash"] == result["base_spec_hash"]


def test_an_export_with_nothing_to_export_is_an_error_not_a_crash(tmp_path):
    with pytest.raises(ExportError, match="no panel"):
        export("csv", panel=None, out_dir=tmp_path)
    with pytest.raises(ExportError, match="unknown format"):
        export("docx", panel=None, out_dir=tmp_path)


# ---------------------------------------------------------------------------
# the validation report
# ---------------------------------------------------------------------------


def test_the_report_states_the_verdict_and_lists_the_checks(run):
    spec, result, _ = run
    html = report_html(spec.model_dump(mode="json", by_alias=True), result)

    assert "PASSED" in html
    assert "ids_unique_per_period" in html
    assert str(result["spec_hash"]) in html


def test_the_report_is_self_contained(run):
    """It gets emailed and filed; a CDN link would make it break later."""
    spec, result, _ = run
    html = report_html(spec.model_dump(mode="json", by_alias=True), result)

    assert "<script" not in html
    assert "src=" not in html
    assert "@import" not in html


def test_a_failed_check_is_shown_rather_than_hidden(run):
    _, result, _ = run
    broken = json.loads(json.dumps(result))
    broken["validation"] = {
        "passed": False,
        "total": 2,
        "failed": 1,
        "checks": [
            {"name": "closed_pool", "description": "d", "passed": False, "violations": 17},
            {"name": "ids_unique_per_period", "description": "d", "passed": True, "violations": 0},
        ],
    }
    html = report_html(None, broken)

    assert "FAILED" in html
    assert "17" in html
    assert "The spec was not recorded" in html
