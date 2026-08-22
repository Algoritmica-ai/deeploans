"""The JSON façade and the CLI.

Everything here checks the *contract* rather than the engine: results are
JSON-serialisable, errors come back as values, and a run leaves behind enough
information to reproduce itself. That contract is what a UI will be built on, so
it is worth pinning now rather than discovering later.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from sdd import api
from sdd.cli import app

PACK = "rmbs_nl_green_lion"
runner = CliRunner()


# ---------------------------------------------------------------------------
# spec handling
# ---------------------------------------------------------------------------


def test_packs_are_discoverable():
    assert PACK in api.list_packs()


def test_a_pack_loads_by_bare_name():
    assert api.load(PACK).meta.name == "green_lion"


def test_a_missing_spec_names_what_is_available():
    with pytest.raises(api.SddError, match="no bundled pack"):
        api.load("not_a_real_pack")


def test_check_reports_a_valid_spec():
    result = api.check(PACK)
    assert result["valid"]
    assert result["spec"]["columns"] == 71
    assert set(result["spec"]["scenarios"]) == {"base", "adverse", "severe"}


def test_check_returns_problems_as_data_not_exceptions(minimal_spec_dict):
    """A user editing a spec file will break it; that is a normal outcome."""
    minimal_spec_dict["derivations"].append({"target": "x", "expr": "ghost * 2"})
    result = api.check(minimal_spec_dict)
    assert not result["valid"]
    assert any("ghost" in p for p in result["problems"])
    assert result["spec"] is None


def test_spec_hash_is_stable_and_content_sensitive():
    first = api.check(PACK)["spec"]["hash"]
    assert first == api.check(PACK)["spec"]["hash"]
    tweaked = api.load(PACK).model_copy(deep=True)
    tweaked.meta.name = "different"
    assert api.spec_hash(tweaked) != first


# ---------------------------------------------------------------------------
# generation and running
# ---------------------------------------------------------------------------


def test_generate_returns_a_serialisable_summary(tmp_path):
    result = api.generate(PACK, 300, out=tmp_path / "book.parquet", seed=1)
    json.dumps(result)  # must not raise
    assert result["rows"] == 300
    assert result["columns"] == 71
    assert (tmp_path / "book.parquet").exists()


def test_run_produces_a_panel_and_passes_its_own_invariants(tmp_path):
    result = api.run(PACK, 400, tmp_path, seed=2, periods=6)
    json.dumps(result, default=str)
    assert result["periods"] == 6
    assert len(result["files"]) == 6
    assert result["validation"]["passed"], result["validation"]
    assert result["surviving_entities"] <= 400


def test_periods_can_be_overridden_without_editing_the_spec(tmp_path):
    """A UI needs to offer 'how many months?' without rewriting a file."""
    result = api.run(PACK, 200, tmp_path, periods=3)
    assert result["periods"] == 3


def test_a_run_writes_a_manifest_that_can_reproduce_it(tmp_path):
    api.run(PACK, 200, tmp_path, seed=7, periods=3)
    manifest = json.loads((tmp_path / api.MANIFEST_NAME).read_text())
    assert manifest["inputs"]["seed"] == 7
    assert manifest["inputs"]["entities"] == 200
    # `base_hash` traces back to the pack on disk; `hash` identifies what was
    # actually run after the --periods override, so the two differ by design.
    assert manifest["spec"]["base_hash"] == api.check(PACK)["spec"]["hash"]
    assert manifest["spec"]["hash"] != manifest["spec"]["base_hash"]
    assert manifest["validation_passed"] is True
    assert "numpy" in manifest["library_versions"]


def test_progress_is_reported_monotonically(tmp_path):
    seen: list[float] = []
    api.run(PACK, 200, tmp_path, periods=4, progress=lambda _s, f: seen.append(f))
    assert seen
    assert seen == sorted(seen)
    assert seen[-1] == pytest.approx(1.0)


def test_the_same_seed_reproduces_a_run(tmp_path):
    import pandas as pd

    a = api.run(PACK, 300, tmp_path / "a", seed=5, periods=4)
    b = api.run(PACK, 300, tmp_path / "b", seed=5, periods=4)
    assert a["surviving_entities"] == b["surviving_entities"]
    pd.testing.assert_frame_equal(pd.read_parquet(a["panel"]), pd.read_parquet(b["panel"]))


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------


def test_an_unknown_scenario_is_refused_by_name(tmp_path):
    with pytest.raises(api.SddError, match="no scenario"):
        api.run(PACK, 100, tmp_path, scenario="apocalypse")


@pytest.mark.slow
def test_stress_scenarios_worsen_outcomes_in_order(tmp_path):
    """base < adverse < severe, on both credit and collateral."""
    import pandas as pd

    distress, values = {}, {}
    for name in ("base", "adverse", "severe"):
        result = api.run(PACK, 4000, tmp_path / name, seed=3, periods=18, scenario=name)
        assert result["validation"]["passed"], f"{name} broke its own invariants"
        panel = pd.read_parquet(result["panel"])
        final = panel[panel["reporting_date"] == panel["reporting_date"].max()]
        distress[name] = final["arrears_bucket"].isin(["60-89 DPD", "90+ DPD", "Defaulted"]).mean()
        values[name] = final["indexed_market_value"].mean()

    assert distress["base"] < distress["adverse"] < distress["severe"]
    assert values["base"] > values["adverse"] > values["severe"]


def test_a_stressed_matrix_is_still_a_valid_spec(tmp_path):
    """Stressing must not smuggle in a transition row that fails validation."""
    spec = api.load(PACK)
    stressed = api._apply_scenario(spec, spec.scenarios["severe"])
    for row in stressed.lifecycle.transitions:
        assert sum(row) == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# validation and fidelity through the façade
# ---------------------------------------------------------------------------


def test_validate_returns_a_summary_alongside_the_detail(tmp_path):
    result = api.run(PACK, 300, tmp_path, periods=4)
    report = api.validate(PACK, result["panel"])
    assert report["passed"]
    assert "checks passed" in report["summary"]


def test_fidelity_takes_its_column_names_from_the_spec(tmp_path):
    a = api.run(PACK, 800, tmp_path / "a", seed=1, periods=4)
    b = api.run(PACK, 800, tmp_path / "b", seed=2, periods=4)
    report = api.fidelity(a["panel"], b["panel"], spec=PACK)
    # The transition delta is only computed when the state column is known,
    # which is the point of passing the spec.
    assert report["transition_delta"] is not None
    assert report["columns_compared"] > 50


# ---------------------------------------------------------------------------
# design (profile -> spec) through the façade
# ---------------------------------------------------------------------------


def test_design_writes_a_spec_that_then_runs(tmp_path):
    source = api.run(PACK, 600, tmp_path / "source", seed=4, periods=6)
    designed = api.design(source["panel"], name="relearned", out=tmp_path / "relearned.yaml")

    assert designed["spec_path"]
    assert (tmp_path / "relearned.yaml").exists()
    json.dumps(designed, default=str)

    rerun = api.run(tmp_path / "relearned.yaml", 400, tmp_path / "rerun", periods=4)
    assert rerun["validation"]["passed"], rerun["validation"]


def test_the_written_spec_warns_that_it_is_inferred(tmp_path):
    source = api.run(PACK, 400, tmp_path / "source", periods=3)
    api.design(source["panel"], name="relearned", out=tmp_path / "spec.yaml")
    header = (tmp_path / "spec.yaml").read_text()[:400]
    assert "inference" in header


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_lists_packs():
    result = runner.invoke(app, ["packs"])
    assert result.exit_code == 0
    assert PACK in result.stdout


def test_cli_check_succeeds_on_a_pack():
    result = runner.invoke(app, ["check", PACK])
    assert result.exit_code == 0
    assert "is valid" in result.stdout


def test_cli_check_exits_nonzero_on_a_broken_spec(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("spec_version: 1\nmeta: {name: broken}\n")
    result = runner.invoke(app, ["check", str(bad)])
    assert result.exit_code == 1
    assert "not valid" in result.stdout


def test_cli_run_end_to_end(tmp_path):
    result = runner.invoke(
        app,
        ["run", PACK, "-n", "300", "-o", str(tmp_path), "--periods", "4", "-q"],
    )
    assert result.exit_code == 0, result.stdout
    assert "invariants passed" in result.stdout
    assert (tmp_path / "all_cutoffs.parquet").exists()


def test_cli_validate_exits_nonzero_when_a_panel_is_broken(tmp_path):
    import pandas as pd

    api.run(PACK, 300, tmp_path, periods=4)
    panel = pd.read_parquet(tmp_path / "all_cutoffs.parquet")
    panel.loc[panel.index[0], "current_balance"] = -1.0
    panel.to_parquet(tmp_path / "broken.parquet", index=False)

    result = runner.invoke(app, ["validate", PACK, str(tmp_path / "broken.parquet")])
    assert result.exit_code == 1
    assert "FAIL" in result.stdout


def test_cli_profile_reports_its_detection_reasoning(tmp_path):
    api.run(PACK, 300, tmp_path, periods=4)
    result = runner.invoke(app, ["profile", str(tmp_path / "all_cutoffs.parquet")])
    assert result.exit_code == 0
    assert "loan_id" in result.stdout
    assert "matched the known" in result.stdout


def test_cli_design_flags_columns_needing_review(tmp_path):
    api.run(PACK, 400, tmp_path / "src", periods=4)
    result = runner.invoke(
        app,
        [
            "design",
            str(tmp_path / "src" / "all_cutoffs.parquet"),
            "-o",
            str(tmp_path / "spec.yaml"),
        ],
    )
    assert result.exit_code == 0
    assert "need review" in result.stdout or "wrote" in result.stdout


def test_cli_emits_json_when_asked(tmp_path):
    api.run(PACK, 200, tmp_path, periods=3)
    result = runner.invoke(app, ["validate", PACK, str(tmp_path / "all_cutoffs.parquet"), "--json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout)["passed"] is True


# ---------------------------------------------------------------------------
# optional backends
# ---------------------------------------------------------------------------


def test_the_nemo_backend_explains_how_to_install_it():
    """Requesting an uninstalled extra must say what to do, not raise ImportError."""
    try:
        import data_designer  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("NeMo Data Designer is installed; the failure path cannot be exercised")

    from sdd.generate.backend_nemo import NemoUnavailable, sample_columns_nemo

    with pytest.raises(NemoUnavailable, match=r"pip install 'sdd\[nemo\]'"):
        sample_columns_nemo(api.load(PACK), 10)


def test_the_deep_polish_explains_how_to_install_it():
    try:
        import sdv  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("SDV is installed; the failure path cannot be exercised")

    import pandas as pd

    from sdd.polish import DeepUnavailable, polish_book

    with pytest.raises(DeepUnavailable, match=r"pip install 'sdd\[deep\]'"):
        polish_book(pd.DataFrame({"a": [1]}), api.load(PACK), seed_data=pd.DataFrame({"a": [1]}))
