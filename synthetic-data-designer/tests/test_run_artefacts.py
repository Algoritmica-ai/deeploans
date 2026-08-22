"""What a run leaves behind on disk.

The specification's §23 asks for six things: the panel, the per-period tapes in
their own folder, the configuration, the manifest, and the validation report in
two formats. A run produced two of them.

The configuration and the report were not missing so much as *unreachable* —
both existed, generated on demand by the web layer as downloads. So a run driven
through the wizard produced them and the same run from `sdd run` did not, and a
directory of tapes found six months later had a manifest saying what made it and
nothing saying whether it was any good.
"""

from __future__ import annotations

import json
import pathlib

import pytest
import yaml

from sdd import api

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    out = tmp_path_factory.mktemp("artefacts")
    result = api.run(PACK, 200, out, seed=3)
    return result, out


def _files(out: pathlib.Path) -> set[str]:
    return {f.relative_to(out).as_posix() for f in out.rglob("*") if f.is_file()}


@pytest.mark.parametrize(
    "name",
    [
        "all_cutoffs.parquet",
        "configuration.yaml",
        "run_manifest.json",
        "validation_report.html",
        "validation_report.json",
    ],
)
def test_every_artefact_the_specification_asks_for_exists(run, name):
    _, out = run
    assert name in _files(out)


def test_the_tapes_have_their_own_folder(run):
    """Thirty-six monthly files beside the reports is a directory nobody reads."""
    _, out = run
    tapes = [f for f in _files(out) if f.startswith("cutoffs/")]
    assert len(tapes) == 36
    assert all(f.endswith(".csv") for f in tapes)


def test_nothing_is_left_loose_in_the_run_root(run):
    """Pins the layout, so anything new has to be a decision rather than a spill.

    It caught the metrics report arriving, which §23 lists under "where
    supported, also provide" — an intended addition, so it is named here.
    """
    _, out = run
    expected = {
        "all_cutoffs.parquet",
        "configuration.yaml",
        "run_manifest.json",
        "validation_report.html",
        "validation_report.json",
        "portfolio_metrics.parquet",
        "portfolio_metrics.csv",
    }
    loose = {f for f in _files(out) if "/" not in f}
    assert loose == expected


def test_the_written_configuration_reproduces_the_run(tmp_path, run):
    """The point of writing it. The manifest records a hash; this is the document
    that hash is of, and it has to still be runnable."""
    result, out = run
    config = yaml.safe_load((out / "configuration.yaml").read_text())

    check = api.check(config)
    assert check["valid"], check["problems"][:3]
    assert check["spec"]["hash"] == result["spec_hash"]

    again = api.run(config, 200, tmp_path / "again", seed=result["seed"], validate_output=False)
    assert again["total_rows"] == result["total_rows"]


def test_the_configuration_carries_its_provenance(run):
    _, out = run
    header = (out / "configuration.yaml").read_text().splitlines()[0]
    assert "sdd" in header
    assert "seed" in header


def test_the_json_report_is_the_validation_result(run):
    result, out = run
    report = json.loads((out / "validation_report.json").read_text())
    assert report["total"] == result["validation"]["total"]
    assert report["passed"] == result["validation"]["passed"]


def test_the_html_report_stands_alone(run):
    """Standalone means openable from a directory with no network and no server."""
    _, out = run
    html = (out / "validation_report.html").read_text()
    assert html.lstrip().lower().startswith("<!doctype html")
    assert "<style" in html, "the report depends on a stylesheet it does not carry"
    assert "http://" not in html and "https://" not in html.replace("http-equiv", "")


@pytest.mark.local_only(
    "the deployed API always validates; it exposes no way to ask it not to, so there is "
    "no such run to observe"
)
def test_a_run_without_validation_writes_no_report(tmp_path):
    """Absent is right; an empty report would claim a check that never ran."""
    api.run(PACK, 100, tmp_path, seed=3, validate_output=False)
    files = _files(tmp_path)
    assert "configuration.yaml" in files
    assert "validation_report.json" not in files
    assert "validation_report.html" not in files


def test_packs_without_a_cutoff_dir_keep_the_flat_layout(tmp_path):
    """Moving them would relocate the output of every spec already written."""
    for pack in ("auto_abs_esma_annex5", "rmbs_nl_green_lion"):
        assert api.load(pack).emit.cutoff_dir is None
        api.run(pack, 100, tmp_path / pack, seed=3, validate_output=False)
        tapes = [f for f in _files(tmp_path / pack) if f.endswith(".csv")]
        assert tapes and all("/" not in f for f in tapes), f"{pack} tapes moved"


def test_the_run_reports_where_it_put_them(run):
    """A caller should not have to guess the filenames."""
    result, _ = run
    artefacts = result["artefacts"]
    assert set(artefacts) == {"configuration", "validation_json", "validation_html"}
    for path in artefacts.values():
        assert pathlib.Path(path).is_file()
