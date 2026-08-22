"""The sample spec that shows every option.

A kitchen-sink example is worth having and rots faster than anything else in a
repository: an option gets added, the example does not, and a reader who trusts
it writes a spec missing the thing they needed.

So it is a real file that really runs, and these tests make it binding. It must
load, generate, pass its own invariants, and still demonstrate every generator
kind, hazard kind, derivation kind, metric kind and chart kind the schema
offers. Add an option without showing it here and the suite fails.

Writing it was the argument for testing it. Four mistakes survived reading and
were caught by `api.check`, and two more only appeared at run time — including
a YAML comma that silently turned a description into a second key.
"""

from __future__ import annotations

import inspect
import typing
from pathlib import Path

import pandas as pd
import pytest
import yaml
from pydantic import BaseModel

import sdd.spec.schema as schema
from sdd import api

SAMPLE = Path(__file__).resolve().parents[1] / "docs" / "reference_spec.yaml"
ENTITIES = 400


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("reference")
    result = api.run(SAMPLE, ENTITIES, out, seed=42, validate_output=True)
    return result, pd.read_parquet(result["panel"])


@pytest.fixture(scope="module")
def document():
    return yaml.safe_load(SAMPLE.read_text(encoding="utf-8"))


def test_it_loads(document):
    check = api.check(SAMPLE)
    assert check["valid"], check["problems"][:5]


def test_it_runs_and_its_own_checks_pass(generated):
    """Validating is not the same as running.

    Two of this file's mistakes passed `api.check` and failed mid-generation —
    an expression naming a variable that does not exist, and a spec combining
    originations with a secondary chain whose column was never declared. Only
    running finds those.
    """
    result, panel = generated
    assert result["total_rows"] > 0
    assert result["entities"] == ENTITIES
    failing = [c["name"] for c in result["validation"]["checks"] if not c.get("passed", True)]
    assert not failing, failing
    assert result["validation"]["passed"]
    assert len(panel) > 0


def test_the_helper_column_is_dropped(generated):
    """`role: helper` is shown in the sample, so it has to behave as advertised."""
    _, panel = generated
    assert "_score_noise" not in panel.columns


def test_every_block_appears(document):
    """A reader should not have to wonder whether a block was simply forgotten."""
    missing = [name for name in schema.DesignSpec.model_fields if name not in document]
    # `benchmark` is deliberately absent: it needs a hidden latent driving the
    # outcome, which would make this a benchmark rather than a reference.
    assert missing == ["benchmark"], missing


@pytest.mark.parametrize(
    ("what", "found_in"),
    [
        ("generator", "columns"),
        ("hazard", "lifecycle"),
        ("derivation", "derivations"),
        ("metric", "metrics"),
        ("chart", "results"),
    ],
)
def test_every_kind_is_demonstrated(document, what, found_in):
    """The claim the document makes about itself, checked against the schema.

    Coverage is what a kitchen-sink example is *for*. If it does not show an
    option, a reader has no way of knowing the option exists.
    """
    text = SAMPLE.read_text(encoding="utf-8")
    expected = _schema_kinds(what)
    missing = sorted(k for k in expected if f"kind: {k}" not in text)
    assert not missing, (
        f"{what} kinds in the schema but not shown in the sample: {missing} "
        f"(add them under `{found_in}`)"
    )


def _schema_kinds(what: str) -> set[str]:
    """Every literal value of `kind` on the models of one family."""
    families = {
        "generator": lambda n: n.endswith("Gen"),
        "hazard": lambda n: n.endswith("Hazard"),
        "derivation": lambda n: n == "Derivation",
        "metric": lambda n: n == "Metric",
        "chart": lambda n: n == "ChartSpec",
    }
    match = families[what]
    out: set[str] = set()
    for name, obj in vars(schema).items():
        if not (inspect.isclass(obj) and issubclass(obj, BaseModel) and match(name)):
            continue
        field = obj.model_fields.get("kind")
        if field is not None:
            out |= set(typing.get_args(field.annotation))
    return out


def test_metric_and_chart_kinds_are_all_used(document):
    """Those two are plain enums rather than discriminated unions, so the
    `kind:` scan above cannot see the ones that never appear."""
    declared_metrics = {m["kind"] for m in document["metrics"]}
    every_metric = set(typing.get_args(schema.Metric.model_fields["kind"].annotation))
    assert declared_metrics == every_metric, every_metric - declared_metrics

    declared_charts = {c["kind"] for c in document["results"]["charts"]}
    every_chart = set(typing.get_args(schema.ChartSpec.model_fields["kind"].annotation))
    assert declared_charts == every_chart, every_chart - declared_charts


def test_the_reference_embeds_it():
    """The sample belongs on the same page as the field tables.

    A table tells you what a field is called; only a worked document tells you
    what a spec looks like.
    """
    reference = (SAMPLE.parent / "SPEC-REFERENCE.md").read_text(encoding="utf-8")
    assert "## A spec using every option" in reference
    assert "name: reference_all_options" in reference
