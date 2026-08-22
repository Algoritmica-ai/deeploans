"""The generated spec reference, and the claims the authoring guide makes.

A reference for 285 fields cannot be maintained by hand — the field added in a
hurry is exactly the one nobody documents. So it is generated from the schema,
and these tests make the generation binding: a description changed without
regenerating, or a new field with no description at all, fails here rather than
leaving the document quietly wrong.
"""

from __future__ import annotations

import inspect
import re
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import BaseModel

import sdd.spec.schema as schema

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "docs" / "SPEC-REFERENCE.md"
GUIDE = ROOT / "docs" / "AUTHORING-WITH-AN-LLM.md"


def _models() -> dict[str, type[BaseModel]]:
    return {
        name: obj
        for name, obj in vars(schema).items()
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj is not BaseModel
    }


def test_the_reference_is_current():
    """Regenerate and compare. Stale documentation is worse than none, because
    it is trusted."""
    result = subprocess.run(
        [sys.executable, "scripts/build_spec_reference.py", "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_every_field_appears():
    """No field may be missing from the reference.

    "All the optionality" is the whole point of the document, so a field that
    exists and is not listed is the one failure mode that matters.
    """
    text = REFERENCE.read_text(encoding="utf-8")
    missing = []
    for name, model in _models().items():
        for field_name, field in model.model_fields.items():
            shown = field.alias or field_name
            if not re.search(rf"\|\s*`{re.escape(shown)}`\s*\|", text):
                missing.append(f"{name}.{shown}")
    assert not missing, f"absent from the reference: {missing[:12]}"


def test_the_described_share_only_grows():
    """A ratchet, not a bar.

    152 of 285 fields had no description when the reference was first
    generated, and writing all of them at once was not the right trade. This
    pins the count so it can improve and cannot slide back — a new field
    without a description fails, and describing an old one lowers the number
    for free.
    """
    undescribed = [
        f"{name}.{field_name}"
        for name, model in _models().items()
        for field_name, field in model.model_fields.items()
        if not field.description
    ]
    assert len(undescribed) <= 145, (
        f"{len(undescribed)} fields have no description, up from the recorded 145. "
        f"New ones: describe them in `Field(description=...)`. {undescribed[:8]}"
    )


def test_the_authoring_guide_only_names_real_fields():
    """The guide is written to be pasted into an LLM as context.

    An invented field name in it becomes an invented field name in every spec
    written from it, and the model has no way to know. Every YAML key the guide
    shows must exist in the schema.
    """
    known = {
        field.alias or field_name
        for model in _models().values()
        for field_name, field in model.model_fields.items()
    }
    # Values inside example generators and SQL are not spec keys.
    ignore = {
        "s",
        "loc",
        "scale",
        "sql",
        "select",
        "from",
        "where",
        "with",
        "group",
        "by",
        "params",
        "kind",
        "name",
        "description",
        "e",
        "g",
    }

    text = GUIDE.read_text(encoding="utf-8")
    blocks = re.findall(r"```yaml\n(.*?)```", text, re.S)
    assert blocks, "the guide shows no YAML at all"

    unknown = set()
    for block in blocks:
        for key in re.findall(r"^\s*-?\s*([a-z_][a-z0-9_]*)\s*:", block, re.M):
            if key not in known and key not in ignore:
                unknown.add(key)
    assert not unknown, f"the guide names fields that do not exist: {sorted(unknown)}"


@pytest.mark.parametrize(
    "claim",
    [
        "terminal",  # entities leave the pool
        "absorbing",  # entities stay, in that state
        "bernoulli",
        "dwell_time",
        "condition",
    ],
)
def test_the_guide_names_mechanisms_that_exist(claim):
    """The distinctions the guide leans on have to be real ones."""
    assert claim in GUIDE.read_text(encoding="utf-8")
    assert claim in REFERENCE.read_text(encoding="utf-8")
