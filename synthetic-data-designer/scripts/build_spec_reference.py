"""Generate the full spec reference from the schema itself.

Two hundred and eighty-five fields across fifty models. A hand-written
reference for that is wrong within a fortnight — the field that gets added in
a hurry is exactly the one nobody documents — so this reads the pydantic models
and writes the document.

Everything in the output already exists in the code: the descriptions are the
`Field(description=...)` text, the prose is the model docstrings, the defaults
and bounds are the validators. Writing the reference by hand would mean saying
all of it twice and letting the two copies drift.

    python scripts/build_spec_reference.py            # write docs/SPEC-REFERENCE.md
    python scripts/build_spec_reference.py --check    # fail if it is stale

`tests/test_spec_reference.py` runs the second form, so a new field without a
description, or a description changed without regenerating, fails the suite
rather than quietly leaving the reference wrong.
"""

from __future__ import annotations

import argparse
import inspect
import sys
import typing
from pathlib import Path
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from pydantic_core import PydanticUndefined

import sdd.spec.schema as schema

OUT = Path(__file__).resolve().parents[1] / "docs" / "SPEC-REFERENCE.md"
SAMPLE = Path(__file__).resolve().parents[1] / "docs" / "reference_spec.yaml"

# The order a reader meets things, which is not the order python defines them.
TOP_LEVEL = "DesignSpec"


def readable(annotation: Any) -> str:
    """A type a person can read, rather than a repr full of `typing.`."""
    if annotation is type(None):
        return "null"
    if annotation is Any:
        return "any"

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is typing.Union or str(origin) == "<class 'types.UnionType'>":
        parts = [readable(a) for a in args if a is not type(None)]
        optional = type(None) in args
        joined = " or ".join(dict.fromkeys(parts))
        return f"{joined}, optional" if optional else joined
    if origin in (list, set, frozenset):
        return f"list of {readable(args[0])}" if args else "list"
    if origin is dict:
        return f"map of {readable(args[0])} to {readable(args[1])}" if args else "map"
    if origin is typing.Literal:
        return " | ".join(f"`{a}`" for a in args)
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return f"[{annotation.__name__}](#{annotation.__name__.lower()})"
    if inspect.isclass(annotation):
        return {"str": "text", "int": "integer", "float": "number", "bool": "true/false"}.get(
            annotation.__name__, annotation.__name__
        )
    return str(annotation).replace("typing.", "")


def constraints(field: Any) -> str:
    """Bounds a value must satisfy, gathered from the field's metadata."""
    out = []
    for item in field.metadata:
        for attribute, symbol in (
            ("ge", "≥"),
            ("gt", ">"),
            ("le", "≤"),
            ("lt", "<"),
            ("min_length", "min length"),
            ("max_length", "max length"),
        ):
            value = getattr(item, attribute, None)
            if value is not None:
                out.append(f"{symbol} {value}")
    return ", ".join(out)


def default_of(field: Any) -> str:
    if field.is_required():
        return "**required**"
    if field.default_factory is not None:
        made = field.default_factory()
        return "empty" if made in ({}, [], set()) else f"`{made}`"
    if field.default is PydanticUndefined or field.default is None:
        return "none"
    return f"`{field.default}`"


def describe(model: type[BaseModel]) -> list[str]:
    """One section: the model's own prose, then a row per field."""
    lines = [f"### {model.__name__}", ""]

    # The model's *own* docstring, never an inherited one. `inspect.getdoc`
    # walks the MRO, so every model that says nothing was rendering `_Base`'s
    # "reject unknown keys" note as though it described that section.
    doc = inspect.cleandoc(model.__dict__.get("__doc__") or "")
    if doc:
        lines += [doc, ""]

    if not model.model_fields:
        return [*lines, ""]

    lines += ["| field | type | default | notes |", "|---|---|---|---|"]
    for name, field in model.model_fields.items():
        shown = field.alias or name
        note = " ".join((field.description or "").split())
        bound = constraints(field)
        if bound:
            note = f"{note} ({bound})" if note else bound
        lines.append(
            f"| `{shown}` | {readable(field.annotation)} | {default_of(field)} | "
            f"{note or '_no description yet_'} |"
        )
    return [*lines, ""]


def ordered_models() -> list[type[BaseModel]]:
    """DesignSpec first, then everything it reaches, breadth-first.

    Definition order in the module is an implementation detail; a reader wants
    the top-level document first and the pieces it refers to after it.
    """
    everything = {
        name: obj
        for name, obj in vars(schema).items()
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj is not BaseModel
    }
    root = everything[TOP_LEVEL]
    seen, queue, out = {TOP_LEVEL}, [root], [root]

    while queue:
        current = queue.pop(0)
        for field in current.model_fields.values():
            for candidate in _models_in(field.annotation):
                if candidate.__name__ not in seen:
                    seen.add(candidate.__name__)
                    out.append(candidate)
                    queue.append(candidate)

    # Anything unreachable still gets documented, at the end, rather than
    # silently omitted because nothing happens to point at it.
    out += [m for n, m in sorted(everything.items()) if n not in seen]
    return out


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return [annotation]
    return [m for arg in get_args(annotation) for m in _models_in(arg)]


def build() -> str:
    models = ordered_models()
    total = sum(len(m.model_fields) for m in models)

    head = [
        "# Spec reference",
        "",
        f"Every field of the spec: **{len(models)} sections, {total} fields**.",
        "",
        "**Generated from the schema** by `scripts/build_spec_reference.py`. Do not edit "
        "this file — change the `Field(description=...)` or the model docstring in "
        "`src/sdd/spec/schema.py` and regenerate. A test fails if the two drift.",
        "",
        "A spec is one YAML document. Only `meta`, `entity` and `columns` are required; "
        "everything else adds behaviour, and a spec with none of it still generates a "
        "single-cut-off table.",
        "",
        "New to the whole idea: [WHAT-IS-SDD.md](WHAT-IS-SDD.md). Writing one with an "
        "LLM: [AUTHORING-WITH-AN-LLM.md](AUTHORING-WITH-AN-LLM.md).",
        "",
        "## Contents",
        "",
    ]
    head += ["- [A spec using every option](#a-spec-using-every-option)"]
    head += [f"- [{m.__name__}](#{m.__name__.lower()})" for m in models]
    head += ["", "---", ""]

    # The sample is embedded rather than linked. A field table tells you what a
    # field is called; only a worked document tells you what a spec looks like,
    # and the two belong on the same page.
    head += [
        "## A spec using every option",
        "",
        "Every block, every generator kind, every hazard kind, every derivation kind, "
        "every metric and every chart, in one file. **It runs** — "
        "`tests/test_reference_spec.py` generates from it, checks the invariants pass, "
        "and fails if a new option is added to the schema and not shown here.",
        "",
        "Not a calibrated pack, and not a recommendation: some of it is deliberately "
        "odd because the point is coverage rather than realism. For sensible examples "
        "read the packs.",
        "",
        f"Source: [`{SAMPLE.name}`]({SAMPLE.name}).",
        "",
        "```yaml",
        SAMPLE.read_text(encoding="utf-8").rstrip(),
        "```",
        "",
        "---",
        "",
    ]

    body: list[str] = []
    for model in models:
        body += describe(model)
    return "\n".join(head + body).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the file is stale")
    args = parser.parse_args()

    generated = build()
    if args.check:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != generated:
            print(
                f"{OUT.relative_to(Path.cwd())} is out of date.\n"
                "Run: python scripts/build_spec_reference.py",
                file=sys.stderr,
            )
            return 1
        print(f"{OUT.name} is current")
        return 0

    OUT.write_text(generated, encoding="utf-8")
    print(f"wrote {OUT.relative_to(Path.cwd())} — {generated.count(chr(10))} lines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
