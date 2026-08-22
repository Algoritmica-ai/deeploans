"""The single entry point everything else calls.

Every function here takes plain values and returns JSON-serialisable dicts. That
is deliberate: the CLI is its first consumer today and a web API will be its
second, and neither should reach past this module into the engine. Keeping one
contract means the UI phase is a thin layer rather than a second implementation.

Three things every run produces, beyond the data itself:

**A manifest**
    Spec hash, seed, row counts, library versions, timings. Written next to the
    output, so a dataset found six months later can be traced back to what made
    it and regenerated exactly.

**Progress callbacks**
    ``progress(stage, fraction)``. A 500k-row run takes minutes; a UI needs to
    show something during them.

**Errors as values, not tracebacks**
    A bad spec is a normal outcome of a user editing a file. It comes back as a
    structured problem list, not an exception a web handler has to catch.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from sdd import __version__

ProgressFn = Callable[[str, float], None]

MANIFEST_NAME = "run_manifest.json"


class SddError(RuntimeError):
    """Something the caller asked for cannot be done."""


# ---------------------------------------------------------------------------
# spec handling
# ---------------------------------------------------------------------------


def load(spec: str | Path | dict[str, Any]) -> Any:
    """Accept a spec as a path, a pack name, or an already-parsed mapping."""
    from sdd.spec import load_spec, load_spec_dict

    if isinstance(spec, dict):
        return load_spec_dict(spec)
    path = Path(spec)
    if not path.exists():
        packed = pack_path(str(spec))
        if packed is None:
            raise SddError(
                f"no spec found at {spec!r}, and no bundled pack of that name "
                f"(available: {', '.join(list_packs()) or 'none'})"
            )
        path = packed
    return load_spec(path)


def packs_dir() -> Path:
    """Where the bundled packs live, whether installed or run from a checkout."""
    installed = Path(__file__).parent / "packs"
    if installed.is_dir():
        return installed
    return Path(__file__).resolve().parents[2] / "packs"


def list_packs() -> list[str]:
    """Bundled packs, in the order they should be offered.

    `meta.display_order` first, then everything else alphabetically. Sorting by
    filename alone put the packs in an order nobody chose, and the top of a list
    is where most people click.

    A pack that will not parse still appears, at the end: the picker is also
    where you would go to find out something is broken.
    """
    directory = packs_dir()
    if not directory.is_dir():
        return []

    import yaml

    def sort_key(path: Path) -> tuple[int, str]:
        try:
            meta = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("meta") or {}
            order = meta.get("display_order")
        except Exception:
            order = None
        return (order if isinstance(order, int) else 10_000, path.stem)

    return [p.stem for p in sorted(directory.glob("*.yaml"), key=sort_key)]


# A pack failing to load with "extra inputs are not permitted" almost always
# means one thing: the process is older than the files. Python imports the spec
# model once at start-up and reads the pack YAML from disk on every request, so
# pulling a change that adds a field leaves a running server rejecting its own
# bundled packs — and the message it produces talks about pydantic rather than
# about restarting.
_STALE_MARKERS = ("extra inputs are not permitted", "extra_forbidden")

RESTART_HINT = (
    "This looks like a server started before the packs changed. Python loads the "
    "spec model once at start-up and re-reads the pack files on every request, so a "
    "running process rejects fields added after it booted. Restart it."
)


def pack_problems() -> dict[str, list[str]]:
    """Bundled packs that will not load, and why.

    Empty when everything is healthy. Checked at start-up and reported through
    the API, because the alternative is a picker that lists a pack and refuses
    to open it.
    """
    problems: dict[str, list[str]] = {}
    for name in list_packs():
        result = check(name)
        if not result["valid"]:
            reasons = list(result["problems"])
            if any(marker in " ".join(reasons).lower() for marker in _STALE_MARKERS):
                reasons.append(RESTART_HINT)
            problems[name] = reasons
    return problems


def pack_path(name: str) -> Path | None:
    candidate = packs_dir() / f"{Path(name).stem}.yaml"
    return candidate if candidate.exists() else None


def spec_hash(spec: Any) -> str:
    """A stable fingerprint of a spec's content, for the manifest."""
    payload = json.dumps(spec.model_dump(mode="json"), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# Everything from `[type=` onward is pydantic's machine detail, and it embeds
# the whole rejected value — which itself contains brackets, so this cannot stop
# at the first closing one.
_PYDANTIC_TAIL = re.compile(r"\s*\[type=.*$")
_NOISE = re.compile(
    r"^(?:invalid design spec:|.* is not a valid design spec:|"
    r"\d+ validation error.*|spec '.*' has \d+ problem\(s\):)$"
)


def explain_problems(error: Exception) -> list[str]:
    """Turn a validation failure into lines a person can act on.

    The messages written into :mod:`sdd.spec.schema` are deliberately specific —
    *"transition matrix row 0 ('Performing') sums to 0.857500, expected 1.0"*.
    Pydantic then wraps each one in a header, a field path, a ``[type=…]`` tail
    carrying the entire rejected input, and a docs link. Handed to a UI verbatim
    the useful sentence ends up as the fourth of five lines, behind
    *"invalid design spec:"*, so the interface shows the user nothing.

    This keeps the sentence and the field it belongs to, and drops the rest.
    """
    problems: list[str] = []
    field: str | None = None

    for raw in str(error).splitlines():
        line = raw.rstrip()
        if not line.strip() or _NOISE.match(line.strip()):
            continue
        if line.strip().startswith("For further information visit"):
            continue

        stripped = line.strip()
        # Our own check_spec bullets are already clean.
        if stripped.startswith("- "):
            problems.append(stripped[2:])
            continue
        # An unindented line with no message is pydantic naming the field.
        if not line.startswith(" ") and "error," not in stripped:
            field = stripped
            continue

        message = _PYDANTIC_TAIL.sub("", stripped)
        for prefix in ("Value error, ", "Assertion failed, "):
            if message.startswith(prefix):
                message = message[len(prefix) :]
        problems.append(f"{field}: {message}" if field else message)

    return problems or [str(error).strip() or "the spec could not be validated"]


def _plural_noun(meta: Any) -> str | None:
    """The plural the interface should use, or None to let it fall back."""
    from sdd.spec.schema import plural_of

    if meta.entity_noun_plural:
        return meta.entity_noun_plural
    return plural_of(meta.entity_noun) if meta.entity_noun else None


def check(spec: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Validate a spec and report problems as data rather than an exception."""
    from sdd.spec import SpecError

    try:
        loaded = load(spec)
    except (SpecError, SddError) as exc:
        return {"valid": False, "problems": explain_problems(exc), "spec": None}
    return {
        "valid": True,
        "problems": [],
        "spec": {
            "name": loaded.meta.name,
            "title": loaded.meta.title or loaded.meta.name.replace("_", " ").title(),
            "description": loaded.meta.description,
            "regulatory_template": loaded.meta.regulatory_template,
            "asset_class": loaded.meta.asset_class,
            "featured": loaded.meta.featured,
            "entity_noun": loaded.meta.entity_noun,
            "entity_noun_plural": _plural_noun(loaded.meta),
            "hash": spec_hash(loaded),
            "columns": len(loaded.output_columns()),
            "periods": loaded.entity.calendar.periods,
            "has_lifecycle": loaded.lifecycle is not None,
            "scenarios": sorted(loaded.scenarios),
            "needs_review": [c.name for c in loaded.columns if c.review],
        },
    }


# ---------------------------------------------------------------------------
# profiling
# ---------------------------------------------------------------------------


def profile(
    sample: str | Path | pd.DataFrame,
    *,
    id_column: str | None = None,
    time_column: str | None = None,
    state_column: str | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Analyse a sample tape. Returns the profile as a plain dict."""
    from sdd.profile import profile_dataset

    result = profile_dataset(
        sample,
        id_column=id_column,
        time_column=time_column,
        state_column=state_column,
        max_rows=max_rows,
    )
    return result.to_dict()


def design(
    sample: str | Path | pd.DataFrame,
    *,
    structure: str | Path | None = None,
    name: str = "profiled",
    out: str | Path | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Profile a sample and write a runnable spec — the whole analysis step."""
    from sdd.profile import build_spec
    from sdd.spec import dump_spec

    spec, prof = build_spec(
        sample, structure=str(structure) if structure else None, name=name, **kwargs
    )

    written = None
    if out:
        written = str(
            dump_spec(
                spec,
                out,
                header=(
                    f"Generated by sdd {__version__} from a sample of {prof.rows:,} rows.\n"
                    "Every generator below is an inference. Columns carrying a `review:`\n"
                    "note were inferred with low confidence — check those first."
                ),
            )
        )

    return {
        "spec": spec.model_dump(mode="json", exclude_none=True, by_alias=True),
        "spec_path": written,
        "profile": prof.to_dict(),
        "needs_review": [c.name for c in spec.columns if c.review],
        "summary": prof.summary(),
    }


# ---------------------------------------------------------------------------
# schema review — step 2 of the wizard
# ---------------------------------------------------------------------------

# Name fragments that mark a column as a date even when it is stored as text.
_DATE_HINTS = ("date", "_dt", "maturity", "origination", "cutoff", "cut_off", "as_of")


def schema(
    spec: str | Path | dict[str, Any], profile: dict[str, Any] | None = None
) -> dict[str, Any]:
    """What was detected about the schema, as an editable table.

    One row per column, carrying both the decision and the evidence behind it —
    the type, whether it is the key, whether it moves over time, how often it was
    blank in the sample, and how confident the inference was. The UI shows this
    before anything is generated, because a wrong key or a mistyped column is
    cheap to fix here and expensive to find later.
    """
    loaded = load(spec)
    by_name = {c["name"]: c for c in (profile or {}).get("columns", [])}

    rows: list[dict[str, Any]] = []
    for column in loaded.columns:
        evidence = by_name.get(column.name, {})
        rows.append(
            {
                "name": column.name,
                "dtype": column.dtype or "str",
                "role": column.role,
                "required": column.required,
                "null_rate": column.null_rate,
                "primary_key": column.name == loaded.entity.id_column,
                "date_column": column.name == loaded.entity.time_column
                or column.dtype == "date"
                or any(hint in column.name.lower() for hint in _DATE_HINTS),
                "generator": column.generator.kind if column.generator else None,
                "domain": column.domain,
                "min": column.min,
                "max": column.max,
                "confidence": column.confidence,
                "review": column.review,
                "description": column.description,
                # From the sample, when there was one.
                "observed_nulls": evidence.get("nulls"),
                "distinct": evidence.get("distinct"),
                "examples": evidence.get("examples"),
            }
        )

    return {
        "columns": rows,
        "primary_key": loaded.entity.id_column,
        "time_column": loaded.entity.time_column,
        "date_columns": [r["name"] for r in rows if r["date_column"]],
        "nullable": [r["name"] for r in rows if not r["required"]],
        "constants": sorted(loaded.constants),
        "derived": [d.target for d in loaded.derivations],
        "needs_review": [r["name"] for r in rows if r["review"]],
        "counts": {
            "columns": len(rows),
            "static": sum(1 for r in rows if r["role"] == "static"),
            "dynamic": sum(1 for r in rows if r["role"] == "dynamic"),
            "derived": len(loaded.derivations),
        },
    }


def edit_schema(spec: str | Path | dict[str, Any], edits: list[dict[str, Any]]) -> dict[str, Any]:
    """Apply the review table's edits — renames, retypes, required/optional.

    A rename has to travel: the same name appears in the entity, the emit order,
    the lifecycle, the dynamics and inside expressions. Renaming the column
    definition alone would produce a spec that validates against nothing and
    fails at generation, so every reference is rewritten with it.
    """
    loaded = load(spec)
    out = loaded.model_copy(deep=True)
    applied: list[str] = []

    for edit in edits:
        original = edit.get("original") or edit.get("name")
        column = out.column(original)
        if column is None:
            continue

        if "dtype" in edit and edit["dtype"] and edit["dtype"] != column.dtype:
            column.dtype = edit["dtype"]
            applied.append(f"{original}: type set to {edit['dtype']}")
        if "required" in edit and bool(edit["required"]) != column.required:
            column.required = bool(edit["required"])
            applied.append(f"{original}: marked {'required' if column.required else 'optional'}")
        if "null_rate" in edit:
            column.null_rate = edit["null_rate"]

        new_name = edit.get("rename") or edit.get("new_name")
        if new_name and new_name != original:
            _rename_column(out, original, str(new_name))
            applied.append(f"{original} renamed to {new_name}")

    if edits and (key := next((e for e in edits if e.get("primary_key")), None)):
        name = key.get("rename") or key.get("name")
        if name and out.column(name) is not None and name != out.entity.id_column:
            out.entity.id_column = name
            applied.append(f"primary key set to {name}")

    payload = out.model_dump(mode="json", exclude_none=True, by_alias=True)
    return {"spec": payload, "applied": applied, **_verdict(payload)}


def _rename_column(spec: Any, old: str, new: str) -> None:
    """Rewrite every reference to a column, not just its definition."""
    import re as _re

    word = _re.compile(rf"\b{_re.escape(old)}\b")

    def swap(text: str | None) -> str | None:
        return word.sub(new, text) if text else text

    for column in spec.columns:
        if column.name == old:
            column.name = new
        gen = column.generator
        if getattr(gen, "kind", None) == "conditional_categorical" and gen.parent == old:
            gen.parent = new

    if spec.entity.id_column == old:
        spec.entity.id_column = new
    if spec.entity.time_column == old:
        spec.entity.time_column = new
    if spec.entity.id_format:
        spec.entity.id_format = swap(spec.entity.id_format)

    for d in spec.derivations:
        if d.target == old:
            d.target = new
        if d.source == old:
            d.source = new
        d.expr = swap(d.expr)
        for rule in d.rules or []:
            rule.if_ = swap(rule.if_) or rule.if_
        d.args = {k: swap(v) or v for k, v in d.args.items()}

    lc = spec.lifecycle
    if lc:
        if lc.state_column == old:
            lc.state_column = new
        lc.state_fields = {
            state: {(new if k == old else k): v for k, v in fields.items()}
            for state, fields in lc.state_fields.items()
        }

    dyn = spec.dynamics
    am = dyn.amortisation
    if am:
        for attribute in ("balance", "rate", "payment", "term"):
            if getattr(am, attribute) == old:
                setattr(am, attribute, new)
        am.flat_when = swap(am.flat_when)
    for index in dyn.indices:
        index.applies_to = [new if c == old else c for c in index.applies_to]
    for counter in dyn.counters:
        if counter.column == old:
            counter.column = new
        counter.expr = swap(counter.expr)
    for accrual in dyn.accruals:
        if accrual.column == old:
            accrual.column = new
        if accrual.add == old:
            accrual.add = new
    if dyn.recovery:
        if dyn.recovery.balance == old:
            dyn.recovery.balance = new
        if dyn.recovery.target == old:
            dyn.recovery.target = new

    target = spec.generation.correlation_target
    if target:
        target.columns = [new if c == old else c for c in target.columns]

    for scenario in spec.scenarios.values():
        scenario.rate_columns = [new if c == old else c for c in scenario.rate_columns]

    if spec.emit.column_order:
        spec.emit.column_order = [new if c == old else c for c in spec.emit.column_order]
    spec.validation.non_negative_columns = [
        new if c == old else c for c in spec.validation.non_negative_columns
    ]
    if old in spec.constants:
        spec.constants[new] = spec.constants.pop(old)

    # Everything below was missing, and the omissions were invisible for the
    # same reason: the only pack exercising the rename declared no metrics, no
    # charts, no groups and no chain, so nothing referenced a column from any of
    # them. Renaming `current_balance` left the report pointing at a column that
    # no longer existed, and the run died mid-generation rather than at the
    # rename that caused it.
    for metric in spec.metrics:
        for attribute in ("column", "weight", "group", "entity_column"):
            if getattr(metric, attribute, None) == old:
                setattr(metric, attribute, new)
        metric.where = swap(metric.where)

    for chart in spec.results.charts:
        if chart.column == old:
            chart.column = new
        if chart.group == old:
            chart.group = new

    for group in spec.groups:
        if group.key == old:
            group.key = new
        for column in group.columns:
            if column.name == old:
                column.name = new
            gen = column.generator
            if getattr(gen, "kind", None) == "conditional_categorical" and gen.parent == old:
                gen.parent = new

    for chain in spec.secondary_chains:
        if chain.lifecycle.state_column == old:
            chain.lifecycle.state_column = new

    for scenario in spec.scenarios.values():
        if old in scenario.segment_stress:
            scenario.segment_stress[new] = scenario.segment_stress.pop(old)

    # Invariants are SQL against the panel, so a rename that does not reach them
    # leaves every custom check querying a column that is gone. Rewritten with
    # the same word-boundary swap the expressions get — the risk is a match
    # inside a string literal, which is the risk already accepted everywhere
    # else a spec holds text.
    for check in spec.validation.custom:
        check.sql = swap(check.sql) or check.sql


# ---------------------------------------------------------------------------
# configuration — step 3 of the wizard
# ---------------------------------------------------------------------------


def configure(
    spec: str | Path | dict[str, Any],
    *,
    method: str | None = None,
    profile: dict[str, Any] | None = None,
    noise: float | None = None,
    correlation: float | None = None,
    outliers: float | None = None,
    missing: float | None = None,
    periods: int | None = None,
    freq: str | None = None,
    default_rate: float | None = None,
    prepayment_rate: float | None = None,
    recovery_rate: float | None = None,
    origination_rate: float | None = None,
    originations_per_period: int | None = None,
) -> dict[str, Any]:
    """Apply the configure form to a spec, and say what each setting did.

    Every control on that form is a change to this document, which is why the
    YAML tab can show the result rather than being a separate way of doing the
    same thing. Settings that cannot apply to a particular spec are reported and
    skipped, not raised: a portfolio with no write-off state should lose its
    recovery box, not its whole form.
    """
    from sdd.age.calibrate import apply_rates, rates
    from sdd.generate.methods import apply_method

    loaded = load(spec)
    notes: list[str] = []

    if method and method != loaded.generation.method:
        loaded, method_notes = apply_method(loaded, method, profile=profile)  # type: ignore[arg-type]
        notes += method_notes

    gen = loaded.generation
    for name, value in (
        ("noise", noise),
        ("correlation", correlation),
        ("outliers", outliers),
        ("missing", missing),
    ):
        if value is not None:
            setattr(gen, name, float(value))

    if gen.correlation_target is None and correlation is not None:
        notes.append(
            "Correlation has nothing to reimpose: no sample was analysed, so no relationship "
            "between columns was ever measured."
        )

    if periods:
        loaded.entity.calendar.periods = int(periods)
    if freq:
        loaded.entity.calendar.freq = freq  # type: ignore[assignment]

    if origination_rate is not None or originations_per_period is not None:
        loaded, origination_notes = set_originations(
            loaded, rate=origination_rate, per_period=originations_per_period
        )
        notes += origination_notes

    loaded, rate_notes = apply_rates(
        loaded,
        default_rate=default_rate,
        prepayment_rate=prepayment_rate,
        recovery_rate=recovery_rate,
    )
    notes += rate_notes

    from sdd.generate.randomness import blankable_columns

    optional = blankable_columns(loaded)
    if missing and not optional:
        notes.append(
            "Missing values have nowhere to go: every column is marked required. Mark the ones "
            "that may be blank as optional in the schema review step."
        )

    payload = loaded.model_dump(mode="json", exclude_none=True, by_alias=True)
    return {
        "spec": payload,
        "notes": notes,
        "rates": rates(loaded),
        "optional_columns": optional,
        **_verdict(payload),
    }


def _verdict(payload: dict[str, Any]) -> dict[str, Any]:
    """Validation of an edited spec, under keys that cannot collide with it.

    ``check`` returns the spec *summary* under ``spec``; these functions return
    the spec *itself* under the same name. Flattening one into the other silently
    replaces the document with its own description — which then fails to load.
    """
    checked = check(payload)
    return {
        "valid": checked["valid"],
        "problems": checked["problems"],
        "summary": checked["spec"],
    }


def set_originations(
    spec: Any, *, rate: float | None = None, per_period: int | None = None
) -> tuple[Any, list[str]]:
    """Turn an open pool on or off, and say what it will do.

    A rate of zero is how the form says "closed pool", so it removes the section
    rather than writing a zero into it — a spec that declares originations and
    then creates none is a spec that lies about its own shape, and the validator
    would switch to the open-pool checks for nothing.
    """
    from sdd.spec.schema import Originations

    out = spec.model_copy(deep=True)
    wanted = rate if rate is not None else per_period

    if not wanted:
        if out.originations is None:
            return out, []
        out.originations = None
        return out, ["New loans switched off: the pool is closed, and only shrinks from here."]

    if out.lifecycle is None:
        return out, [
            "New loans not applied: this configuration has no lifecycle, so there is no "
            "ageing for them to join."
        ]

    existing = out.originations
    fresh = existing.fresh if existing else True
    reset_expr = dict(existing.reset_expr) if existing else {}
    dated = _date_new_loans(out, reset_expr) if fresh else []

    out.originations = Originations(
        rate=rate if rate else None,
        per_period=per_period if per_period else None,
        start_period=existing.start_period if existing else 1,
        end_period=existing.end_period if existing else None,
        fresh=fresh,
        reset=dict(existing.reset) if existing else {},
        reset_expr=reset_expr,
    )

    periods = out.entity.calendar.periods
    window = periods - out.originations.start_period + 1
    if rate:
        note = (
            f"New loans arrive every period at {rate:.1%} of the opening book, across "
            f"{max(window, 0)} cut-off(s) — roughly {rate * window:.0%} of the opening size "
            "added in total, before attrition."
        )
    else:
        note = (
            f"{per_period:,} new loans arrive every period, across {max(window, 0)} cut-off(s) "
            f"— {per_period * max(window, 0):,} in total, before attrition."
        )

    notes = [note]
    if dated:
        notes.append(
            f"They are dated to the period they arrive ({', '.join(dated)}), so anything "
            "computed from an origination date — seasoning, remaining term — follows."
        )
    elif fresh:
        notes.append(
            "They enter performing with every upward-ticking counter at zero. This "
            "configuration has no origination-date column, so if it derives seasoning from "
            "one, set `originations.reset_expr` in the Advanced tab."
        )
    return out, notes


# Columns that carry the date an entity was written. Matched by name, because
# nothing in the spec declares which column means "origination date" — and this
# is the layer where name matching belongs, in front of a user who is told it
# happened.
_ORIGINATION_DATE_COLUMNS = {
    "origination_year": "period_year",
    "origination_month": "period_month",
    "origination_day": "period_day",
}


def _date_new_loans(spec: Any, reset_expr: dict[str, str]) -> list[str]:
    """Point any origination-date column at the period the entity arrives.

    Without this a newly originated loan carries a sampled origination date from
    years ago, and every column derived from it — seasoning, remaining term —
    describes a loan that is not new at all.
    """
    known = {c.name for c in spec.columns} | {d.target for d in spec.derivations}
    applied = []
    for column, expression in _ORIGINATION_DATE_COLUMNS.items():
        if column in known and column not in reset_expr:
            reset_expr[column] = expression
            applied.append(column)
    return applied


def capabilities(spec: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Which controls this spec can honour, so the UI can disable the rest."""
    from sdd.age.calibrate import default_states, prepayment_hazard, rates
    from sdd.generate.randomness import blankable_columns

    loaded = load(spec)
    lc = loaded.lifecycle
    return {
        "optional_columns": blankable_columns(loaded),
        "ageing": lc is not None,
        "default_rate": bool(lc and lc.transitions and default_states(loaded)),
        "prepayment_rate": prepayment_hazard(loaded) is not None,
        "recovery_rate": bool(lc and lc.terminal),
        "originations": lc is not None,
        "origination_rate": (loaded.originations.rate if loaded.originations else None),
        "originations_per_period": (
            loaded.originations.per_period if loaded.originations else None
        ),
        "correlation": loaded.generation.correlation_target is not None,
        "scenarios": sorted(loaded.scenarios),
        "states": lc.states if lc else [],
        "rates": rates(loaded),
        "deep_models": _deep_available(),
    }


def _deep_available() -> bool:
    from importlib.util import find_spec

    return find_spec("sdv") is not None


# ---------------------------------------------------------------------------
# generation
# ---------------------------------------------------------------------------


def generate(
    spec: str | Path | dict[str, Any],
    num_records: int,
    *,
    out: str | Path | None = None,
    seed: int = 42,
    backend: str = "numpy",
    progress: ProgressFn | None = None,
) -> dict[str, Any]:
    """Build the period-0 book only."""
    from sdd.generate import build_book
    from sdd.generate.targets import apply_targets

    loaded = load(spec)
    loaded, _ = apply_targets(loaded, num_records)
    started = time.time()
    book = build_book(loaded, num_records, seed=seed, backend=backend, progress=progress)

    written = None
    if out:
        path = Path(out)
        path.parent.mkdir(parents=True, exist_ok=True)
        book[loaded.output_columns()].to_parquet(path, index=False)
        written = str(path)

    return {
        "rows": len(book),
        "columns": len(loaded.output_columns()),
        "path": written,
        "seconds": round(time.time() - started, 2),
        "spec_hash": spec_hash(loaded),
    }


def run(
    spec: str | Path | dict[str, Any],
    num_records: int,
    out_dir: str | Path,
    *,
    seed: int = 42,
    periods: int | None = None,
    scenario: str | None = None,
    backend: str = "numpy",
    sample: str | Path | pd.DataFrame | None = None,
    validate_output: bool = True,
    progress: ProgressFn | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Generate and age in one call — the normal path.

    ``scenario`` selects a named stress overlay from the spec. ``periods``
    overrides the calendar, which is how a UI offers "how many months?" without
    editing the spec. ``sample`` is the real tape, needed only by the deep
    generation methods.

    ``max_rows`` caps the panel. It is for a shared deployment and is None
    everywhere else — a person generating on their own machine is not told what
    they may ask for.
    """
    from sdd.age.panel import run_ageing
    from sdd.generate import build_book
    from sdd.generate.targets import apply_targets

    loaded = load(spec)
    # The hash of the spec *as loaded*, before any run-time override. Recording
    # both this and the effective hash is what lets someone holding a dataset
    # six months later say "this came from pack X, run with these changes" —
    # the effective hash alone matches nothing on disk.
    base_hash = spec_hash(loaded)

    if periods:
        loaded = loaded.model_copy(deep=True)
        loaded.entity.calendar.periods = periods

    chosen = None
    if scenario:
        if scenario not in loaded.scenarios:
            raise SddError(
                f"spec {loaded.meta.name!r} has no scenario {scenario!r} "
                f"(available: {', '.join(sorted(loaded.scenarios)) or 'none'})"
            )
        chosen = loaded.scenarios[scenario]
        loaded = _apply_scenario(loaded, chosen)

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    started = time.time()

    def book_progress(stage: str, fraction: float) -> None:
        if progress:
            progress(f"book: {stage}", fraction * 0.35)

    seed_data = None
    if sample is not None:
        from sdd.profile import read_sample

        seed_data = sample if isinstance(sample, pd.DataFrame) else read_sample(sample)

    loaded, target_notes = apply_targets(loaded, num_records)

    notes: dict[str, Any] = {}
    if target_notes:
        notes["targets"] = target_notes
    # One table per group, shared by the opening book and every later cohort, so
    # a facility acquired in month twenty can belong to an obligor from month one.
    group_state: dict[str, pd.DataFrame] = {}
    book = build_book(
        loaded,
        num_records,
        seed=seed,
        backend=backend,
        sample=seed_data,
        notes=notes,
        progress=book_progress,
        group_state=group_state,
    )
    book_seconds = time.time() - started

    def age_progress(stage: str, fraction: float) -> None:
        if progress:
            progress(f"ageing: {stage}", 0.35 + fraction * 0.6)

    aged_at = time.time()
    result = run_ageing(
        loaded,
        book,
        out_path,
        seed=seed,
        scenario=chosen,
        progress=age_progress,
        max_rows=max_rows,
        group_state=group_state,
    )
    age_seconds = time.time() - aged_at

    report = None
    if validate_output and result.get("panel"):
        from sdd.validate import validate_panel

        report = validate_panel(loaded, result["panel"]).to_dict()

    if progress:
        progress("done", 1.0)

    payload = {
        "spec": loaded.meta.name,
        "spec_hash": spec_hash(loaded),
        "base_spec_hash": base_hash,
        "scenario": scenario,
        "seed": seed,
        "method": loaded.generation.method,
        "generation_notes": notes,
        "entities": num_records,
        "periods": result["periods"],
        "surviving_entities": result["final_rows"],
        "originated": result.get("originated", 0),
        "total_entities": num_records + result.get("originated", 0),
        "total_rows": result.get("total_rows"),
        "files": result["files"],
        "panel": result["panel"],
        "mix": result["mix"],
        "metrics": result.get("metrics") or [],
        "validation": report,
        "timings": {
            "book_seconds": round(book_seconds, 2),
            "ageing_seconds": round(age_seconds, 2),
            "total_seconds": round(time.time() - started, 2),
        },
    }
    _write_manifest(out_path, loaded, payload)
    payload["artefacts"] = _write_artefacts(out_path, loaded, payload)
    return payload


def _apply_scenario(spec: Any, scenario: Any) -> Any:
    """Return a copy of the spec with the scenario's overlay baked in.

    Transition stressing happens here rather than inside the ageing loop so the
    stressed matrix is validated by the same rules as a hand-written one — a
    scenario cannot smuggle in a row that does not sum to 1.
    """
    from sdd.age.panel import stress_transitions
    from sdd.spec import load_spec_dict

    stressed = spec.model_copy(deep=True)
    if stressed.lifecycle:
        matrix = stress_transitions(spec, scenario)
        if matrix:
            stressed.lifecycle.transitions = matrix
    for column in scenario.rate_columns:
        stressed.derivations.append(_rate_shift_derivation(column, scenario.rate_shift))
    return load_spec_dict(stressed.model_dump(mode="json", by_alias=True))


def _rate_shift_derivation(column: str, shift: float) -> Any:
    from sdd.spec.schema import Derivation

    return Derivation(target=column, expr=f"{column} + {shift}", stage="book")


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def validate(spec: str | Path | dict[str, Any], panel: str | Path | pd.DataFrame) -> dict[str, Any]:
    """Run every spec-derived invariant against a panel."""
    from sdd.validate import validate_panel

    loaded = load(spec)
    report = validate_panel(loaded, panel)
    return {**report.to_dict(), "summary": report.summary()}


def fidelity(
    reference: str | Path | pd.DataFrame,
    synthetic: str | Path | pd.DataFrame,
    *,
    spec: str | Path | dict[str, Any] | None = None,
    id_column: str | None = None,
    time_column: str | None = None,
    state_column: str | None = None,
) -> dict[str, Any]:
    """Score synthetic data against the sample it was meant to resemble."""
    from sdd.profile import read_sample
    from sdd.validate import compare

    if spec is not None:
        loaded = load(spec)
        id_column = id_column or loaded.entity.id_column
        time_column = time_column or loaded.entity.time_column
        state_column = state_column or (loaded.lifecycle.state_column if loaded.lifecycle else None)

    left = reference if isinstance(reference, pd.DataFrame) else read_sample(reference)
    right = synthetic if isinstance(synthetic, pd.DataFrame) else read_sample(synthetic)

    report = compare(
        left,
        right,
        id_column=id_column,
        time_column=time_column,
        state_column=state_column,
    )
    return {**report.to_dict(), "summary": report.summary()}


def charts(
    spec: str | Path | dict[str, Any],
    panel: str | Path | pd.DataFrame,
    *,
    reference: str | Path | pd.DataFrame | None = None,
    columns: list[str] | None = None,
    metrics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Every chart the results view draws, aggregated server-side."""
    from sdd.validate import build_charts

    return build_charts(load(spec), panel, reference, columns=columns, metrics=metrics)


def table(
    panel: str | Path,
    *,
    search: str | None = None,
    sort: str | None = None,
    descending: bool = False,
    offset: int = 0,
    limit: int = 50,
    columns: list[str] | None = None,
) -> dict[str, Any]:
    """A searchable, sortable window onto a panel, without loading it.

    Backed by duckdb over the parquet file: a browser asking for rows 900-950 of
    a twelve-million-row panel, sorted by balance, gets them in milliseconds and
    the panel never enters memory. Identifiers are quoted and the search term is
    bound as a parameter, because both arrive from a browser.
    """
    import duckdb

    path = str(Path(panel))
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TEMP VIEW t AS SELECT * FROM read_parquet('{path}')")
        available = [row[0] for row in con.execute("DESCRIBE t").fetchall()]
        picked = [c for c in (columns or available) if c in available] or available
        quoted = ", ".join(_quote(c) for c in picked)

        where, params = "", []
        if search:
            # Cast everything to text and match anywhere: a person searching a
            # tape is looking for "a loan with 44 in it", not writing SQL.
            haystack = " || '|' || ".join(
                f"COALESCE(CAST({_quote(c)} AS VARCHAR), '')" for c in picked
            )
            where = f"WHERE {haystack} ILIKE ?"
            params.append(f"%{search}%")

        total = con.execute(f"SELECT COUNT(*) FROM t {where}", params).fetchone()[0]

        order = ""
        if sort and sort in available:
            order = f"ORDER BY {_quote(sort)} {'DESC' if descending else 'ASC'}"

        frame = con.execute(
            f"SELECT {quoted} FROM t {where} {order} LIMIT {int(limit)} OFFSET {int(offset)}",
            params,
        ).fetchdf()
    finally:
        con.close()

    return {
        "columns": picked,
        "rows": frame.astype(object).where(frame.notna(), None).values.tolist(),
        "total": int(total),
        "offset": int(offset),
        "limit": int(limit),
    }


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def export(
    fmt: str,
    *,
    panel: str | Path | None = None,
    spec: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    out_dir: str | Path,
    stem: str = "synthetic_data",
) -> dict[str, Any]:
    """Produce a downloadable artefact: csv, parquet, xlsx, yaml or report."""
    from sdd.export import MEDIA_TYPES
    from sdd.export import export as _export

    path = _export(fmt, panel=panel, spec=spec, result=result, out_dir=out_dir, stem=stem)
    return {
        "path": str(path),
        "name": path.name,
        "bytes": path.stat().st_size,
        "media_type": MEDIA_TYPES[fmt],
    }


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


def _write_artefacts(out_dir: Path, spec: Any, payload: dict[str, Any]) -> dict[str, str]:
    """The configuration and the validation report, written beside the data.

    Both already existed — as *downloads*, generated on demand by the web layer.
    So a run driven through the wizard produced them and the same run from
    `sdd run` did not, and a directory of tapes found six months later had the
    manifest to say what made it and nothing to say whether it was any good.

    Reproducibility is the point of writing the configuration: the manifest
    records a hash, and this is the document that hash is of.
    """
    from sdd.spec import dump_spec

    written: dict[str, str] = {}

    config = dump_spec(
        spec,
        out_dir / "configuration.yaml",
        header=(
            f"Generated by sdd {__version__} — spec hash {payload['spec_hash']}, "
            f"seed {payload['seed']}. Re-running this file with that seed reproduces "
            "the data beside it."
        ),
    )
    written["configuration"] = str(config)

    report = payload.get("validation")
    if report:
        from sdd.export import report_html

        as_json = out_dir / "validation_report.json"
        as_json.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        written["validation_json"] = str(as_json)

        as_html = out_dir / "validation_report.html"
        as_html.write_text(
            report_html(spec.model_dump(mode="json", exclude_none=True), payload),
            encoding="utf-8",
        )
        written["validation_html"] = str(as_html)

    return written


def _write_manifest(out_dir: Path, spec: Any, payload: dict[str, Any]) -> Path:
    """Record everything needed to reproduce this run exactly."""
    manifest = {
        "generated_by": f"sdd {__version__}",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "spec": {
            "name": spec.meta.name,
            "asset_class": spec.meta.asset_class,
            "hash": payload["spec_hash"],
            "base_hash": payload["base_spec_hash"],
            "version": spec.spec_version,
        },
        "inputs": {
            "entities": payload["entities"],
            "periods": payload["periods"],
            "seed": payload["seed"],
            "scenario": payload["scenario"],
        },
        "outputs": {
            "files": len(payload["files"]),
            "panel": payload["panel"],
            "surviving_entities": payload["surviving_entities"],
            "originated": payload.get("originated", 0),
        },
        "timings": payload["timings"],
        "validation_passed": (payload["validation"] or {}).get("passed"),
        "library_versions": _versions(),
    }
    path = out_dir / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return path


def _versions() -> dict[str, str]:
    import numpy
    import scipy

    return {
        "numpy": numpy.__version__,
        "pandas": pd.__version__,
        "scipy": scipy.__version__,
    }
