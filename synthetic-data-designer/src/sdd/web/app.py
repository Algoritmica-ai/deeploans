"""Local web UI — a thin HTTP layer over :mod:`sdd.api`.

Deliberately thin, and deliberately local. Every endpoint here is a few lines
that unpack a request, call one `api` function, and return its dict. If an
endpoint needed real logic, the logic would belong in `api` where the CLI can
reach it too.

Four design choices worth stating:

**Runs happen on a worker thread, not in the request.**
    A 500k-row run takes minutes. The endpoint returns a job id immediately and
    the browser polls for progress, so nothing depends on an HTTP connection
    staying open.

**A spec is edited as JSON and validated on every change.**
    The browser holds the whole spec and posts it back to ``/api/check`` as the
    user edits. Validation lives in one place — the same loader the CLI uses —
    so the UI cannot accept a spec the engine would reject.

**Uploads are remembered as a source, not as two loose files.**
    A schema and a sample belong together: the schema fixes the columns, the
    sample supplies the distributions, and the deep generation methods need the
    sample again at run time. One token ties them, and the browser passes it
    along instead of re-uploading or re-profiling.

**Downloads are confined to the job's own directory.**
    Paths are resolved and checked against the workspace root before anything is
    served, because a filename arriving from a browser is untrusted input.
"""

from __future__ import annotations

import os
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from sdd import __version__, api
from sdd.age.panel import RowLimitExceeded

STATIC = Path(__file__).parent / "static"

# Uploads and outputs live here. One directory keeps cleanup and the download
# path check simple. Overridable because a container's working directory is
# often read-only, and the writable path is not knowable at build time.
WORKSPACE = Path(os.environ.get("SDD_WORKSPACE") or Path.cwd() / ".sdd-workspace")

# A tape big enough to profile but small enough to keep the UI responsive.
PROFILE_ROW_LIMIT = 200_000


def _limit(name: str) -> int | None:
    """An optional ceiling, read from the environment. None means no ceiling."""
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw else 0
    except ValueError:
        return None
    return value if value > 0 else None


# Ceilings for a *shared* deployment. All unset by default, because the tool is
# a local one and a person generating on their own laptop should not be told
# what they may ask for. A hosted instance is a different situation: it is one
# machine serving strangers, and an unbounded run there is a denial of service
# with extra steps.
MAX_RECORDS = _limit("SDD_MAX_RECORDS")
MAX_PERIODS = _limit("SDD_MAX_PERIODS")
MAX_UPLOAD_BYTES = (_limit("SDD_MAX_UPLOAD_MB") or 0) * 1024 * 1024 or None

# The ceiling that actually bounds the machine.
#
# MAX_RECORDS counts *entities*, and a panel row is one entity at one cut-off,
# so the two are separated by the number of periods — and by originations, which
# add entities as the pool ages and are set in the spec the browser posts. So
# entities alone bound nothing: measured here, 1,000 entities aged 30 periods
# with `originations.rate: 1.0` produced 413,938 rows. This is enforced inside
# the ageing loop, per period, against the row count itself.
MAX_ROWS = _limit("SDD_MAX_ROWS")

# Whether this instance is shared with people who cannot see the filesystem.
#
# It changes what the interface is allowed to claim. Run locally, the page says
# nothing leaves this machine, and that is true. Hosted, it is false — uploads
# land on someone else's disk, in a workspace every visitor shares — and an
# interface that keeps saying it would be lying to the person deciding whether
# to upload a real loan tape.
SHARED = bool(os.environ.get("SDD_SHARED"))

# What the upload box accepts, split by what the file is for. A schema may be a
# taxonomy or a data dictionary; a sample is always data.
SCHEMA_SUFFIXES = (".csv", ".tsv", ".parquet", ".pq", ".xlsx", ".xlsm", ".xls", ".json")
SAMPLE_SUFFIXES = (".csv", ".tsv", ".parquet", ".pq", ".xlsx", ".xlsm", ".xls", ".json", ".jsonl")

_jobs: dict[str, dict[str, Any]] = {}
_sources: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="sdd-run")

app = FastAPI(title="Synthetic Data Designer", version=__version__)


def _workspace() -> Path:
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    return WORKSPACE


def _safe_path(root: Path, candidate: str) -> Path:
    """Resolve ``candidate`` and refuse anything outside ``root``."""
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise HTTPException(400, "path escapes the workspace")
    if not resolved.exists():
        raise HTTPException(404, f"no such file: {candidate}")
    return resolved


def _uploads() -> Path:
    path = _workspace() / "uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# packs and specs
# ---------------------------------------------------------------------------


@app.get("/api/meta")
def meta() -> dict[str, Any]:
    return {
        "version": __version__,
        "packs": api.list_packs(),
        "deep_models": api._deep_available(),
        "schema_formats": sorted({s.lstrip(".") for s in SCHEMA_SUFFIXES}),
        "sample_formats": sorted({s.lstrip(".") for s in SAMPLE_SUFFIXES}),
        "shared": SHARED,
        # Packs that will not load. The picker shows them as unavailable with
        # the reason, rather than listing something that fails when clicked.
        "pack_problems": api.pack_problems(),
        "limits": {
            "records": MAX_RECORDS,
            "periods": MAX_PERIODS,
            "rows": MAX_ROWS,
            "upload_mb": (MAX_UPLOAD_BYTES // (1024 * 1024)) if MAX_UPLOAD_BYTES else None,
        },
    }


@app.get("/api/packs/{name}")
def get_pack(name: str) -> dict[str, Any]:
    """The full spec of a bundled pack, as JSON the editor can bind to."""
    try:
        spec = api.load(name)
    except api.SddError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        problems = api.pack_problems().get(name) or [str(exc)]
        raise HTTPException(400, f"{name} could not be loaded. " + " ".join(problems)) from exc
    payload = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    return {
        "spec": payload,
        "summary": api.check(name)["spec"],
        "capabilities": api.capabilities(name),
    }


@app.post("/api/check")
def check(spec: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Validate a spec. Problems come back as a list, never as a 500."""
    return api.check(spec)


@app.post("/api/spec/yaml")
def spec_to_yaml(spec: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Render a spec as YAML for the raw-edit escape hatch."""
    import yaml

    return {"yaml": yaml.safe_dump(spec, sort_keys=False, allow_unicode=True, width=100)}


@app.post("/api/spec/parse")
def spec_from_yaml(payload: dict[str, str] = Body(...)) -> dict[str, Any]:
    """Parse YAML back into a spec, validating it on the way."""
    import yaml

    try:
        raw = yaml.safe_load(payload.get("yaml", ""))
    except yaml.YAMLError as exc:
        return {"valid": False, "problems": [f"YAML did not parse: {exc}"], "spec": None}
    if not isinstance(raw, dict):
        return {"valid": False, "problems": ["the document is not a mapping"], "spec": None}
    result = api.check(raw)
    result["parsed"] = raw
    return result


# ---------------------------------------------------------------------------
# step 1 — upload
# ---------------------------------------------------------------------------


@app.post("/api/upload")
async def upload(file: UploadFile = File(...), kind: str = Form("sample")) -> dict[str, Any]:
    """Store an uploaded file and report what was found in it.

    ``kind`` says what the file is *for*, not what it is: the same CSV can be a
    schema (its header) or a sample (its rows), and only the person uploading it
    knows which they meant.
    """
    if not file.filename:
        raise HTTPException(400, "no filename")
    suffix = Path(file.filename).suffix.lower()
    allowed = SCHEMA_SUFFIXES if kind == "schema" else SAMPLE_SUFFIXES
    if suffix not in allowed:
        raise HTTPException(
            400,
            f"expected {', '.join(sorted({s.lstrip('.') for s in allowed}))} for a {kind} "
            f"file, got {suffix.lstrip('.') or 'no extension'}",
        )

    stored = _uploads() / f"{uuid.uuid4().hex[:8]}{suffix}"
    try:
        _store(file, stored)
    except ValueError as exc:
        stored.unlink(missing_ok=True)
        raise HTTPException(413, str(exc)) from exc

    try:
        detail = _describe(stored, kind)
    except Exception as exc:
        stored.unlink(missing_ok=True)
        raise HTTPException(400, f"could not read the file: {exc}") from exc

    return {
        "file": stored.name,
        "kind": kind,
        "original_name": file.filename,
        "size_bytes": stored.stat().st_size,
        **detail,
    }


def _store(file: UploadFile, target: Path) -> None:
    """Write an upload to disk, refusing one that exceeds the size ceiling.

    Copied in chunks and checked as it goes, rather than trusting the declared
    content length or writing the whole thing and measuring afterwards — neither
    of which stops a stranger filling a shared disk.
    """
    written = 0
    with target.open("wb") as handle:
        while chunk := file.file.read(1024 * 1024):
            written += len(chunk)
            if MAX_UPLOAD_BYTES is not None and written > MAX_UPLOAD_BYTES:
                raise ValueError(
                    f"this instance accepts uploads up to "
                    f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB. Take a sample of the tape — "
                    "the distributions settle long before a large file is exhausted."
                )
            handle.write(chunk)


def _describe(path: Path, kind: str) -> dict[str, Any]:
    """What the upload holds, according to what it is for."""
    if kind == "schema":
        from sdd.profile.template import load_template

        template = load_template(path)
        if not template.fields:
            raise ValueError("no columns were found in this schema")
        return {
            "columns": template.column_names,
            "fields": [f.to_dict() for f in template.fields],
            "typed": sum(1 for f in template.fields if f.dtype),
        }

    from sdd.profile import read_sample

    head = read_sample(path, max_rows=5)
    if head.empty:
        raise ValueError("the file has no rows")
    return {"columns": list(head.columns), "preview_rows": len(head)}


@app.post("/api/analyse")
def analyse(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Step 1's button: read the schema, profile the sample, build a spec.

    Either input alone is enough. A schema on its own fixes the columns and their
    types and leaves the distributions to be chosen in the configure step; a
    sample on its own is profiled and everything is inferred. Together, the
    schema wins on structure and the sample wins on shape.
    """
    schema_file = payload.get("schema_file")
    sample_file = payload.get("sample_file")
    if not schema_file and not sample_file:
        raise HTTPException(400, "upload a schema, sample data, or both")

    uploads = _uploads()
    schema_path = _safe_path(uploads, schema_file) if schema_file else None
    sample_path = _safe_path(uploads, sample_file) if sample_file else None
    name = _slug(payload.get("name") or (sample_file or schema_file or "dataset"))

    try:
        if sample_path is not None:
            result = api.design(
                sample_path,
                structure=schema_path,
                name=name,
                id_column=payload.get("id_column") or None,
                time_column=payload.get("time_column") or None,
                state_column=payload.get("state_column") or None,
                # Someone is watching a spinner. Shapes settle long before a
                # large tape is exhausted.
                max_rows=PROFILE_ROW_LIMIT,
            )
        else:
            result = _design_from_schema(schema_path, name)
    except Exception as exc:
        raise HTTPException(400, f"could not build a configuration: {exc}") from exc

    token = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _sources[token] = {
            "schema_file": schema_file,
            "sample_file": sample_file,
            "sample_path": str(sample_path) if sample_path else None,
            "profile": result.get("profile"),
            "base_spec": result["spec"],
        }

    return {
        "source": token,
        "schema": api.schema(result["spec"], result.get("profile")),
        "capabilities": api.capabilities(result["spec"]),
        **result,
    }


def _design_from_schema(schema_path: Path | None, name: str) -> dict[str, Any]:
    """Build a spec from a schema with no sample behind it.

    Every column gets a generator consistent with its declared type and nothing
    more, because nothing more is known. The spec is honest about that: each
    column carries a review note, and the configure step's rule-based method is
    the one that matches this state of knowledge.
    """
    from sdd.profile.template import load_template
    from sdd.spec.schema import (
        Calendar,
        CategoricalGen,
        Column,
        DesignSpec,
        Emit,
        Entity,
        Generation,
        Meta,
        ScipyGen,
        SequenceGen,
        UniformGen,
    )

    assert schema_path is not None
    template = load_template(schema_path)
    fields = template.fields

    id_field = next(
        (f for f in fields if f.name.lower().endswith("id") or "identifier" in f.name.lower()),
        None,
    )
    date_field = next((f for f in fields if f.dtype == "date" or "date" in f.name.lower()), None)

    columns: list[Column] = []
    for f in fields:
        dtype = f.dtype or "str"
        if f is id_field:
            generator: Any = SequenceGen(prefix="E", width=8)
        elif dtype == "date":
            generator = CategoricalGen(values=["2024-01-31"])
        elif dtype == "int":
            generator = UniformGen(low=f.minimum or 0, high=f.maximum or 100, decimals=0)
        elif dtype == "float":
            generator = ScipyGen(
                dist="lognorm", params={"s": 0.5, "loc": 0.0, "scale": 100.0}, decimals=2
            )
        elif dtype == "bool":
            generator = CategoricalGen(values=[True, False], weights=[0.5, 0.5])
        else:
            generator = CategoricalGen(values=f.domain or ["A", "B", "C"])
        columns.append(
            Column(
                name=f.name,
                role="static",
                dtype=dtype,
                generator=generator,
                description=f.description,
                domain=f.domain,
                min=f.minimum,
                max=f.maximum,
                confidence=0.2,
                review="declared by the schema but never observed; choose a generator or "
                "upload sample data to fit one",
            )
        )

    if id_field is None:
        columns.insert(
            0,
            Column(
                name="entity_id",
                role="static",
                dtype="str",
                generator=SequenceGen(prefix="E", width=8),
                description="Added because the schema declared no identifier.",
                confidence=1.0,
            ),
        )
    if date_field is None:
        columns.insert(
            1,
            Column(
                name="as_of_date",
                role="dynamic",
                dtype="str",
                generator=CategoricalGen(values=["2024-01-31"]),
                description="Added because the schema declared no cut-off date.",
                confidence=1.0,
            ),
        )

    spec = DesignSpec(
        meta=Meta(
            name=name,
            description=f"Built from the schema {template.name!r}: "
            f"{len(fields)} declared field(s), no sample data.",
            source=template.source,
        ),
        entity=Entity(
            id_column=(id_field.name if id_field else "entity_id"),
            time_column=(date_field.name if date_field else "as_of_date"),
            calendar=Calendar(start="2024-01-31", periods=1, freq="month_end"),
        ),
        columns=columns,
        generation=Generation(method="rule_based"),
        emit=Emit(filename=f"{name}_{{yyyymm}}.csv", formats=["csv"]),
    )
    payload = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    return {
        "spec": payload,
        "profile": None,
        "needs_review": [c.name for c in spec.columns if c.review],
        "summary": f"{len(columns)} column(s) declared by the schema, no sample data profiled.",
    }


def _slug(text: str) -> str:
    import re

    stem = Path(str(text)).stem
    cleaned = re.sub(r"\W+", "_", stem).strip("_").lower()
    return cleaned or "dataset"


# ---------------------------------------------------------------------------
# step 2 — schema review
# ---------------------------------------------------------------------------


@app.post("/api/schema")
def schema(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """The detected schema as an editable table."""
    profile = payload.get("profile") or _source_field(payload.get("source"), "profile")
    return api.schema(payload["spec"], profile)


@app.post("/api/schema/edit")
def edit_schema(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Apply the review table's edits and re-validate.

    The edited spec becomes the new base for this source. Without that, changing
    the generation method later would rebuild from the spec as first designed and
    silently discard every rename made here.
    """
    try:
        result = api.edit_schema(payload["spec"], payload.get("edits") or [])
    except api.SddError as exc:
        raise HTTPException(400, str(exc)) from exc

    token = payload.get("source")
    if token and result["valid"]:
        with _jobs_lock:
            if token in _sources:
                _sources[token]["base_spec"] = result["spec"]
    return result


# ---------------------------------------------------------------------------
# step 3 — configure
# ---------------------------------------------------------------------------


@app.post("/api/configure")
def configure(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Apply the configure form. The spec that comes back *is* the YAML tab.

    The method is always applied to the spec as first designed, never to an
    already-rewritten one: switching from statistical back to distribution has to
    recover the fitted shapes, and a normal no longer knows them.
    """
    source = _source_field(payload.get("source"), "base_spec")
    spec = payload.get("spec")
    method = payload.get("method")

    base = source if (source and method and payload.get("from_base", True)) else spec
    if base is None:
        raise HTTPException(400, "a spec is required")

    try:
        result = api.configure(
            base,
            method=method,
            profile=_source_field(payload.get("source"), "profile"),
            noise=payload.get("noise"),
            correlation=payload.get("correlation"),
            outliers=payload.get("outliers"),
            missing=payload.get("missing"),
            periods=payload.get("periods"),
            freq=payload.get("freq"),
            default_rate=payload.get("default_rate"),
            prepayment_rate=payload.get("prepayment_rate"),
            recovery_rate=payload.get("recovery_rate"),
            origination_rate=payload.get("origination_rate"),
            originations_per_period=payload.get("originations_per_period"),
        )
    except api.SddError as exc:
        raise HTTPException(400, str(exc)) from exc
    result["capabilities"] = api.capabilities(result["spec"]) if result["valid"] else None
    return result


def _source_field(token: str | None, field: str) -> Any:
    if not token:
        return None
    with _jobs_lock:
        return (_sources.get(token) or {}).get(field)


# ---------------------------------------------------------------------------
# step 4 — generation
# ---------------------------------------------------------------------------

# The stages the progress view names, in order. The engine reports finer-grained
# stages than a person wants to read, so each is mapped to one of these.
STAGES = (
    ("reading", "Reading schema"),
    ("profiling", "Profiling sample"),
    ("configuring", "Building configuration"),
    ("generating", "Generating synthetic data"),
    ("ageing", "Ageing portfolio"),
    ("validating", "Running validation"),
    ("packaging", "Preparing downloads"),
)


def _stage_of(raw: str) -> str:
    """Map an engine stage onto one of the seven the UI shows.

    The first three happened before the run was queued — the schema was read, the
    sample profiled and the configuration built in earlier steps — so a run
    starts at "generating" and the view shows them already done.
    """
    text = raw.lower()
    if text.startswith(("ageing:", "period")):
        return "ageing"
    if "validat" in text:
        return "validating"
    if text in ("done", "finished"):
        return "packaging"
    return "generating"


@app.post("/api/run")
def start_run(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Queue a run and return its id. Progress is polled, not streamed."""
    spec = payload.get("spec")
    if not isinstance(spec, dict):
        raise HTTPException(400, "a spec object is required")

    validation = api.check(spec)
    if not validation["valid"]:
        return JSONResponse({"error": "spec is not valid", "problems": validation["problems"]}, 400)

    token = payload.get("source")
    sample_path = _source_field(token, "sample_path")
    method = (spec.get("generation") or {}).get("method", "distribution")
    if method in ("ctgan", "hybrid") and not sample_path:
        return JSONResponse(
            {
                "error": f"the {method} method learns from real data",
                "problems": [
                    f"{method} trains on the sample tape, and this run has none. Upload sample "
                    "data in step 1, or choose a method that works from the schema alone."
                ],
            },
            400,
        )

    try:
        records = _positive_int(payload, "num_records", default=10_000)
        periods = _positive_int(payload, "periods", default=spec["entity"]["calendar"]["periods"])
        seed = _non_negative_int(payload, "seed", default=42)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if problems := _over_the_ceiling(records, periods):
        return JSONResponse({"error": "this instance has run limits", "problems": problems}, 400)

    job_id = uuid.uuid4().hex[:12]
    out_dir = _workspace() / "runs" / job_id
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "stage": "waiting for a worker",
            "step": "reading",
            "progress": 0.0,
            "started": time.time(),
            "eta_seconds": None,
            "result": None,
            "error": None,
            "spec": spec,
            "source": token,
        }

    _pool.submit(
        _execute,
        job_id,
        spec,
        out_dir,
        records,
        seed,
        periods,
        payload.get("scenario") or None,
        sample_path,
    )
    return {"job": job_id, "stages": [{"key": k, "label": v} for k, v in STAGES]}


def _integer(payload: dict[str, Any], name: str, *, default: int) -> int:
    """Read a JSON integer without silently turning invalid input into a default.

    ``value or default`` makes ``0`` look like an omitted value, and ``int``
    truncates a decimal. Both are surprising for a generation request and used
    to turn malformed browser input into an internal error later in a worker.
    """
    value = payload.get(name, default)
    if value is None:
        value = default
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        # OverflowError, not only ValueError: `int(nan)` raises ValueError but
        # `int(inf)` raises OverflowError, which is an ArithmeticError and walks
        # straight past a `(TypeError, ValueError)` clause. Infinity reaches here
        # because `json.loads` accepts the non-standard `Infinity` and `NaN`
        # literals by default, so a browser can post either. The two look like
        # one case and are not.
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    return parsed


def _positive_int(payload: dict[str, Any], name: str, *, default: int) -> int:
    value = _integer(payload, name, default=default)
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _non_negative_int(payload: dict[str, Any], name: str, *, default: int) -> int:
    value = _integer(payload, name, default=default)
    if value < 0:
        raise ValueError(f"{name} must be at least 0")
    return value


def _over_the_ceiling(records: int, periods: int) -> list[str]:
    """Whether a requested run exceeds what a shared instance will do.

    Both ceilings are unset unless someone deployed this for other people, so a
    local run is never told what it may ask for.
    """
    problems = []
    if MAX_RECORDS is not None and records > MAX_RECORDS:
        problems.append(
            f"This instance generates up to {MAX_RECORDS:,} entities at a time, and this run "
            f"asks for {records:,}. Run it locally for more — `pip install sdd` and `sdd ui`."
        )
    if MAX_PERIODS is not None and periods > MAX_PERIODS:
        problems.append(
            f"This instance ages up to {MAX_PERIODS} periods, and this run asks for {periods}."
        )
    # MAX_ROWS is deliberately not checked here.
    #
    # The obvious pre-check is `records * periods > MAX_ROWS`, and it is wrong in
    # the direction that matters. Entities reaching a terminal state stop
    # producing rows, so that product is an *upper* bound on a closed pool, not a
    # lower one: measured, 50,000 entities over 60 periods yields 2,033,298 rows,
    # not 3,000,000. A pre-check on it refuses runs that would have fitted, and
    # the refusal is unanswerable — the person asking cannot know the survival
    # curve. Originations break the bound the other way, so it is not reliable in
    # either direction.
    #
    # The ageing loop counts rows as it writes them and stops at the period that
    # crosses the line, so the work wasted by not rejecting early is bounded by
    # the ceiling itself. A late but correct limit beats an early wrong one.
    return problems


def _execute(
    job_id: str,
    spec: dict[str, Any],
    out_dir: Path,
    num_records: int,
    seed: int,
    periods: int | None,
    scenario: str | None,
    sample_path: str | None,
) -> None:
    def progress(stage: str, fraction: float) -> None:
        with _jobs_lock:
            job = _jobs.get(job_id)
            if not job:
                return
            job["stage"] = stage
            job["step"] = _stage_of(stage)
            job["progress"] = round(fraction, 4)
            # Straight-line extrapolation from elapsed time. Crude, and honest
            # about being crude: it is a countdown, not a promise.
            elapsed = time.time() - job["started"]
            job["eta_seconds"] = (
                round(elapsed * (1 - fraction) / fraction, 1) if fraction > 0.02 else None
            )

    with _jobs_lock:
        _jobs[job_id].update(status="running", step="configuring")

    try:
        result = api.run(
            spec,
            num_records,
            out_dir,
            seed=seed,
            periods=int(periods) if periods else None,
            scenario=scenario,
            sample=sample_path,
            validate_output=True,
            progress=progress,
            max_rows=MAX_ROWS,
        )
        # Hand the browser workspace-relative names, never absolute paths.
        root = _workspace().resolve()
        result["files"] = [str(Path(f).resolve().relative_to(root)) for f in result["files"]]
        if result.get("panel"):
            result["panel"] = str(Path(result["panel"]).resolve().relative_to(root))
        result["out_dir"] = str(out_dir.resolve().relative_to(root))
        with _jobs_lock:
            _jobs[job_id].update(
                status="done",
                progress=1.0,
                stage="done",
                step="packaging",
                eta_seconds=0,
                result=result,
            )
    except RowLimitExceeded as exc:
        # The periods written before the ceiling was hit are of no use to anyone
        # and would sit in a shared workspace until the instance restarts. A
        # limit that stops the run but keeps its output is only half a limit.
        shutil.rmtree(out_dir, ignore_errors=True)
        with _jobs_lock:
            _jobs[job_id].update(status="error", error=str(exc))
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id].update(status="error", error=f"{type(exc).__name__}: {exc}")


@app.get("/api/run/{job_id}")
def get_run(job_id: str) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "no such job")
        # The spec is held for exports, not for the browser, which already has it.
        return {k: v for k, v in job.items() if k != "spec"}


# ---------------------------------------------------------------------------
# step 5 — results
# ---------------------------------------------------------------------------


@app.get("/api/charts/{job_id}")
def charts(job_id: str, columns: str | None = None) -> dict[str, Any]:
    """Every chart for a finished run, aggregated server-side."""
    job = _finished(job_id)
    panel = job["result"].get("panel")
    if not panel:
        raise HTTPException(400, "this run wrote no panel to chart")

    reference = _source_field(job.get("source"), "sample_path")
    try:
        return api.charts(
            job["spec"],
            _safe_path(_workspace(), panel),
            reference=reference,
            columns=[c for c in (columns or "").split(",") if c] or None,
            # A series chart plots the figure the report already carries rather
            # than recomputing it, so the two cannot disagree on screen.
            metrics=job["result"].get("metrics") or None,
        )
    except Exception as exc:
        raise HTTPException(400, f"could not build the charts: {exc}") from exc


@app.get("/api/table/{job_id}")
def table(
    job_id: str,
    search: str | None = None,
    sort: str | None = None,
    descending: bool = False,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """A searchable, sortable page of the generated panel."""
    job = _finished(job_id)
    panel = job["result"].get("panel")
    if not panel:
        raise HTTPException(400, "this run wrote no panel")
    return api.table(
        _safe_path(_workspace(), panel),
        search=search,
        sort=sort,
        descending=descending,
        offset=max(0, int(offset)),
        limit=max(1, min(int(limit), 500)),
    )


def _finished(job_id: str) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "no such job")
    if job["status"] != "done" or not job.get("result"):
        raise HTTPException(409, f"this run is {job['status']}, not finished")
    return job


# ---------------------------------------------------------------------------
# step 6 — download
# ---------------------------------------------------------------------------


@app.get("/api/export/{job_id}")
def export(job_id: str, format: str = "csv") -> FileResponse:
    """Produce one of the five downloads and serve it."""
    from sdd.export import ExportError

    job = _finished(job_id)
    result = job["result"]
    panel = result.get("panel")

    try:
        produced = api.export(
            format,
            panel=_safe_path(_workspace(), panel) if panel else None,
            spec=job["spec"],
            result=result,
            out_dir=_safe_path(_workspace(), result["out_dir"]),
            stem=_slug(result.get("spec") or "synthetic_data"),
        )
    except ExportError as exc:
        raise HTTPException(400, str(exc)) from exc

    return FileResponse(
        produced["path"], filename=produced["name"], media_type=produced["media_type"]
    )


@app.get("/api/download")
def download(path: str) -> FileResponse:
    resolved = _safe_path(_workspace(), path)
    return FileResponse(resolved, filename=resolved.name)


@app.get("/api/preview")
def preview(path: str, rows: int = 25) -> dict[str, Any]:
    """First few rows of an output file, for the results table."""
    from sdd.profile import read_sample

    resolved = _safe_path(_workspace(), path)
    frame = read_sample(resolved, max_rows=max(1, min(rows, 200)))
    return {
        "columns": list(frame.columns),
        "rows": frame.astype(object).where(frame.notna(), None).values.tolist(),
    }


class RevalidatedStatic(StaticFiles):
    """Static files that a browser must revalidate before reusing.

    Without this the page is served with no cache directives at all, and a
    browser is then free to invent a freshness lifetime and keep an old
    stylesheet for hours — which it does. Editing `styles.css` and reloading
    then shows the previous design, and the mismatch is invisible because the
    HTML and the JavaScript may or may not be equally stale.

    ``no-cache`` does not mean "do not cache": it means "ask me first". The
    ETag that :class:`StaticFiles` already sends still turns an unchanged file
    into a 304 with no body, so this costs one conditional request per asset and
    guarantees the page you are looking at is the code on disk.
    """

    async def get_response(self, path: str, scope: Any) -> Any:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
        return response


# Mounted last so /api/* wins.
app.mount("/", RevalidatedStatic(directory=STATIC, html=True), name="static")


def serve(host: str = "127.0.0.1", port: int = 8000, *, reload: bool = False) -> None:
    """Run the UI. Binds to localhost only unless told otherwise."""
    import uvicorn

    _report_pack_problems()
    uvicorn.run("sdd.web.app:app" if reload else app, host=host, port=port, reload=reload)


def _report_pack_problems() -> None:
    """Say, before anything else, if a bundled pack will not load.

    Silence here is what made the failure so hard to place: the picker listed a
    pack, clicking it returned a 500, and the only clue was a pydantic message
    in a log about extra inputs. Whatever the cause, the person starting the
    server should hear it from the server.
    """
    problems = api.pack_problems()
    if not problems:
        return

    print("\n  One or more bundled packs will not load:\n", file=sys.stderr)
    for name, reasons in problems.items():
        print(f"    {name}", file=sys.stderr)
        for reason in reasons:
            print(f"      - {reason}", file=sys.stderr)
    print(
        "\n  The interface will show these as unavailable rather than failing when clicked.\n",
        file=sys.stderr,
    )
