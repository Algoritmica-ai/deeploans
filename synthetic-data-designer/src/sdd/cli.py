"""Command line interface — a thin wrapper over :mod:`sdd.api`.

Deliberately thin. Everything the CLI can do, the API can do, which is what
keeps the eventual web layer honest: if a command needs logic that is not in
``api``, the logic is in the wrong place.

    sdd ui                                open the local web UI
    sdd packs                             list the bundled asset-class packs
    sdd profile SAMPLE                    analyse a tape, print what it found
    sdd design SAMPLE -o spec.yaml        analyse it and write a runnable spec
    sdd check SPEC                        validate a spec without running it
    sdd run SPEC -n 50000 -o ./out        generate and age
    sdd validate SPEC PANEL               check a panel against its spec
    sdd fidelity REFERENCE SYNTHETIC      score synthetic against real
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

import typer

from sdd import __version__, api

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Spec-driven synthetic data designer for structured finance.",
)


def _echo_json(payload: object) -> None:
    typer.echo(json.dumps(payload, indent=2, default=str))


def _bar(stage: str, fraction: float) -> None:
    width = 28
    filled = int(fraction * width)
    sys.stderr.write(
        f"\r  [{'#' * filled}{'.' * (width - filled)}] {fraction:4.0%}  {stage[:38]:<38}"
    )
    sys.stderr.flush()
    if fraction >= 1.0:
        sys.stderr.write("\n")


@app.command()
def ui(
    host: Annotated[str, typer.Option(help="Bind address. Localhost by default.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to serve on.")] = 8000,
    reload: Annotated[bool, typer.Option("--reload", help="Auto-reload on code changes.")] = False,
) -> None:
    """Open the local web UI: upload a tape, edit the spec, run it, inspect the result."""
    try:
        from sdd.web.app import serve
    except ImportError as exc:
        typer.secho(
            "the web UI needs a couple of extra packages:\n  pip install 'sdd[web]'",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from exc

    typer.secho(f"\n  Synthetic Data Designer  ->  http://{host}:{port}\n", fg=typer.colors.GREEN)
    serve(host=host, port=port, reload=reload)


@app.command()
def version() -> None:
    """Print the version."""
    typer.echo(f"sdd {__version__}")


@app.command()
def packs() -> None:
    """List the bundled asset-class packs."""
    found = api.list_packs()
    if not found:
        typer.echo("no packs bundled")
        raise typer.Exit(0)
    for name in found:
        info = api.check(name)["spec"]
        typer.echo(
            f"  {name:<28} {info['asset_class']:<10} "
            f"{info['columns']:>3} columns, {info['periods']:>3} periods"
        )


@app.command()
def profile(
    sample: Annotated[Path, typer.Argument(help="CSV or parquet tape to analyse.")],
    id_column: Annotated[
        str | None, typer.Option("--id", help="Override entity id detection.")
    ] = None,
    time_column: Annotated[
        str | None, typer.Option("--time", help="Override cut-off detection.")
    ] = None,
    max_rows: Annotated[int | None, typer.Option(help="Read at most this many rows.")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Emit the full profile as JSON.")] = False,
) -> None:
    """Analyse a sample tape and report what it found."""
    from sdd.profile import profile_dataset

    result = profile_dataset(
        sample, id_column=id_column, time_column=time_column, max_rows=max_rows
    )
    if as_json:
        _echo_json(result.to_dict())
    else:
        typer.echo(result.summary())
        for key, why in result.detection_notes.items():
            typer.echo(f"  {key:<12}{why}")


@app.command()
def design(
    sample: Annotated[Path, typer.Argument(help="CSV or parquet tape to analyse.")],
    out: Annotated[Path, typer.Option("--out", "-o", help="Where to write the spec.")] = Path(
        "spec.yaml"
    ),
    structure: Annotated[
        Path | None, typer.Option(help="Taxonomy JSON, CSV header, or data dictionary.")
    ] = None,
    name: Annotated[str, typer.Option(help="Name for the generated spec.")] = "profiled",
    id_column: Annotated[str | None, typer.Option("--id")] = None,
    time_column: Annotated[str | None, typer.Option("--time")] = None,
    periods: Annotated[
        int | None, typer.Option(help="Cut-offs to generate. Defaults to the sample's.")
    ] = None,
) -> None:
    """Analyse a sample and write a runnable spec."""
    result = api.design(
        sample,
        structure=structure,
        name=name,
        out=out,
        id_column=id_column,
        time_column=time_column,
        periods=periods,
    )
    typer.echo(result["summary"])
    typer.echo(f"\nwrote {result['spec_path']}")
    if result["needs_review"]:
        typer.secho(
            f"\n{len(result['needs_review'])} column(s) need review before you trust them:",
            fg=typer.colors.YELLOW,
        )
        for column in result["needs_review"][:12]:
            typer.echo(f"  - {column}")
        typer.echo("\nEach carries a `review:` note in the spec explaining what to check.")


@app.command()
def check(
    spec: Annotated[str, typer.Argument(help="Spec file, or a bundled pack name.")],
) -> None:
    """Validate a spec without running it."""
    result = api.check(spec)
    if not result["valid"]:
        typer.secho("spec is not valid:", fg=typer.colors.RED)
        for problem in result["problems"]:
            typer.echo(f"  {problem}")
        raise typer.Exit(1)

    info = result["spec"]
    typer.secho(f"{info['name']} is valid", fg=typer.colors.GREEN)
    typer.echo(f"  asset class  {info['asset_class']}")
    typer.echo(f"  columns      {info['columns']}")
    typer.echo(f"  periods      {info['periods']}")
    typer.echo(f"  lifecycle    {'yes' if info['has_lifecycle'] else 'no'}")
    typer.echo(f"  scenarios    {', '.join(info['scenarios']) or 'none'}")
    typer.echo(f"  hash         {info['hash']}")
    if info["needs_review"]:
        typer.secho(f"  review       {', '.join(info['needs_review'][:8])}", fg=typer.colors.YELLOW)


@app.command()
def run(
    spec: Annotated[str, typer.Argument(help="Spec file, or a bundled pack name.")],
    num_records: Annotated[
        int, typer.Option("--num-records", "-n", help="How many entities.")
    ] = 10_000,
    out_dir: Annotated[Path, typer.Option("--out", "-o", help="Output directory.")] = Path("./out"),
    seed: Annotated[int, typer.Option(help="Random seed. Same seed, same data.")] = 42,
    periods: Annotated[
        int | None, typer.Option(help="Override the spec's number of cut-offs.")
    ] = None,
    scenario: Annotated[
        str | None, typer.Option(help="Named stress overlay from the spec.")
    ] = None,
    backend: Annotated[str, typer.Option(help="Sampling backend: numpy or nemo.")] = "numpy",
    skip_validation: Annotated[bool, typer.Option("--skip-validation")] = False,
    quiet: Annotated[bool, typer.Option("--quiet", "-q", help="No progress bar.")] = False,
) -> None:
    """Generate a portfolio and age it into a panel."""
    try:
        result = api.run(
            spec,
            num_records,
            out_dir,
            seed=seed,
            periods=periods,
            scenario=scenario,
            backend=backend,
            validate_output=not skip_validation,
            progress=None if quiet else _bar,
        )
    except api.SddError as exc:
        typer.secho(str(exc), fg=typer.colors.RED)
        raise typer.Exit(1) from exc

    typer.echo(
        f"\n{result['spec']}: {result['entities']:,} entities x {result['periods']} periods"
        + (f" [{result['scenario']}]" if result["scenario"] else "")
    )
    typer.echo(
        f"  {result['surviving_entities']:,} survived to the final cut-off "
        f"({result['surviving_entities'] / max(result['entities'], 1):.1%})"
    )
    typer.echo(f"  {len(result['files'])} file(s) + {result['panel']}")
    typer.echo(
        f"  {result['timings']['book_seconds']:.1f}s book, "
        f"{result['timings']['ageing_seconds']:.1f}s ageing"
    )

    report = result.get("validation")
    if report:
        colour = typer.colors.GREEN if report["passed"] else typer.colors.RED
        typer.secho(
            f"  {report['total'] - report['failed']}/{report['total']} invariants passed", fg=colour
        )
        for failed in [c for c in report["checks"] if not c["passed"]]:
            typer.echo(f"    FAIL {failed['name']}: {failed['violations']:,} row(s)")
        if not report["passed"]:
            raise typer.Exit(1)


@app.command()
def validate(
    spec: Annotated[str, typer.Argument(help="Spec file, or a bundled pack name.")],
    panel: Annotated[Path, typer.Argument(help="Panel parquet or CSV to check.")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Check a panel against every invariant its spec implies."""
    result = api.validate(spec, panel)
    if as_json:
        _echo_json(result)
    else:
        typer.secho(
            result["summary"], fg=typer.colors.GREEN if result["passed"] else typer.colors.RED
        )
    if not result["passed"]:
        raise typer.Exit(1)


@app.command()
def fidelity(
    reference: Annotated[Path, typer.Argument(help="The real or reference tape.")],
    synthetic: Annotated[Path, typer.Argument(help="The generated tape.")],
    spec: Annotated[
        str | None, typer.Option(help="Spec, to name the id/time/state columns.")
    ] = None,
    id_column: Annotated[str | None, typer.Option("--id")] = None,
    time_column: Annotated[str | None, typer.Option("--time")] = None,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Score synthetic data against the sample it was meant to resemble."""
    result = api.fidelity(
        reference, synthetic, spec=spec, id_column=id_column, time_column=time_column
    )
    if as_json:
        _echo_json(result)
    else:
        typer.secho(
            result["summary"], fg=typer.colors.GREEN if result["passed"] else typer.colors.YELLOW
        )


if __name__ == "__main__":
    app()
