"""Shared fixtures.

``minimal_spec_dict`` is deliberately small and asset-class-neutral: two states,
one hazard, one amortisation kernel. Tests that want to break something start
from it and mutate one field, which keeps each negative control readable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKS = REPO_ROOT / "packs"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Where the §31 release suite runs.

    Registered here rather than in `tests/release/conftest.py` because pytest
    reads `pytest_addoption` only from the rootdir plugin — declared in a
    subdirectory it is silently ignored, and the flag comes back as an
    unrecognised argument.
    """
    parser.addoption(
        "--release-target",
        action="store",
        default="local",
        help=(
            "'local' (default) runs in this process; a URL runs against that deployed "
            "instance instead, e.g. --release-target=https://example.hf.space. This "
            "redirects every `api.run` in the suite, not only the §31 release tests."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "local_only(reason): the deployment cannot exercise this; skipped when "
        "--release-target names a URL",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip what a deployment genuinely cannot run.

    Kept as an explicit marker with a written reason rather than as silent
    tolerance in the runner. Some differences between local and deployed are
    real — the instance always validates, it caps rows, it has no PyTorch — and
    a harness that papered over them would be reporting on itself. Marking them
    makes the set countable, and the reason travels with the test.
    """
    if config.getoption("--release-target") == "local":
        return
    for item in items:
        marker = item.get_closest_marker("local_only")
        if marker is not None:
            why = marker.args[0] if marker.args else "not available on a deployment"
            item.add_marker(pytest.mark.skip(reason=f"local only: {why}"))


@pytest.fixture(scope="session", autouse=True)
def _remote_target(request):
    """Point the whole suite at a deployment, when one is named.

    The seam is `sdd.api.run` itself rather than each call site. Roughly 290
    tests call it, in every shape the signature allows, and rewriting them all
    would be a large diff whose only content is plumbing — and would leave every
    *future* test local-only unless its author remembered to opt in.

    Patched for the session rather than per test, because each remote run is
    seconds of network and queueing and a per-test client would pay the
    handshake hundreds of times.
    """
    choice = request.config.getoption("--release-target")
    if choice == "local":
        yield None
        return

    from tests.remote_runner import RemoteRunner

    from sdd import api

    runner = RemoteRunner(choice)
    try:
        meta = runner.meta()
    except Exception as exc:
        pytest.exit(f"could not reach {choice}: {exc}", returncode=2)

    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(
            f"running against {choice} — version {meta.get('version', 'unknown')}, "
            f"packs {', '.join(str(p) for p in meta.get('packs', []))}",
            bold=True,
        )

    original = api.run
    api.run = runner
    try:
        yield runner
    finally:
        api.run = original
        runner.close()


@pytest.fixture
def minimal_spec_dict() -> dict[str, Any]:
    return {
        "spec_version": 1,
        "meta": {"name": "mini", "asset_class": "test"},
        "entity": {
            "id_column": "loan_id",
            "time_column": "as_of",
            "calendar": {"start": "2024-01-31", "periods": 4, "freq": "month_end"},
        },
        "params": {"deal_year": 2024},
        "constants": {"currency": "EUR"},
        "columns": [
            {
                "name": "loan_id",
                "role": "static",
                "dtype": "str",
                "generator": {"kind": "sequence", "prefix": "L", "width": 4},
            },
            {
                "name": "as_of",
                "role": "dynamic",
                "dtype": "str",
                "generator": {"kind": "constant", "value": "2024-01-31"},
            },
            {"name": "currency", "role": "constant"},
            {
                "name": "state",
                "role": "dynamic",
                "dtype": "category",
                "generator": {
                    "kind": "categorical",
                    "values": ["Performing", "Late"],
                    "weights": [0.9, 0.1],
                },
            },
            {
                "name": "balance",
                "role": "dynamic",
                "dtype": "float",
                "generator": {"kind": "uniform", "low": 100000, "high": 200000, "decimals": 2},
            },
            {
                "name": "rate",
                "role": "static",
                "dtype": "float",
                "generator": {"kind": "constant", "value": 3.0},
            },
            {
                "name": "payment",
                "role": "static",
                "dtype": "float",
                "generator": {"kind": "constant", "value": 900.0},
            },
            {
                "name": "term",
                "role": "dynamic",
                "dtype": "int",
                "generator": {"kind": "constant", "value": 300},
            },
            {"name": "arrears", "role": "derived", "dtype": "float"},
            {"name": "band", "role": "derived"},
        ],
        "derivations": [
            {"target": "arrears", "expr": "0.0"},
            {
                "target": "band",
                "kind": "bucket",
                "bucket": "b",
                "source": "balance",
                "stage": "both",
            },
        ],
        "buckets": {"b": {"bins": [0, 150000, 1000000], "labels": ["low", "high"]}},
        "lifecycle": {
            "state_column": "state",
            "states": ["Performing", "Late", "Redeemed"],
            "terminal": ["Redeemed"],
            "transitions": [[0.95, 0.05], [0.50, 0.50]],
            "hazards": [
                {
                    "kind": "bernoulli",
                    "name": "prepayment",
                    "annual_rate": 0.12,
                    "to_state": "Redeemed",
                }
            ],
            "state_fields": {"Redeemed": {"balance": 0.0}},
        },
        "dynamics": {
            "amortisation": {
                "kind": "annuity",
                "balance": "balance",
                "rate": "rate",
                "payment": "payment",
                "only_when_state": "Performing",
            },
            "counters": [{"column": "term", "step": -1, "clip_min": 0}],
            "accruals": [{"column": "arrears", "add": "payment", "when": "not_performing"}],
        },
        "emit": {
            "filename": "mini_{yyyymm}.csv",
            "column_order": ["loan_id", "as_of", "currency", "state", "balance", "arrears", "band"],
        },
    }


@pytest.fixture
def rmbs_spec_path() -> Path:
    return PACKS / "rmbs_nl_green_lion.yaml"
