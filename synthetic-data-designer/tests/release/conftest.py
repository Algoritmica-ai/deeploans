"""Choosing where the release tests run.

By default they run in this process, which keeps them usable in CI. Pointing
`--release-target` at a URL runs the identical tests against that deployment,
which is what §31 asks for and the only way to exercise the Docker build, the
copy script and the HTTP layer at all.

The `--release-target` option itself is registered in the parent conftest:
pytest only reads `pytest_addoption` from the rootdir plugin, so declaring it
here silently did nothing and the flag came back as an unrecognised argument.
"""

from __future__ import annotations

import pytest
from tests.release.targets import FACILITIES_HINT, LocalTarget, SpaceTarget

PACK = "clo_eu_leveraged_loans"


@pytest.fixture(scope="session")
def target(request, tmp_path_factory):
    """The thing under test: this working tree, or a deployment."""
    choice = request.config.getoption("--release-target")
    if choice == "local":
        return LocalTarget(tmp_path_factory.mktemp("release"))

    remote = SpaceTarget(choice)
    try:
        meta = remote.meta()
    except Exception as exc:  # reported, never swallowed
        pytest.fail(f"could not reach {choice}: {exc}")

    packs = [p.get("name", p) if isinstance(p, dict) else p for p in meta.get("packs", [])]
    if PACK not in packs:
        pytest.fail(f"{choice} does not serve {PACK}; it has {packs}")
    if meta.get("pack_problems"):
        pytest.fail(f"{choice} reports pack problems: {meta['pack_problems']}")

    request.addfinalizer(remote.close)

    # Say what was reached, in the test output. "Did the release suite run
    # against the deployment?" should be answerable by reading the log rather
    # than by trusting that a flag was passed — a suite that silently fell back
    # to local would pass just as green and prove nothing.
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(
            f"release target: {choice} (version {meta.get('version', 'unknown')}, "
            f"packs {', '.join(packs)})",
            bold=True,
        )
    return remote


@pytest.fixture(scope="session")
def standard_run(target):
    """§31 Test A, shared by A, B, C and D so the run happens once.

    Session-scoped because against a deployment each run is minutes, and B, C
    and D all compare against the same base rather than needing their own.
    """
    return target.run(PACK, entities=FACILITIES_HINT, periods=36, seed=42)


@pytest.fixture(scope="session")
def adverse_run(target):
    return target.run(PACK, entities=FACILITIES_HINT, periods=36, seed=42, scenario="adverse")


@pytest.fixture(scope="session")
def severe_run(target):
    return target.run(PACK, entities=FACILITIES_HINT, periods=36, seed=42, scenario="severe")
