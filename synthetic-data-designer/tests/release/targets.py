"""Where a release test runs: this process, or a deployed instance.

Every other test in this repo imports the library and calls it directly, which
proves the code is right. It does not prove the thing at
``huggingface.co/spaces/...`` is right, because between the two sits a Docker
build, a copy script, a different Python, a different filesystem and an HTTP
layer — and nothing crosses those.

That gap is not hypothetical. Group detection passed 671 local tests and
returned nothing at all on the deployed Space, because the web route hands
``build_spec`` a *path* while every test handed it a *DataFrame*. The feature
was dead on both routes a user actually takes, and silently, since a spec with
no groups is a perfectly valid spec. It was found by hand.

So the §31 tests are written once against this interface and run against either
target. The local target keeps them fast enough for CI; the remote target is
what the specification actually asks for.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

# §31 Test A fixes the population at 500 facilities, and B, C and D reuse it.
FACILITIES_HINT = 500

# A run against a deployed instance is minutes, not seconds: the request queues
# behind a worker pool of one on a two-core box.
REMOTE_TIMEOUT_SECONDS = 900
POLL_SECONDS = 3.0


@dataclass
class RunResult:
    """One completed run, however it was produced.

    Deliberately narrow. Anything a test can reach here is available from both
    targets, so a test cannot quietly become local-only by reading a field the
    HTTP interface does not expose.
    """

    panel: pd.DataFrame
    metrics: pd.DataFrame
    validation: dict[str, Any]
    entities: int
    periods: int
    total_rows: int
    spec_hash: str | None

    @property
    def invariants_passed(self) -> bool:
        return bool(self.validation.get("passed"))

    @property
    def failing_checks(self) -> list[str]:
        return [c["name"] for c in self.validation.get("checks", []) if not c.get("passed", True)]


class Target:
    """Something that can run a pack and hand back the result."""

    name = "target"

    def run(
        self,
        pack: str,
        *,
        entities: int,
        periods: int | None = None,
        seed: int,
        scenario: str | None = None,
        spec_overrides: dict[str, Any] | None = None,
    ) -> RunResult:
        raise NotImplementedError


class LocalTarget(Target):
    """Runs in this process, against the working tree."""

    name = "local"

    def __init__(self, tmp_path: Path):
        self._tmp = tmp_path
        self._counter = 0

    def run(
        self,
        pack: str,
        *,
        entities: int,
        periods: int | None = None,
        seed: int,
        scenario: str | None = None,
        spec_overrides: dict[str, Any] | None = None,
    ) -> RunResult:
        from sdd import api

        spec = api.load(pack).model_dump(mode="json", exclude_none=True, by_alias=True)
        if periods:
            spec["entity"]["calendar"]["periods"] = periods
        _apply(spec, spec_overrides)

        self._counter += 1
        out = self._tmp / f"run{self._counter}"
        result = api.run(spec, entities, out, seed=seed, scenario=scenario, validate_output=True)

        return RunResult(
            panel=pd.read_parquet(result["panel"]),
            metrics=pd.DataFrame(result["metrics"]),
            validation=result["validation"],
            entities=result["entities"],
            periods=result["periods"],
            total_rows=result["total_rows"],
            spec_hash=result.get("spec_hash"),
        )


class SpaceTarget(Target):
    """Drives a deployed instance over HTTP, as a browser would.

    The panel comes back as a parquet export rather than through the paged table
    endpoint. Paging would work and would be slower and lossier — the table
    endpoint formats for display — and the export is the file a user downloads,
    which is the thing worth testing.
    """

    name = "space"

    def __init__(self, base_url: str):
        import httpx

        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=120.0, follow_redirects=True)

    def close(self) -> None:
        self._client.close()

    def meta(self) -> dict[str, Any]:
        response = self._client.get("/api/meta")
        response.raise_for_status()
        return response.json()

    def load(self, pack: str) -> dict[str, Any]:
        response = self._client.get(f"/api/packs/{pack}")
        response.raise_for_status()
        payload = response.json()
        return payload.get("spec") or payload

    def run(
        self,
        pack: str,
        *,
        entities: int,
        periods: int | None = None,
        seed: int,
        scenario: str | None = None,
        spec_overrides: dict[str, Any] | None = None,
    ) -> RunResult:
        spec = self.load(pack)
        if periods:
            spec["entity"]["calendar"]["periods"] = periods
        _apply(spec, spec_overrides)

        body: dict[str, Any] = {"spec": spec, "num_records": entities, "seed": seed}
        if periods:
            body["periods"] = periods
        if scenario:
            body["scenario"] = scenario

        started = self._client.post("/api/run", json=body)
        if started.status_code != 200:
            raise AssertionError(f"the deployed instance refused the run: {started.text[:400]}")
        job = started.json()["job"]

        deadline = time.monotonic() + REMOTE_TIMEOUT_SECONDS
        state: dict[str, Any] = {}
        while time.monotonic() < deadline:
            time.sleep(POLL_SECONDS)
            state = self._client.get(f"/api/run/{job}").json()
            if state.get("error"):
                raise AssertionError(f"the deployed run failed: {state['error'][:400]}")
            if state.get("status") == "done":
                break
        else:
            raise AssertionError(
                f"the deployed run did not finish inside {REMOTE_TIMEOUT_SECONDS}s "
                f"(stage {state.get('stage')!r}, {state.get('progress')})"
            )

        result = state["result"]
        export = self._client.get(f"/api/export/{job}", params={"format": "parquet"})
        export.raise_for_status()

        return RunResult(
            panel=pd.read_parquet(io.BytesIO(export.content)),
            metrics=pd.DataFrame(result.get("metrics") or []),
            validation=result.get("validation") or {},
            entities=result["entities"],
            periods=result["periods"],
            total_rows=result["total_rows"],
            spec_hash=result.get("spec_hash"),
        )


def _apply(spec: dict[str, Any], overrides: dict[str, Any] | None) -> None:
    """Shallow-merge overrides into a spec, one level deep.

    Enough for what §31 needs — a shorter reinvestment window, a lifted
    prepayment rate — and no more, because a deep merge would let a test change
    something it did not mean to and still pass.
    """
    for key, value in (overrides or {}).items():
        if isinstance(value, dict) and isinstance(spec.get(key), dict):
            spec[key].update(value)
        else:
            spec[key] = value
