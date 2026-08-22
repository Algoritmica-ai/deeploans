"""Run the suite's `api.run` calls against a deployed instance instead.

The §31 release tests were written against a `Target` abstraction, which works
because there are six of them. The other ~290 end-to-end tests call
`sdd.api.run` directly, in every shape the function allows, and rewriting each
call site would be a large diff whose only content is plumbing — and would leave
every *future* test local-only unless its author remembered.

So the seam is the function, not the call site. When `--release-target` names a
URL, `sdd.api.run` is replaced by one that drives the deployed HTTP API and
returns a result indistinguishable from the local one:

* the same twenty-one keys, because the deployment computes them with the same
  code and returns them over the wire;
* the same files on disk, because every artefact the remote run produced is
  downloaded into the caller's own `out_dir`, at the same relative path.

That second half is what makes it transparent. A test doing
`pd.read_parquet(result["panel"])` or asserting `tmp_path / "run_manifest.json"`
exists cannot tell the difference, and did not have to be told.

**What it deliberately does not fake.** Anything the deployment refuses is
allowed to fail: the row ceilings, the unavailable deep-learning methods, an
upload the instance will not take. Those are real differences between running
locally and running on a shared CPU box, and a harness that smoothed them over
would be reporting on itself rather than on the deployment.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from sdd.api import SddError

# A deployed run queues behind a worker pool of one on two cores.
TIMEOUT_SECONDS = 900
POLL_SECONDS = 2.0

# A long pass meets the occasional read timeout. Retried, because a blip in the
# network is not a finding about the deployment.
RETRIES = 4
RETRY_BACKOFF_SECONDS = 3.0

# Artefacts that sit beside the panel but are not listed in `files`.
EXTRA_ARTEFACTS = (
    "all_cutoffs.parquet",
    "configuration.yaml",
    "portfolio_metrics.parquet",
    "portfolio_metrics.csv",
    "run_manifest.json",
    "validation_report.json",
    "validation_report.html",
)


class RemoteRunError(SddError):
    """The deployment refused or failed a run.

    Subclasses the library's own error on purpose. A dozen tests assert that a
    bad request raises — `pytest.raises(api.SddError, match="no scenario")` and
    friends — and the deployment *does* refuse those, for the right reason, with
    the reason in the message. Raising an unrelated type would have failed those
    tests for a difference in plumbing rather than in behaviour, which is
    exactly the kind of false signal this harness must not produce.
    """


class RemoteRunner:
    """Stands in for `sdd.api.run`, against a deployed instance."""

    def __init__(self, base_url: str):
        import httpx

        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=180.0, follow_redirects=True)
        self._packs: dict[str, dict[str, Any]] = {}
        self.runs = 0

    def close(self) -> None:
        self._client.close()

    def _get(self, url: str, **kwargs: Any) -> Any:
        """A GET that survives a blip.

        A two-hour pass against a one-worker instance meets the occasional read
        timeout, and two tests failed on one in the first full run. A timeout is
        a fact about the network, not about the deployment's behaviour, and
        reporting it as a test failure would train the reader to ignore failures.
        Genuine refusals still surface: only transport errors are retried, and
        only a few times.
        """
        import httpx

        last: Exception | None = None
        for attempt in range(RETRIES):
            try:
                return self._client.get(url, **kwargs)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = exc
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
        raise RemoteRunError(f"{url} kept failing after {RETRIES} attempts: {last}")

    def meta(self) -> dict[str, Any]:
        response = self._client.get("/api/meta")
        response.raise_for_status()
        return response.json()

    # -- the api.run signature, exactly -------------------------------------

    def __call__(
        self,
        spec: Any,
        num_records: int,
        out_dir: str | Path,
        *,
        seed: int = 42,
        periods: int | None = None,
        scenario: str | None = None,
        backend: str = "numpy",
        sample: Any = None,
        validate_output: bool = True,
        progress: Any = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        if sample is not None:
            raise RemoteRunError(
                "this run trains on a sample tape, which would mean uploading it to a shared "
                "public instance; run it locally instead"
            )

        payload: dict[str, Any] = {
            "spec": self._as_dict(spec),
            "num_records": int(num_records),
            "seed": int(seed),
        }
        if periods:
            payload["periods"] = int(periods)
        if scenario:
            payload["scenario"] = scenario

        started = self._client.post("/api/run", json=payload)
        if started.status_code != 200:
            raise RemoteRunError(_message(started))

        job = started.json()["job"]
        result = self._await(job, progress)

        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        self.runs += 1
        return self._localise(result, out)

    # -- helpers -------------------------------------------------------------

    def _as_dict(self, spec: Any) -> dict[str, Any]:
        """A pack name, a path, or a spec dict — the same three the local call takes."""
        if isinstance(spec, dict):
            return spec
        name = str(spec)
        if name.endswith((".yaml", ".yml", ".json")):
            from sdd import api

            return api.load(name).model_dump(mode="json", exclude_none=True, by_alias=True)
        if name not in self._packs:
            response = self._get(f"/api/packs/{name}")
            if response.status_code != 200:
                raise RemoteRunError(f"the deployment does not serve pack {name!r}")
            payload = response.json()
            self._packs[name] = payload.get("spec") or payload
        return self._packs[name]

    def _await(self, job: str, progress: Any = None) -> dict[str, Any]:
        """Poll to completion, reporting progress through the caller's callback.

        Locally `progress` is invoked from inside the run; remotely the same
        information arrives by polling, so it is forwarded rather than dropped.
        Clamped monotonic and finished at exactly 1.0, because that is what the
        local contract promises and a caller drawing a progress bar from a
        number that went backwards would be right to complain.
        """
        deadline = time.monotonic() + TIMEOUT_SECONDS
        state: dict[str, Any] = {}
        highest = 0.0
        while time.monotonic() < deadline:
            time.sleep(POLL_SECONDS)
            state = self._get(f"/api/run/{job}").json()
            if progress is not None:
                fraction = float(state.get("progress") or 0.0)
                if fraction > highest:
                    highest = fraction
                    progress(state.get("stage") or "running", min(highest, 1.0))
            if state.get("error"):
                raise RemoteRunError(state["error"])
            if state.get("status") == "done":
                if progress is not None:
                    progress("done", 1.0)
                return state["result"]
        raise RemoteRunError(
            f"the deployed run did not finish inside {TIMEOUT_SECONDS}s "
            f"(stage {state.get('stage')!r})"
        )

    def _localise(self, result: dict[str, Any], out: Path) -> dict[str, Any]:
        """Pull the run's artefacts down and point the result at the local copies.

        Without this a test would get a result whose `panel` names a path on
        someone else's filesystem, and every `read_parquet` in the suite would
        fail on a deployment for a reason that has nothing to do with the
        deployment.
        """
        remote_dir = (result.get("out_dir") or "").rstrip("/")
        local = dict(result)

        wanted: list[str] = list(result.get("files") or [])
        if result.get("panel"):
            wanted.append(result["panel"])
        for name in EXTRA_ARTEFACTS:
            wanted.append(f"{remote_dir}/{name}" if remote_dir else name)
        # `artefacts` come back absolute; the download endpoint wants the path
        # relative to the workspace it serves.
        for path in (result.get("artefacts") or {}).values():
            text = str(path)
            if remote_dir and remote_dir in text:
                wanted.append(text[text.index(remote_dir) :])

        fetched: dict[str, str] = {}
        for remote_path in dict.fromkeys(wanted):
            relative = remote_path[len(remote_dir) :].lstrip("/") if remote_dir else remote_path
            if not relative:
                continue
            destination = out / relative
            if destination.exists():
                fetched[remote_path] = str(destination)
                continue
            response = self._get("/api/download", params={"path": remote_path})
            if response.status_code != 200:
                # Not every artefact exists for every spec — a pack with no
                # metrics writes no metrics file. Absence is the same answer
                # locally, so it is not an error.
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(response.content)
            fetched[remote_path] = str(destination)

        local["out_dir"] = str(out)
        if result.get("panel"):
            local["panel"] = fetched.get(result["panel"], str(out / "all_cutoffs.parquet"))
        local["files"] = [fetched.get(f, f) for f in (result.get("files") or [])]
        local["artefacts"] = {
            key: fetched.get(_relative(str(value), remote_dir), str(value))
            for key, value in (result.get("artefacts") or {}).items()
        }
        return local


def _relative(path: str, remote_dir: str) -> str:
    return path[path.index(remote_dir) :] if remote_dir and remote_dir in path else path


def _message(response: Any) -> str:
    try:
        payload = response.json()
    except Exception:
        return f"HTTP {response.status_code}: {response.text[:300]}"
    problems = payload.get("problems")
    detail = payload.get("error") or payload.get("detail") or ""
    return f"{detail} {problems if problems else ''}".strip() or f"HTTP {response.status_code}"
