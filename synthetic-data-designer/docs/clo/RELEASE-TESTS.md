# §31 release tests

Six scenarios the specification says to run before a release. They exist as one
named suite that runs against **either** the working tree or a deployed
instance.

```bash
# against this working tree — fast, runs in CI
pytest -m release

# against a deployment — what §31 actually asks for
pytest -m release --release-target=https://algoritmica-synthetic-data-designer.hf.space
```

They are **deselected from the normal suite**, so `pytest` on its own reports
`6 deselected` and stays as fast as it was.

## Why "against a deployment" is the point

Every other test in this repo imports the library and calls it directly. That
proves the *code* is right. It does not prove the thing a user touches is right,
because between the two sits a Docker build, a copy script, a different Python,
a different filesystem and an HTTP layer — and no other test crosses any of them.

This is not hypothetical. Group detection passed 671 local tests and returned
**nothing at all** on the deployed Space: the web route hands `build_spec` a
*path* while every test handed it a *DataFrame*. The feature was dead on both
routes a real user takes, silently, because a spec with no groups is a perfectly
valid spec. It was found by poking the Space by hand.

## What each test asserts

| | scenario | expectation, and how it is made checkable |
|---|---|---|
| **A** | 500 facilities, 36 periods, seed 42, base | invariants all pass; turnover strictly between 0 and 10% a month; distress between 0.5% and 25%; **diversification read off `effective_obligors`**, not the plain count — a hundred obligors where two hold half the money is not a diversified book and the plain count cannot say so |
| **B** | adverse, same seed and population | more distress, more defaults, lower market value, **lower recovery *rate*** — see below |
| **C** | severe, same seed and population | at least **1.5×** the distress of adverse, largest realised losses, lowest average price |
| **D** | Test A run twice | same `spec_hash`, frames equal cell for cell, metrics tables identical |
| **E** | reinvestment window shortened to 6 | facilities join up to period 6, none after |
| **F** | severe with prepayment lifted 3× | at least three of the four terminal states exercised; **zero resurrections**; a terminal facility reported exactly once |

Two of these needed a judgement rather than a literal reading.

**Test B's "lower recoveries" is read as the recovery _rate_.** Adverse produces
roughly seven times the defaults, so gross recoveries *rise* even as each
default returns materially less — €30.6m in base against €155.1m in adverse, on
rates of 0.554 and 0.401. Read as the amount, the expectation would be close to
unsatisfiable: the rate would have to collapse almost to zero before the total
could fall. The rate is also what the pack declares (`recovery_multiplier` 0.8
and 0.55), so it is the reading the configuration can be checked against.

**Test C's "materially greater" is given a floor.** Three scenarios that ordered
correctly by a rounding error would satisfy a bare `>` and tell an investor
nothing, so severe must be at least half again as bad as adverse.

**Tests E and F change the calibration on purpose.** E shortens the reinvestment
window because a window ending at the edge of the panel cannot distinguish
"stopped correctly" from "the data ran out". F lifts the exit rates because at
the pack's own calibration a 36-period run matures a handful of facilities, and
a test that never sees an exit cannot show the exit works.

## How it is wired

`tests/release/targets.py` holds a `Target` with two implementations behind one
narrow `RunResult`. The result type is deliberately small: anything a test can
reach is available from both targets, so a test cannot quietly become local-only
by reading a field the HTTP interface does not expose.

The remote target drives the same endpoints a browser does — `GET /api/packs`,
`POST /api/run`, poll `GET /api/run/{job}`, then `GET /api/export?format=parquet`
for the panel. The export is used rather than the paged table endpoint because
it is the file a user actually downloads.

The suite prints what it reached:

```
release target: https://algoritmica-synthetic-data-designer.hf.space (version 0.1.0, packs …)
```

"Did the release suite run against the deployment?" should be answerable by
reading the log, not by trusting that a flag was passed — a suite that silently
fell back to local would pass just as green and prove nothing.

## Verified

Against `Algoritmica/synthetic-data-designer`, 21 Aug 2026: **6 passed.**

Checks that the run really left this machine:

| | |
|---|---|
| unreachable host | fails loudly — `could not reach …: 404 for /api/meta` |
| reachable host that is not the app | same, at the same guard |
| runtime, local | 12.7s |
| runtime, deployed | 83.4s |

The 6.6× gap is the network round-trips, remote generation behind a one-worker
pool, and three parquet downloads.

## Caveats

- **Slow.** Minutes against a deployment. This is a pre-release step, not part
  of the normal loop, which is why it is deselected by default.
- **It writes to a shared public workspace.** The Space is a public demo and its
  workspace is shared with every visitor. The suite uses bundled packs and
  generates synthetic data only — nothing sensitive should ever be added to it.
