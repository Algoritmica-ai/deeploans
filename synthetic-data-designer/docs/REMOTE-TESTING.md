# Running the suite against a deployment

```bash
pytest                                                          # this working tree
pytest --release-target=https://algoritmica-synthetic-data-designer.hf.space
```

The second form redirects **every `api.run` in the suite** at the named
deployment — not only the six §31 release tests.

## Why the seam is the function, not the call site

The §31 tests were written against a `Target` abstraction. That works for six
tests. Around 290 others call `sdd.api.run` directly, in every shape the
signature allows, and rewriting each one would be a large diff whose only
content is plumbing — and would leave every *future* test local-only unless its
author remembered to opt in.

So `sdd.api.run` itself is replaced for the session. A test does not know, and
does not have to be told.

## What makes it transparent

Two things, and the second is the one that matters.

**The same keys.** The deployment computes its result with the same code and
returns all twenty-one fields over the wire, so nothing has to be reconstructed.

**The same files on disk.** Every artefact the remote run produced is downloaded
into the caller's own `out_dir`, at the same relative path — the panel, the
per-cut-off CSVs, `configuration.yaml`, `portfolio_metrics.*`,
`run_manifest.json`, both validation reports. A test doing
`pd.read_parquet(result["panel"])`, or asserting `tmp_path / "run_manifest.json"`
exists, cannot tell the difference.

Without that second half a test would receive a result whose `panel` names a
path on someone else's filesystem, and every `read_parquet` in the suite would
fail on a deployment for a reason that has nothing to do with the deployment.

## What it deliberately does not fake

Some differences between a working tree and a shared CPU instance are **real**,
and a harness that smoothed them over would be reporting on itself:

- the instance **always validates**, and exposes no way to ask it not to;
- it **caps rows and records**, and refuses a run above the ceiling;
- it has **no PyTorch**, so the deep methods are unavailable;
- training on a sample means **uploading a tape to a public shared workspace**.

Tests that depend on any of those carry `@pytest.mark.local_only("<reason>")`
and are skipped, with the reason printed. Kept as an explicit marker rather than
as silent tolerance inside the runner, so the set is countable and the reason
travels with the test.

## What it cannot cover, and why that is not a gap

Roughly half the suite never calls `api.run`: `test_samplers`, `test_deriver`,
`test_spec_loader`, `test_fidelity`, `test_randomness` and their neighbours test
pure functions. There is no HTTP surface for `ks_distance()`, and "running it
against a deployment" is not a coherent idea — the deployment imports the same
function from the same wheel.

What a deployment can get wrong is what it *serves*: the packs it bundles, the
runs it performs, the artefacts it writes. That is what this covers.

## Cost, and when to run it

A full pass is **hours**, against a one-worker pool on two cores, and it writes
every run into the Space's public shared workspace. That is a deliberate
trade — the Space is a public capability demo with temporary storage — but it
makes this a pre-release exercise rather than something for the normal loop.

For a quick check, the six §31 scenarios still run on their own in about two
minutes:

```bash
pytest -m release --release-target=<url>
```
