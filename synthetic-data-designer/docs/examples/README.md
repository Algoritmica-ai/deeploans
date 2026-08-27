# What a run produces

Real output from a real run, committed so the artefacts can be read without
installing anything. Regenerate with:

```bash
sdd run clo_eu_leveraged_loans -n 500 --seed 42 -o ./out
```

| file | what it is |
|---|---|
| [`validation_report.json`](validation_report.json) | every automated check and its result — **GitHub renders this one** |
| [`validation_report.html`](validation_report.html) | the same report, formatted. Download and open it; GitHub shows HTML as source |
| [`run_manifest.json`](run_manifest.json) | what produced the data: configuration hash, seed, method, versions |

## The run

European CLO pack, 500 facilities over 36 monthly cut-offs — **15,995 rows**.

## What the validation report contains

**59 automated checks, all passing.** Nineteen are hand-written SQL invariants
specific to this asset class; the other forty are generated from the spec itself, so
declaring a column static produces a check that it never changes:

| | |
|---|---:|
| custom SQL invariants | 19 |
| column domains respected | 14 |
| plausibility bands | 11 |
| group attributes stable | 6 |
| structural (ids unique, terminal states absorb, …) | 4 |
| state fields applied | 3 |
| counters step correctly | 2 |
| **total** | **59** |

Two things worth knowing about them.

**They are derived from the configuration, not written separately.** A spec that
declares an obligor group generates checks that the obligor's attributes never
disagree across its facilities. There is no second document to keep in step.

**Every check has a negative-control test proving it can fail.** A check that cannot
fail is decoration, so the test suite deliberately breaks each condition and asserts
the check catches it — see
[`tests/test_invariants.py`](../../tests/test_invariants.py).

## What the manifest is for

Reproducibility, stated rather than claimed. It records the configuration hash, the
random seed, the generation method and the library versions, so the same inputs
produce the same data — the same rows, in the same order, with the same values.

That is what lets someone re-derive a dataset months later and get the file they were
given, which is the question an auditor asks first.
