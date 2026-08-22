# Profiler backwards: items 1–3

**Branch** `profiler-backwards-1-to-3` · **Suite** 633 passed, 1 skipped · **Ruff** `check` and
`format --check` both clean on exit code.

## What "backwards" means

The designer normally runs **forwards**: you write a YAML spec, it generates a panel. The profiler
runs it **backwards**: you hand it a panel — a real loan tape, or one of our own outputs — and it
works out a spec that would produce something like it. That spec is what the web UI shows you after
you upload a file.

The round trip is the honest test of it: run a pack, feed the output back to the profiler, and see
how much of the original spec comes back. Before this branch:

```
                 original -> relearned
groups                  1 -> 0
secondary chains        1 -> 0
entity.targets          1 -> 0
metrics                19 -> 0
results.charts          4 -> 0
```

Six items were listed from that gap. This branch does the first three.

## A correction to what I told you earlier

I said of items 2 and 3 that *"they're in the spec the panel came from, and the profiler simply
doesn't carry them across. That's copying, not learning."*

That is wrong, and the distinction matters for what these features can honestly claim.
`build_spec(panel, ...)` receives **a DataFrame and nothing else**. There is no source spec sitting
beside it to copy from — when you upload your own tape there is no source spec at all. So:

- **Targets are genuinely inferable.** The tape states what the book was worth on day one. Reading
  it off is measurement.
- **Metrics and charts are not.** Which figures matter is a judgement no tape records. A tape does
  not say that its owner cares about weighted average spread rather than weighted average life; both
  are computable from the same columns and only one is the point.

So item 3 *learns*. Item 2 *proposes*. The code says which is which in each function's own docstring,
because a reader who confuses them will over-trust the output.

---

## Item 1 — the numeric guard (done, verified)

**The bug.** Regenerating the auto-loan pack picked `interest_rate_type` — a column holding the word
"Fixed" — as the loan's interest rate, in preference to `current_interest_rate_pct`, which holds 4.2.
The match was on the column *name* containing "rate", and "interest_rate_type" contains "rate".

**The fix.** `detect_by_name()` gained a `numeric_only` flag, backed by a new `_holds_numbers()`
helper that judges a column **on its values, not its declared type**:

```python
def _holds_numbers(series: pd.Series) -> bool:
    numeric = pd.to_numeric(series, errors="coerce")
    observed = series.notna().sum()
    return bool(observed) and numeric.notna().sum() / observed > 0.95
```

The dtype cannot be trusted here: a tape read from CSV arrives as `object`, and a perfectly good rate
column would be rejected for having been parsed loosely. So the test is whether the values convert.

**Verified.**

```
rate column  original : current_interest_rate_pct
rate column  relearned: current_interest_rate_pct
regenerates  : 2,349 rows
```

---

## Item 3 — targets (learned)

### What a target is

Generators draw each loan independently, so a portfolio's total is whatever the draws happen to sum
to. A real deal has a **size** — €500m of collateral, not "however much 500 loans came to". A target
is how a spec says so, and it works by scaling the *generator* rather than rescaling drawn values, so
that assets bought later by a reinvesting pool come out the same size as the original ones.

### What was learned

The opening total, read off the first cut-off. All three packs now come back with it exactly:

| pack | column | target | observed opening | regenerated |
|---|---|---:|---:|---:|
| CLO | `current_par` | 260,664,808 | 260,664,808 | 275,383,307 (+5.6%) |
| Green loans | `current_balance` | 71,201,279 | 71,201,279 | 70,819,144 (−0.5%) |
| Auto | `current_principal_balance` | 4,104,144 | 4,104,144 | −3.9% |

The regeneration gap is ordinary sampling error at 250 entities, and it is what the feature has always
documented: *this aims; it does not guarantee*.

### Two bugs found on the way

**The entity count was the wrong one.** First run produced a CLO target of **560,950,666 over 538
facilities** against an observed opening book of **260,664,808 over 250**. `profile.entities` counts
every entity the panel ever *met* — and a CLO reinvests, buying collateral as loans repay, so it met
538 facilities in a 250-facility deal. Multiplying the opening mean by 538 states a portfolio that
never existed at any one moment.

The profiler now records `opening_entities` separately, and a test pins the two apart so they cannot
quietly collapse into one:

```python
assert profile.opening_entities == ENTITIES
assert profile.entities > profile.opening_entities
```

**Beta had no closed-form mean.** `apply_targets` refuses a generator whose mean it cannot compute,
and the green-loan pack's balance fits a **beta** distribution — which the table did not cover, so
that pack got no target at all despite being perfectly scalable. Added:

```python
if gen.dist == "beta":
    a, b = float(params.get("a", 1.0)), float(params.get("b", 1.0))
    return loc + scale * a / (a + b) if a + b > 0 else None
```

### Why a target is only emitted where it will work

`apply_targets` **raises** on a generator with no closed-form mean. A target learned onto a resampled
column would therefore not fail the run that wrote it — it would fail every run afterwards, which is
the worst possible place to put a failure. So the profiler checks `_expected_value` first and stays
silent rather than emitting one that breaks later. A second test guards against the guard becoming a
blanket refusal.

### Negative control

A target that agrees with the fit is indistinguishable from a target that is ignored — both
regenerate the same book. So the test asks for three times the size:

```
target x1.0: asked 6,308,191   got 6,283,645
target x3.0: asked 18,924,574  got 18,850,935
```

The book follows. The target is doing work, not decorating the spec.

---

## Item 2 — metrics and charts (proposed)

### What these are

A **metric** is one figure computed for every cut-off — what the book is worth, how many assets are
in it, what it earns, how much of it is in trouble. They land in `portfolio_metrics.csv` beside the
panel. A **chart** draws one of them on the results screen.

### What is now proposed

Four metrics, being the ones that hold for any book with a balance column:

| metric | kind | means |
|---|---|---|
| `total_balance` | sum | what the book is worth |
| `active_entities` | count | how many assets are reporting |
| `wa_rate` | weighted mean | the coupon, weighted by balance |
| `non_performing_pct` | share where | balance outside the healthy state |

and three charts drawn from them: portfolio balance, state mix, non-performing share. Each carries an
`explain` note behind an information icon — the same mechanism added for CCC on the CLO pack, since a
chart labelled "non-performing share" means nothing to a reader who has not met the vocabulary.

Charts plot the **metric**, not a re-aggregation of the panel, so the line on the screen is the number
in the report rather than a second calculation that might disagree with it.

### Verified end to end

```
 period       date  total_balance  active_entities  wa_rate  non_performing_pct
      0 2024-01-31     6165993.34            400.0   5.0324            0.025563
      1 2024-02-29     5979702.27            400.0   5.0353            0.027531
      2 2024-03-31     5748761.06            397.0   5.0433            0.032379
```

Validation is not enough for these — a metric naming a missing column, or weighting by one that holds
text, passes the schema and dies mid-run. So the tests regenerate the learned spec and read the
metrics table back, and pass the charts through `configured_charts` to confirm each produces a payload
with points in it rather than an empty box.

### The rate-column trap, again

`wa_rate` weights by balance and is taken over a rate column — which is exactly the column item 1's
bug picked wrongly. `_rate_column()` therefore checks numeric dtype and excludes names containing
`type`, `flag`, `code` and `index`, and a test asserts it across all three packs.

---

## Round trip after this branch

```
                 original -> relearned
groups                  1 -> 0     (item 6, deferred)
secondary chains        1 -> 0     (item 5, deferred)
entity.targets          1 -> 1     ✅
metrics                19 -> 4     ✅ proposed, not recovered
results.charts          4 -> 3     ✅ proposed, not recovered
```

Metrics read 4 against 19 and charts 3 against 4, and that is the intended outcome rather than a
shortfall. The CLO pack's 19 include WARF, diversity score and CCC bucket concentration — figures that
exist because a CLO manager is measured on them, not because they are visible in the rows. Proposing
them for an auto-loan tape would be worse than proposing nothing.

## What remains

| item | work |
|---|---|
| 4 — condition hazards | ~3 days |
| 5 — secondary chains | ~3 days |
| 6 — groups | ~1.5 weeks |

## Files

- `src/sdd/profile/panel.py` — `_holds_numbers()`, `numeric_only` (item 1)
- `src/sdd/profile/profiler.py` — `opening_entities`
- `src/sdd/profile/build.py` — `_build_targets`, `_build_metrics`, `_build_results`,
  `_balance_column`, `_rate_column`
- `src/sdd/generate/targets.py` — beta mean
- `tests/test_profiler_report.py` — 15 tests
