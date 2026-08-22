# Profiler backwards: items 4–6

**Branch** `profiler-backwards-4-to-6` · **Suite** 652 passed, 1 skipped · **Ruff** `check` and
`format --check` both clean on exit code.

## What this closes

The profiler runs the designer backwards: hand it a panel and it works out a spec that would
produce something like it. Round-tripping our own packs is the honest test, and it had six gaps.
Items 1–3 shipped last week. These are the other three, and they were the hard ones.

```
                  original -> before -> after
condition hazards        1 ->    0   ->   1   ✅
secondary chains         1 ->    0   ->   1   ✅
groups                   1 ->    0   ->   1   ✅
```

All three recover **exactly** on the CLO pack, and — the harder half — produce **nothing** on the
two packs that declare none of it. False positives here are worse than misses: a missed group is
visible in the spec and can be added by hand, while a spurious one silently corrupts every
concentration figure the book is measured on and looks plausible doing it.

---

## Item 4 — maturity as a rule, not a chance

### What was wrong

A loan matures when *its own* maturity date arrives. Relearned as a flat monthly rate, a 96-month
facility gets the same chance of maturing in month three as a 60-month one. The spec still ran and
the state was still reachable, which is exactly why it went unnoticed — it was a *different rule*
producing plausible-looking data.

### Recovered exactly

| | original | relearned |
|---|---|---|
| kind | `condition` | `condition` |
| when | `months_to_maturity <= 1` | `months_to_maturity <= 1` |
| excluded | `[Defaulted]` | `[Defaulted]` |

Read from the **landing** row rather than the last row before the move. The engine advances counters
and then evaluates, so a facility whose countdown reads 2 on its last live row is at 1 when the rule
fires. Taking the earlier value would shorten every term by a month on each round trip.

### Three false positives, each instructive

**`current_balance <= 0` for prepayment** — true for 255 prepaid facilities out of 255, and
completely circular. Entering Prepaid is what *sets* the balance to zero. Regenerate with "prepay
when the balance reaches zero" and nothing ever prepays, because nothing reaches zero without
prepaying first. Prepayment would have vanished from the book silently.

Fixed by asking whether the entity **crossed** the threshold or the transition **put it there**:

> `months_to_maturity` reads 2, then 1 — one ordinary step of a counter that steps by one.
> `current_balance` reads four million, then zero. A step is a crossing; a cliff is an assignment.

**`days_past_due >= 180` for charge-off** — scores perfectly against a nine-month workout, for the
uninteresting reason that days past due *is* the workout clock in other units. Restating a clock as
a threshold on its own read-out adds nothing and hides the mechanism. Fixed by ordering: the dwell
test runs first, and only where no dwell spike exists does the condition get a hearing. A genuine
rule looks different — facilities mature after wildly varying spells in the performing state, so
there is no spike to explain them.

**Five defaulted facilities** dropped a perfect rule to 93% precision and lost the whole condition.
Their terms keep counting down through the workout, so they reach zero months to maturity and never
mature. They are not counter-examples, they are a carve-out — and the pack says so in as many words.
Recovering `excluded_states: [Defaulted]` is a better answer than loosening the threshold, because
it puts the exception in the spec where a reader can see it.

### One more bug, found by relearning a relearned spec

A second-generation panel held a **single** maturity, and "modal dwell = 18, on 100% of events"
wrote an eighteen-month fixed delay into the spec on a sample of one. The dwell test had no minimum
event count. It has one now; below it, a flat rate is the honest fallback — also badly measured from
three events, but it does not dress the guess up as a mechanism.

---

## Item 5 — the rating, migrating on its own

### What was wrong

A credit rating moves under its own steam, and normally moves *before* distress is visible: a
company is downgraded while still paying every instalment, which is the early warning the rating
exists to give. Relearned as an ordinary categorical column it is redrawn independently each period,
and a facility flickers between BB and CCC from one month to the next.

### Recovered

Column, all nine grades, `D` absorbing, and the coupling that keeps it honest:

```
forced_by: {Defaulted: D, Recovered: D}     ← exactly the original
```

`forced_by` matters more than it looks. Uncoupled, the two machines run independently and the output
carries BB-rated facilities sitting in default — nonsense that no invariant would catch.

The `stress` direction is measured as a ratio of worsening rates and comes back in the right
*direction* with a smaller magnitude (CCC 1.58 against a declared 3.2). That is expected: the
declared figure is applied on top of a matrix already tilted toward distress, while the measurement
sees only the net effect. Rows forced into a chain state are excluded from the measurement — the
rating is D *because* the facility defaulted, so measuring what D does to default risk would be
reading the arrow backwards.

### The false positives

The CLO panel offered **three** chains where the pack has one: `rating_at_cutoff` (nine grades),
`rating_bucket` (four) and `ccc_flag` (two). All three migrate, all three produce a clean matrix, and
two of them are the first with detail thrown away. Run independently they would drift apart and the
output would carry facilities rated B- whose bucket said CCC. The finer column wins, because it is
the one the others can be recovered from.

The auto and green packs offered five more — `balance_bucket`, `current_ltv_band`,
`cltomv_current_bucket` and friends. A balance bucket does migrate as a loan amortises; generated
independently it would report a 300k–350k band on a loan carrying 80k.

These were already known to the codebase as **derivations** — the bucket detector catches every one
of them. The problem was ordering: derivation detection ran *after* the panel learner, so the panel
learner saw them as untagged categorical columns. Moving it first fixed all five at once, and is
right beyond this feature: a band should follow the balance it was cut from, not run beside it.

---

## Item 6 — the obligor behind the facilities

The estimated 1.5 weeks, and the one where the surgery matters as much as the detection.

### What was wrong

A relearned CLO had **one company per facility**. Industry, country and revenue were drawn per
facility, so the same obligor came out in four industries at once, and every concentration figure —
largest obligor, top ten, single industry — became meaningless.

### Recovered

| | original | relearned |
|---|---|---|
| key | `obligor_id` | `obligor_id` |
| id format | `OBL{seq:05d}` | `OBL{seq:05d}` |
| ratio | 0.45 | 0.465 |
| max members | 6 | 6 |
| new group rate | 0.6 | 0.642 |
| attributes | 6 declared | 9 found, including all 6 |

Regenerated: 218 groups against 210, max 6 members against 6, mean 3.12 against 3.28, and **zero
groups disagreeing with themselves** on any attribute.

The three extra attributes (`ebitda_eur`, `enterprise_value_eur`, `industry_concentration_bucket`)
are genuinely constant per obligor — they are computed from revenue and industry — so they belong on
the group. Finding more than was declared is the right answer here, not a miss.

### The surgery

Detection alone would change nothing. Noting the key while leaving the attributes where they are
still draws the industry per facility. So the shared columns are **moved**: out of `spec.columns`,
where they are drawn once per entity, into `group.columns`, where they are drawn once per group and
joined onto every member. The key column is dropped, since the generator mints it from `id_format`.

### Four things that had to be got right

**The degenerate case.** Partition a book into 400 groups of one and *every* column is constant
within its group, so a near-unique column scores as the perfect parent record. The first pass picked
`ebitda_eur` — a float, near-unique, explaining all nine other static columns vacuously. A key
identifies; it does not measure.

**The category case.** `occupancy` explains `property_usage` and `buy_to_let_flag` and passes as a
parent record holding 353 mortgages. `economic_region_nuts3` explains `province` perfectly, because
a region rolls up into one. Neither is a parent record: 353 households that all own their homes are
not the same household. Two thresholds handle these — a cap on members per group, and a requirement
that the key explain **two** attributes rather than one, since a single explained column is too
easily a coarsening of the key itself.

**The ratchet.** `ratio` says how many groups were *created*; a tape shows how many ended up with at
least one member, and Zipf weights leave a tail holding nobody. The CLO creates 180 obligors per 400
facilities and 127 appear. Copying the visible 0.32 back would create 127 next time, of which ~90
would appear, and the book would lose obligors on every round trip — ending at one facility per
obligor, which is the structure this feature exists to preserve. So `ratio` and the Zipf exponent are
fitted **jointly, by running the generator's own allocator** across a grid and keeping the pair whose
output looks most like what was observed. Simulating rather than solving also means there is only one
model of the allocation, so the fit cannot drift from the thing it is fitting.

**The exponent is not identifiable, and the outcome is.** The fit lands on 1.2 against a declared
1.6 with a small error — different (ratio, exponent) pairs produce near-identical books. The tests
therefore check the *realised* lumpiness (mean members, maximum members, singleton share), not the
parameter. Matching a parameter that does not uniquely determine the outcome would be a worse test.

### A loader bug this exposed

`emit.column_order` counted group columns as produced by nothing, so a spec declaring both groups and
a column order was rejected for naming ten columns "nothing produces" — every one of which the
generator does produce. Invisible until now because the one pack with groups declares no column
order, so nothing exercised the pair together. Fixed in `sdd/spec/loader.py`.

### What is lost, and not recovered here

Revenue and leverage really do move together on a real book. Once both are group attributes, that
relationship is not carried anywhere — group attributes are drawn from their own generators, marginal
by marginal. The obligor's columns stay mutually consistent **across its facilities**, which is the
point of the feature; they are no longer correlated **with each other**. That is a real loss and this
branch does not close it.

---

## The round trip, end to end

Relearning a relearned spec, three generations deep, over a 72-period panel:

| generation | groups | chains | maturity rule | realised group size |
|---|---|---|---|---|
| 0 (the pack) | 1 | 1 | `condition` | — |
| 1 | 1 | 1 | `condition` + `[Defaulted]` | 227 groups, mean 3.01, max 6 |
| 2 | 1 | 1 | `condition` + `[Defaulted]` | 211 groups, mean 3.24, max 6 |
| 3 | 1 | 1 | `condition` + `[Defaulted, Recovered]` | 205 groups, mean 3.34, max 6 |

All three structures survive. There is mild drift — the book gets slightly lumpier each generation,
mean members 3.01 → 3.34 — which is worth watching but is not the ratchet the joint fit was built to
prevent.

## The honest limit

**A rule cannot be learned from a panel that does not contain the event.** At the CLO pack's own 24
periods, a book of 60–96 month facilities matures four of them; relearn that twice and the maturities
run out entirely. Nothing here fixes that, and the panel has to be long enough to hold the event.
There is a test pinning this so it is not mistaken for a regression later.

## Files

- `src/sdd/profile/panel.py` — `learn_condition`, `_crossed_rather_than_set`,
  `_condition_exclusions`, `MIN_EXIT_EVENTS`; `learn_secondary_chains`, `_chain_candidates`,
  `_drop_derived`, `_chain_matrix`, `_chain_coupling`
- `src/sdd/profile/groups.py` — new; detection and the joint shape fit
- `src/sdd/profile/profiler.py` — derivation detection moved ahead of the dynamics pass
- `src/sdd/profile/build.py` — `_build_groups`, `_build_secondary_chains`, `_drop_from_correlation`
- `src/sdd/spec/loader.py` — group columns count as produced
- `tests/test_profiler_structure.py` — 19 tests
- `tests/test_profiler_exits.py` — the limitation test it asked to have updated, updated
