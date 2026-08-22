# Tranche 1b — the CLO pack

**Branch:** `tranche-1b-clo-pack` · **Tests:** 469 passed, 1 skipped ·
**Lint & format:** clean · **Validation:** 40/40 invariants

`clo_eu_leveraged_loans` now loads from the Upload screen and runs end to end.

## What a default run produces

`500 facilities × 36 monthly cut-offs, seed 42, base scenario` — **1.8 seconds**

| | |
|---|---|
| Rows | 16,681 |
| Opening portfolio | 500 facilities |
| Acquired during reinvestment | 288 |
| Columns | 56 |
| Invariants | **40/40 pass** |

**Final outcomes:** 286 prepaid · 281 still performing · 138 sold · 33 resolved
out of default · 25 on watchlist · 14 distressed · 6 matured · 5 in workout.

Every one of the eight credit states is reached, and each of the four exits by a
different mechanism.

## What makes it different from the two consumer packs

| | RMBS / Auto | CLO |
|---|---|---|
| Balance | amortises monthly | **bullet** — flat, then repaid at maturity |
| Pool | closed, only shrinks | **open** — reinvests for 24 months, then stops |
| Ladder | 30 / 60 / 90 days past due | **watchlist → distressed → defaulted** |
| After default | charge-off | **9-month workout, then recovery** |
| Exits | prepay, charge off | **prepay, sell, mature, recover** |

## Scenarios — §15 acceptance rules

Same seed, same opening portfolio; only the transition probabilities and
overlays change.

| Scenario | Distress + default | Avg price | Realised loss | CCC share |
|---|---|---|---|---|
| base | 4.90% | 97.39 | €47m | 4.90% |
| adverse | 22.18% | 94.96 | €428m | 22.18% |
| severe | 40.73% | 85.70 | €1,189m | 40.73% |

All three ordering rules hold: distress and losses rise, price falls.

**One engine addition was needed.** §15 requires adverse and severe to show
*lower recoveries*, and `Scenario` had no recovery knob — a stressed run
recovered as much per write-off as the base case. Added
`Scenario.recovery_multiplier` (0.80 adverse, 0.55 severe), capped at 1.0 so a
scenario can never conjure a recovery larger than the balance it recovers
against.

## Four bugs found by running it

Each was caught by an invariant, not by reading the YAML.

**1. `interest_coverage_ratio` declared static, recomputed every period.**
323 violations of `static_stable`. The ratio moves when the coupon moves, so it
is dynamic. A one-word fix, but the invariant caught a genuine contradiction
between the declaration and the derivation.

**2. Realised losses were always zero.**
The formula measured `current_par - recovery_amount` on the resolution row — but
by then the workout has closed the facility out and its par is zero, so every
loss computed as zero. §17 needs cumulative realised losses and §15 needs them
to rise under stress; both would have silently reported nothing.

Fixed with a `par_at_default` column, stamped once when the facility defaults
using the same `coalesce` pattern Tranche 0 established for event dates. The
recovery invariant now measures against that rather than against a figure that
is zero by construction.

**3. The balance went to zero a month before the facility matured.**
The `bullet` kernel makes the whole balance fall due when **one** month remains,
and the maturity condition fired at **zero**. So a facility spent its final month
reported as *performing with nothing outstanding*.

The condition is now `months_to_maturity <= 1`, which is the month the final
payment actually falls due. Worth noting this is a different off-by-one from the
one fixed in Tranche 1a — that was the engine reading counters too early, this is
the pack disagreeing with the amortisation kernel about which month is last.

**4. Two rows of market value differed by a cent.**
`numpy` rounds halves to even, DuckDB rounds them away from zero, so an exact
half-cent legitimately disagrees. Tolerance widened to two cents with the reason
recorded. 2 rows in 16,681 — the relationship holds; the rounding convention
does not.

## Test report

**16 tests** in `tests/test_clo_pack.py`, each stated as what would be wrong if
it failed.

| Test | Would catch |
|---|---|
| `the_default_run_completes_and_validates` | any of the 40 invariants failing |
| `the_shape_matches_the_specification` | column count outside 55–70, wrong key or date column |
| `every_credit_state_is_reachable` | a state nothing can reach — including all four exits |
| `the_balance_is_bullet_not_amortising` | the balance drifting down monthly |
| `the_portfolio_reinvests_and_then_stops` | nothing acquired, or acquisitions after the window closes |
| `facilities_reach_the_end_of_their_own_term` | maturity silently never firing |
| `terminal_facilities_stop_reporting` | a prepaid or sold facility still reporting |
| `a_default_is_followed_by_a_workout` | the workout collapsing to instant resolution |
| `a_loss_is_booked_against_the_par_that_defaulted` | bug 2 returning |
| `event_dates_are_stamped_once` | dates being rewritten each period |
| `scenarios_order_correctly` ×3 | stress ordering breaking on distress, price or loss |
| `the_run_is_reproducible` | the seed no longer determining output |
| `the_pack_does_not_overclaim` | grouping or rating migration appearing without the docs catching up |
| `no_real_company_or_manager_names` | a real company name reaching the output |

### One assertion was wrong before it was right

`a_default_is_followed_by_a_workout` first asserted a resolved facility reports
`Defaulted` for **9** periods, matching the configured workout. It observed 8 —
and the implementation was correct.

A nine-period workout produces eight `Defaulted` rows: the ninth period is the
one it resolves in, and that row already reads `Recovered`. The test now says
that explicitly rather than being loosened to `>= 8`, because the exact number is
the thing worth pinning.

## What this pack deliberately does not do

Both are asserted by `test_the_pack_does_not_overclaim`, so they cannot be
quietly forgotten.

**One obligor per facility.** A real CLO holds several facilities per company,
and every concentration limit investors care about is stated per *obligor*. This
pack therefore **does not model obligor concentration** and has no concentration
invariant — one would measure nothing and pass for the wrong reason. Grouping is
Stage 3.

**Ratings are derived, not migrated.** The letter grade is computed from the
credit state plus a per-facility offset. Real ratings drift within a performing
state and are often downgraded before any distress is visible. This is the
simplification §7 explicitly permits, and it must not be described as a rating
migration model.

Also out of scope per §28: the entire liability side — tranches, waterfalls, OC
and IC tests, equity. Also absent: portfolio metrics (Stage 4) and CLO-specific
charts (Stage 5), so the results screen still shows the mortgage charts.

## Files

| File | Change |
|---|---|
| `packs/clo_eu_leveraged_loans.yaml` | new — 56 columns, 34 derivations, 8 states, 14 custom invariants |
| `src/sdd/spec/schema.py` | `Scenario.recovery_multiplier` |
| `src/sdd/age/panel.py` | apply it, capped at 1.0 |
| `tests/test_clo_pack.py` | 16 tests |

## Next

Stage 2 — aggregate targets, so the portfolio hits a stated €500m of collateral
par rather than landing wherever the draws take it.
