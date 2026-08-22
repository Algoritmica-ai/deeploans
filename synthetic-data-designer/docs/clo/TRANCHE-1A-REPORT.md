# Tranche 1a — maturity as a state

**Branch:** `tranche-1-condition-hazard` · **Tests:** 452 passed, 1 skipped ·
**Lint:** clean · **Format:** clean

Tranche 0 found that loans could not mature. This closes that gap, generically.

## What was wrong

The lifecycle moves entities between states two ways: a **transition matrix** for
the states an entity can sit in, and **hazards** for the exits. There were two
kinds of hazard, and neither can see the data:

| Hazard | Fires when | Blind to |
|---|---|---|
| `bernoulli` | a flat chance comes up | everything about the entity |
| `dwell_time` | N periods spent in one state | everything about the entity |

Maturity is not like either. A loan matures when **its own** maturity date
arrives — a 24-month loan and a 72-month loan written the same day mature four
years apart. That is a condition on a column, and nothing could express it.

The loader was right about this and said so, before generating any data:

```
states ['Matured'] can never be reached:
they are not in transition_states and no hazard targets them
```

**Neither shipped pack modelled maturity either.** RMBS and Auto both reach
`Redeemed` through prepayment alone, so every loan in them either prepays,
defaults, or runs to the end of the simulation still owing money. That is an
engine gap the CLO work surfaced, not one it introduced.

## What was built

A third hazard kind. Deliberately generic — nothing about it is CLO-specific.

```yaml
lifecycle:
  states:   [Performing, ..., Matured]
  terminal: [..., Matured]
  hazards:
    - kind: condition
      name: maturity
      when: "remaining_term_months <= 0"
      to_state: Matured
      excluded_states: [Defaulted]
```

`when` is any expression over the entity's own columns, evaluated by the same
restricted evaluator that derivations already use — a spec stays data, never
executable code. The special name `period` is also available, so a window can be
expressed without a column.

### The ordering decision

Condition hazards run **first**, before the probabilistic hazards and before the
matrix. The period now has four passes rather than three:

| Pass | What | Why here |
|---|---|---|
| **0** | **condition hazards** | **facts, not chances** |
| 1 | bernoulli hazards | decided on the previous state |
| 2 | matrix transition | everyone a hazard did not move |
| 3 | dwell-time hazards | consequence of the state just entered |

A loan that has reached its maturity date **has matured**. It must not then be
drawn into prepaying or being sold in the same period. Putting conditions last
would make the maturity invariant — *"matured facilities terminate"* — hold only
most of the time, which is the same as not holding.

### Where the evaluation happens

The lifecycle engine works in state indices and never sees the data frame. So the
frame-aware part stays in the ageing loop, which evaluates each condition and
hands the engine a plain boolean mask per hazard. The engine keeps knowing
nothing about pandas, and the evaluator stays in one place.

## Test report

Seven tests, `tests/test_condition_hazard.py`. Each is stated as the thing that
would be wrong if it failed, not as the code path it covers.

| Test | What would be broken if it failed |
|---|---|
| `a_state_reachable_only_by_condition_is_not_an_orphan` | The loader still rejects `Matured`, so no pack can declare it |
| `loans_actually_mature` | Loans never reach the state — and every matured row satisfies its condition, so maturity is a fact and not a coincidence |
| `maturity_terminates_the_facility` | A matured loan keeps reporting. Asserts one matured row per facility, and that it is that facility's **last** row |
| `condition_beats_the_probabilistic_hazards` | Ordering is wrong and maturity is only probable |
| `an_unknown_column_in_the_condition_is_refused` | A typo in `when` becomes a crash mid-run instead of a load-time error |
| `the_condition_may_use_the_period_number` | `period` is not in scope, so time-based conditions need a dummy column |
| `packs_without_a_condition_hazard_are_untouched` | The new pass changed RMBS or Auto. Asserts byte-equal panels across two runs |

### One test was wrong before it was right

The ordering test first used a real maturity condition with a 90% per-period
prepayment rate, expecting some loans to mature anyway. It failed — and the
implementation was fine.

At 90% per period the pool prepays itself empty inside two periods, while loan
terms are 24–72 months. **No loan ever survived long enough to reach its maturity
date**, so the test could not observe the ordering it was meant to test. It was
measuring pool survival, not precedence.

Rewritten so both rules apply to the same facilities in the same period: the
condition is made true for everyone at period 1, prepayment stays at 90%. If
conditions did not settle first, roughly nine in ten facilities would redeem.
Observed: **0 redeemed, all matured.**

That distinction matters — a test that fails for the wrong reason is worse than
no test, because fixing the code to satisfy it would have been fixing nothing.

### Regression

`packs_without_a_condition_hazard_are_untouched` runs the auto pack twice with no
condition hazard declared and asserts the panels are identical. The new pass is a
no-op when nothing declares it, so RMBS and Auto are unaffected.

**Deliberately not done:** neither shipped pack has been given a maturity hazard.
That would change their output, their tests and their screenshots, and it is a
modelling decision about those products rather than part of this change. Worth
raising separately — as it stands, an auto loan in the Auto pack never reaches
the end of its term.

## Files

| File | Change |
|---|---|
| `src/sdd/spec/schema.py` | `ConditionHazard`, added to the `Hazard` union |
| `src/sdd/age/lifecycle.py` | `condition_hazards`, pass 0 in `step()` |
| `src/sdd/age/panel.py` | `_condition_masks()` — evaluate against the frame |
| `src/sdd/spec/loader.py` | `when` names validated at load time |
| `tests/test_condition_hazard.py` | 7 tests |

## Next

Tranche 1b — the CLO pack YAML itself, which can now declare all four terminal
states.
