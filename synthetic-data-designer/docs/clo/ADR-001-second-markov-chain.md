# ADR-001 — Rating migration is a second Markov chain, and the seam stays open

**Status:** accepted · **Decided:** during Stage 2 · **Built:** Phase 7

## Context

Ratings in the CLO pack are *derived* from the credit state: Performing maps to
BB/B, Distressed to CCC, Defaulted to D. §7 of the specification permits this,
and Tranche 1b took it.

It has one consequence that matters commercially. A real rating moves **before**
distress is visible — a company is downgraded from B to B− while paying every
instalment on time, and that downgrade is the early warning the rating exists to
give. Deriving the rating from the state destroys that ordering, and collapses
the CCC share into the distressed share exactly:

| Scenario | Distress + default | CCC share |
|---|---|---|
| base | 4.90% | 4.90% |
| adverse | 22.18% | 22.18% |
| severe | 40.73% | 40.73% |

Every CLO indenture caps CCC-rated assets, and breaching that cap is one of the
main things that diverts cash away from equity. A pack whose CCC bucket only
fills when loans are already visibly distressed cannot be used to test that
logic — the quiet downgrade of a still-paying loan is the case the limit exists
to catch, and it never happens here.

So a genuine rating chain is needed. It is scheduled last, because it is a
modelling design problem rather than a coding one, and because everything before
it ships value without it.

## Decision

**Build it in Phase 7. Keep it buildable from now on.**

"Keep it buildable" is enforced by tests, not by intention:
`tests/test_second_chain_seam.py` constructs a second `LifecycleEngine` from a
nine-grade rating matrix, steps it alongside the credit chain, and exercises both
coupling directions. If the engine grows a hidden dependency on there being
exactly one lifecycle, those tests fail and the later phase reverts from an
addition to a rewrite.

## Why the seam is already open

`LifecycleEngine` is the only compiled unit, and it is instantiated exactly once
in the whole codebase. It takes a `Lifecycle` object and never reaches for
`spec.lifecycle` itself, so a second instance is already constructible today.

| Property | State | Why it matters |
|---|---|---|
| `LifecycleEngine` instantiations | **1** | the class is a unit, not a singleton |
| `step()` signature | pure | `(state_idx, dwell, rng, multipliers, masks)` — no frame, no spec |
| `dwell` | dict keyed by hazard name | already generalises to several chains |
| `hazard_multipliers` | per-hazard scaling | the hook the upward coupling needs |
| `DesignSpec.lifecycle` | **singular** | the one real blocker |

## The known blocker

`DesignSpec.lifecycle` is a single optional `Lifecycle`, and ~95 places read it
directly. Phase 7 adds a field *beside* it — not a rewrite of the engine.

A test records which of those two is true today and must be **updated rather
than deleted** when it changes.

## Consequences

**Now, and for every phase before 7:** no new code may assume a single state
column, and nothing may reach into `LifecycleEngine` for spec-level state. The
seam tests are the check.

**Phase 7 then adds:** a rating chain beside the credit chain; downward coupling
(a default forces D — unambiguous, a masked assignment); upward coupling (a worse
rating raises the chance of distress — the ambiguous half, which is where the
three weeks go); a CCC-bucket invariant; and the removal of the derived-rating
derivation from the pack.

**Until then**, `test_the_pack_does_not_overclaim` fails if rating migration
appears without the documentation catching up.
