# §21 plausibility, and group attributes that move together

**Branch** `fidelity-and-group-correlation` · **Suite** 690 passed, 1 skipped, 6 deselected ·
**Release suite** 6 passed · **Ruff** `check` and `format --check` clean on exit code.

Two items, and one of them caught a modelling bug in the shipped pack.

---

## §21 — plausibility, not replication

§21 is narrower than it first reads, and the wording is the design:

> For the bundled public pack, fidelity should focus on **broad plausibility** rather than
> claiming replication of a proprietary CLO dataset.

So this is **not** a KS distance against a reference tape. There is no reference tape, and
manufacturing one would mean shipping vendor-derived parameters — which the same section
explicitly forbids. A `PlausibilityBand` is a declared range with a stated reason.

### What it catches that nothing else does

Every invariant in the pack passes on a portfolio of four-thousand-euro loans to companies in one
country, all rated the same, none of them ever repaying. The balances tie, the states move, the
totals reconcile. It is internally consistent and obviously not a CLO.

### The eleven bands

The ten characteristics §21 names, plus the CCC share at close, which is the one an indenture
actually caps.

| band | statistic | range | observed |
|---|---|---|---:|
| facility_size | median | 400k – 10m | 830,085 |
| spread | median | 300 – 600 bps | 395.5 |
| market_price | median | 88 – 101 | 97.7 |
| maturity | median | 60 – 96 months | 84 |
| leverage | median | 3.5 – 7.0× | 5.0 |
| rating_mix | share in single-B | 0.35 – 0.85 | 0.674 |
| ccc_at_close | share CCC and below | 0.00 – 0.10 | 0.045 |
| country_spread | distinct | 5 – 25 | 10 |
| industry_spread | distinct | 8 – 40 | 15 |
| seniority | share senior secured | 0.85 – 1.0 | 0.905 |
| covenant_lite | share cov-lite | 0.55 – 0.98 | 0.825 |

**The ranges come from the market, not from the pack's output.** That distinction is the whole
value of the section: a band fitted to whatever the generator happens to produce can never fail,
and quietly encodes the pack's own quirks as "plausible". Every band carries a `note` saying where
its range comes from, because a range with no stated reason is a number nobody can challenge.

Bands measuring origination facts are taken **at the first cut-off**. A facility's size is decided
once; pooling it across cut-offs weights it by how long each facility happened to survive, so a
book whose largest loans prepay first would read as smaller than it was written.

They are reported through the same `CheckResult` the invariants use, so they reach the validation
report, the HTML artefact and the results screen for free. A plausibility check nobody sees is
worth nothing. Each carries the number and the range rather than a row count — how far outside it
landed is the entire content of the finding.

### The bug it caught

At seed 7, `market_price` failed: **median 103.89**. A book of leveraged loans trading above par.

That is not a bad band, it is a modelling error. A leveraged loan is **callable at par** — if the
price runs up, the borrower refinances and the lender is repaid at 100 — so the price cannot
sustain much above par however well the credit does. The `loan_price` index was drift plus
volatility with nothing to stop it, and a random walk with no bound wanders anywhere given enough
periods. Unbounded is right for a house-price index, which really can run away. It is wrong for a
quantity with an economic ceiling.

Fixed by giving `Index` a `clip_min` and `clip_max`, applied **every step** rather than once at the
end — a bound that only holds on the final cut-off is not a bound — and setting the loan price to
40–101. The opening generator was then aligned to the same range: left at its old 20–103 the book
could print a facility at 103 which the index pulled to 101 the very next month, a discontinuity
with no cause.

| seed | before | after |
|---|---:|---:|
| 3 | — | median 96.0, max 101.0 |
| 7 | **median 103.9** | median 96.7, max 101.0 |
| 42 | — | median 97.6, max 101.0 |

### Negative controls

A band that cannot fail is decoration:

- **shrink the deal to an eighth** → `facility_size` observed 103,761 against 400,000–10,000,000,
  and it fails. Nothing else in the suite catches this; every invariant still passes.
- **generate every obligor in France** → `country_spread` observed 1 against 5–25, fails, and the
  run's overall validation fails with it.
- **a band naming a column that does not exist** → reported as an error rather than skipped.
  Silently skipping would be the worst outcome: fewer checks, all passing, and nobody counts them.

### Custom CLO calibration — correctly still absent

§21's second half — marginals, rank correlations, rating migration, default rates against a real
vendor dataset — is conditional on *"where an appropriately licensed real vendor dataset becomes
available"*. There is none, and §21 forbids vendor-derived parameters in the public pack. The
machinery for it already exists (`sdd.validate.fidelity`, `sdd fidelity`, `api.fidelity`) and works
against any reference tape a licensed user supplies.

---

## Group attributes that move together

The gap the group work opened, in its own words at the time:

> Revenue and leverage really do move together on a real book. Once both are group attributes that
> relationship is not carried anywhere.

### What changed

`Group` gained a `correlation_target`, reimposed on the group table by the **same Iman-Conover
reordering** the entity columns already use — extracted into `reorder_to_correlation` so there is
one implementation and the two cannot drift into meaning different things.

Reordering permutes values already drawn, so it cannot change any column's own distribution: every
range and clip the pack declares survives exactly, and only the pairing changes.

The CLO pack now declares its obligors' structure, as directional assumptions with reasons:

```
margin x leverage   +0.35   a lender advances more turns of EBITDA against durable margins
revenue x margin    -0.15   larger borrowers here skew to lower-margin industrials and retail
revenue x leverage  -0.10   size buys slightly better terms
```

Realised: −0.177, −0.075, +0.322.

### The profiler learns it back

Measured **one row per group, never one per entity**. An obligor with six facilities would
otherwise contribute six identical rows, and the correlation would be weighted by how much each
company happened to borrow — the companies with the most facilities deciding what the relationship
between revenue and leverage looks like.

Without this the structure survives exactly one generation: profile the output and the attributes
come back independent, which is where this started.

### The half-feature, and the fix

First cut held the correlation on the opening book and lost it everywhere else:

| | opening obligors | whole book |
|---|---:|---:|
| ebitda × enterprise value | 0.987 | 0.799 |
| ebitda × revenue | 0.889 | 0.676 |

Reinvestment mints a **median of four obligors at a time**, and the reordering on a table that
small is mostly noise. Measured at a target of 0.90:

| rows | mean | sd |
|---:|---:|---:|
| 5 | 0.847 | **0.242** |
| 10 | 0.876 | 0.104 |
| 50 | 0.881 | 0.045 |
| 200 | 0.886 | 0.017 |

So a correlated table is now drawn in a batch of at least 60 and cut back. Truncating afterwards is
safe, and that is not obvious: the reordering gives every row a *joint* draw, so the first `n` rows
are an ordinary sample of that joint distribution and both the marginals and the correlation
survive in expectation. The cost is a few hundred discarded numbers.

Round-trip error on the strongest pair fell from **0.241 to 0.025**.

---

## A pre-existing bug a new seed exposed

A learned secondary chain's `initial_distribution` summed to **0.999998** — nine rating grades each
rounded to six places — and the loader rightly refuses a distribution that does not sum to 1. So
profiling an ordinary tape returned a 500 whenever the mix happened to round that way.
Seed-dependent, which is why it survived until a test picked a different seed.

The matrix rows already had `_renormalise` for exactly this; the opening mix did not. Now both do,
and eight seeds were checked rather than the one that failed.

---

## Files

- `src/sdd/spec/schema.py` — `PlausibilityBand`, `Validation.plausibility`,
  `Group.correlation_target`, `Index.clip_min` / `clip_max`
- `src/sdd/validate/invariants.py` — `_check_plausibility`
- `src/sdd/generate/randomness.py` — `reorder_to_correlation` extracted
- `src/sdd/generate/groups.py` — group correlation, batch floor
- `src/sdd/age/dynamics.py` — index clipping, applied every step
- `src/sdd/profile/build.py` — `_group_correlation`
- `src/sdd/profile/panel.py` — `_renormalise_shares`
- `packs/clo_eu_leveraged_loans.yaml` — eleven bands, group correlation, bounded price
- `tests/test_plausibility.py` — 11 tests · `tests/test_group_correlation.py` — 8 tests
