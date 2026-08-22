# The CLO P1 tail

**Branch** `clo-p1-tail` · **Suite** 671 passed, 1 skipped · **Ruff** `check` and `format --check`
both clean on exit code.

Closes everything §16 and §17 left marked P1: the sector stress overlay, and the three metrics
deferred because §17 warns off reproducing a rating agency's calculations.

## The licensing question first, because it shaped the build

Renaming the measures is **not** what solves it. The exposure is not the word "WARF" — that is
industry vocabulary describing an arithmetic operation. It is the **tables**: the specific factor
per rating grade, and the diversity-score lookup with its industry-correlation assumptions. Those
are compilations published in an agency's methodology documents, and copying them is copying their
work product. The same table under a different heading is the same table.

So we do not use their tables. Full reasoning in
[`GENERIC-CREDIT-MEASURES.md`](GENERIC-CREDIT-MEASURES.md); the short version is below. *Not legal
advice — this removes the specific thing that creates exposure, and whether that suffices for a
given arrangement is a lawyer's call.*

---

## §17's three P1 metrics

### `wa_credit_factor` — credit quality as one number

Ratings are labels; there is no midpoint between B+ and CCC. `credit_factor` is the numeric
stand-in, and this is its par-weighted mean.

**The factors are this pack's own model.** Each is the probability that the pack's rating chain
reaches D within five years from that grade, ×10,000. Take the `transitions` matrix under
`secondary_chains`, raise it to the 60th power, read the column for D:

```
BB    618      B-  2,522      CCC  4,575
BB-   854    CCC+  3,562      CCC- 5,611
B+  1,195                     D   10,000
B   1,717
```

Reproducible from the pack file and nothing else — and a test recomputes them from the matrix on
every run, so an agency table pasted in quietly would fail the suite.

Two properties beyond the licensing point. The factor and the migration behaviour are **the same
model**, so a book that downgrades faster genuinely reads worse; an imported table would sit
alongside our migration model with no relationship to it. And it is **substitutable** — nine rules
in the YAML, so a licensed user pastes in the real factors and changes nothing else.

Measured: opens at 1,534 (between B+ and B, matching the opening rating mix) and climbs to 2,577
over 36 periods as the book migrates down.

**It needed no new metric kind.** It is a `weighted_mean` over a derived column — the existing
machinery, unchanged. That it fell out is the evidence the abstraction was right.

### `effective_obligors` — diversity as a count

The inverse Herfindahl: 1 ÷ Σ(obligor's squared share of par). Competition regulators use the HHI;
its inverse is standard in ecology, political science and portfolio theory. No agency involved.

It reads as a **count**, which is the useful part. Measured on a 400-facility book: 127 obligors,
82.4 effective. Negative control — move half the money into two of them and it falls to 16.1.

Honestly scoped: an agency diversity score also folds in which industries are correlated with which,
and we do not have those assumptions. Ours answers "how concentrated is the money", not "how
correlated are these businesses". The industry concentration metrics sit beside it for the rest.

### `portfolio_turnover` — how much of the book changed hands

Par that left since the last cut-off, as a share of the par that was there. Departures only —
counting arrivals as well would report a pool that replaced every asset as 200% turned over. The
first cut-off reports nothing rather than zero: a book cannot have turned over before it existed.

**This one shipped with a bug and had to be rewritten.** Most packs zero the balance as an entity
enters its terminal state — that is what `state_fields` is for — so the row on which a loan
disappears reads zero. Valued there, the auto pack reported **exactly zero turnover on every
cut-off** while losing a quarter of its loans. It read non-zero on the CLO only because that pack
happens not to zero `current_par` on exit, which is an accident of one pack and not a property of
the measure.

Now valued at the last balance the entity carried while still outstanding:

| pack | entities | turnover per period | cumulative |
|---|---|---:|---:|
| CLO | 400 → 268 | 0.0324 | 1.134 |
| Green loans | 400 → 335 | 0.0071 | 0.163 |
| Auto | 400 → 289 | 0.0150 | 0.344 |

The CLO exceeds 1.0 because it reinvests: facilities that arrive *and* leave inside the panel are
real turnover, and that is the thing the metric exists to show.

---

## §16 — the sector stress overlay

`default_multiplier` moves the whole book at once, and that is not how a downturn arrives. 2008 was
not every sector worsening by the same factor. A recession lands on the sectors exposed to it, and a
portfolio's real risk is how much of it sits in those sectors — which a uniform multiplier **cannot
express**, because every book of the same size behaves identically however lopsided its mix, and the
concentration figures come out unchanged no matter how severe it is set.

`Scenario.segment_stress` is column → {value → multiplier}. The adverse case:

```yaml
segment_stress:
  industry:
    Retail: 2.4      Transportation: 1.5     Healthcare: 0.9
    Consumer Products: 1.9    Energy: 1.4     Business Services: 0.95
    Media: 1.7
```

Multiplies with `default_multiplier` rather than replacing it, so a retail borrower carries 3.0 × 2.4
while a healthcare borrower carries 3.0 × 1.0.

**It rides the same per-entity channel as the rating-stress coupling**, deliberately. Both say the
same kind of thing — *these* entities are likelier to slip than the book as a whole — and one
mechanism means a rating-driven stress and a sector-driven one compose by multiplication instead of
one silently overwriting the other.

### Measured

Worsening rate per sector, inside one adverse run of 900 facilities:

| industry | worsening rate | realised × | declared |
|---|---:|---:|---:|
| Retail | 0.1870 | 1.69 | 2.40 |
| Consumer Products | 0.1814 | 1.64 | 1.90 |
| Media | 0.1627 | 1.48 | 1.70 |
| Transportation | 0.1413 | 1.28 | 1.50 |
| Energy | 0.1272 | 1.15 | 1.40 |
| *(unnamed sectors)* | 0.1103 | 1.00 | — |
| Business Services | 0.1017 | 0.92 | 0.95 |
| Healthcare | 0.0958 | 0.87 | 0.90 |

**Spearman rank correlation with the declared multipliers: 1.00.** Unnamed sectors cluster within
±10% of the baseline — a scenario says which parts of the book it lands on, not which parts it
spares.

The realised multipliers are compressed against the declared ones (2.40 → 1.69) because the
multiplier tilts the worsening *portion* of a transition row and the row is then renormalised. That
is the same `_scale_worse` semantics `default_multiplier` and the rating coupling already use, so
all three read consistently.

Measured on transitions inside one run rather than by comparing two runs, because comparing runs
conflates the overlay with reinvestment feedback and Monte Carlo noise.

---

## Two invariants, with negative controls

- `credit_factor_matches_the_rating` — every facility's factor is the one its grade maps to. A
  factor drifted from the rating would corrupt the headline while every rating on the tape still
  looked right.
- `credit_factor_is_monotone_in_the_grade` — a worse grade never carries a lower factor. If the
  ordering broke, a downgrade could *improve* reported credit quality.

Both were shown to fail when the mapping is deliberately broken (9 and 7 violations). 48 invariants
now, all passing.

---

## Genericity: metrics on all three packs

The plan's M4.9 asked for metrics on a second pack to prove the abstraction. It had never been
done — **the auto and green packs carried no metrics and no charts at all**, and fell back to the
generic column-sniffing results screen. The two new kinds are exactly where a CLO-shaped abstraction
would show, so both packs now carry a full report:

| pack | metrics | charts | the new kinds, applied |
|---|---:|---:|---|
| CLO | 22 | 6 | effective **obligors**, turnover |
| Auto | 9 | 4 | effective **manufacturers**, turnover |
| Green loans | 8 | 4 | effective **provinces**, turnover |

A car pool is diversified across manufacturers and a mortgage book across provinces. Same
arithmetic, three asset classes, one metric vocabulary. If `effective_count` and `turnover` only made
sense for corporate obligors they would belong in the CLO pack as bespoke arithmetic rather than in
the vocabulary every pack draws on.

Two tests had to be rewritten as a result — both asserted that the other packs declared *nothing*.
They now construct the bare case explicitly, which is what they were always testing.

---

## A bug this uncovered

**Renaming a column in the UI silently broke the report, the charts, the group key and the rating
chain.** `_rename_column` rewrites every reference to a column across the spec, and it did not reach
`metrics`, `results.charts`, `groups`, `secondary_chains`, `scenarios.segment_stress`, or the SQL in
`validation.custom`.

Invisible until now for one reason: the only pack the rename test exercised declared none of those
things. Giving the other packs metrics is what surfaced it — renaming `current_balance` left the
report pointing at a column that no longer existed, and the run died mid-generation rather than at
the rename that caused it. Groups and chains had the same hole since they shipped last week.

---

## Files

- `src/sdd/spec/schema.py` — `effective_count` and `turnover` metric kinds, `Metric.entity_column`,
  `Scenario.segment_stress` and its validator
- `src/sdd/metrics.py` — both kinds; `running` widened to carry per-entity state, namespaced by
  metric name
- `src/sdd/age/panel.py` — `_segment_multipliers`, joined into the existing per-entity channel
- `src/sdd/api.py` — `_rename_column` reaches metrics, charts, groups, chains, scenarios and SQL
- `packs/clo_eu_leveraged_loans.yaml` — `credit_factor` column and derivation, three metrics, two
  charts, two invariants, sector overlays on adverse and severe
- `packs/auto_abs_esma_annex5.yaml`, `packs/rmbs_nl_green_lion.yaml` — metrics and charts
- `tests/test_generic_credit_measures.py` — 17 tests
- `docs/clo/GENERIC-CREDIT-MEASURES.md` — the licensing note
