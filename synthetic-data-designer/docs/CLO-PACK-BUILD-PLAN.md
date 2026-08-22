# European CLO pack — build plan

Seven stages taking `clo_eu_leveraged_loans` from nothing to a deployed
calibrated pack. Built from the v1 specification (26pp), checked against the
engine as it stands at `a177797`.

**Total: 11–15 engineer-weeks.** First demoable pack in 2 weeks.

## Delivery phases

| Phase | What ships | Status |
|---|---|---|
| **0** | Spikes — five unknowns resolved | ✅ done |
| **1a** | Condition hazard: loans reach the end of their term | ✅ done |
| **1b** | The CLO pack — 58 columns, 8 states, 48 checks | ✅ done |
| **2** | Aggregate targets — the portfolio has a size | ✅ done |
| **3** | Obligor grouping — one company, many facilities | ✅ done |
| **4** | The standard report — 19 per-period portfolio metrics | ✅ done |
| **5** | CLO charts — metadata-driven, built on phase 4 | ✅ done |
| **6** | Release — deploy, docs, screenshots, release tests | ✅ done |
| **7** | Rating migration — a second Markov chain | ✅ done |

A rendered version of this document is published as an artifact for sharing.
This file is the source; edit it here.

## The sequencing decision

The per-period portfolio metrics — the nineteen monthly figures in §17, "the
standard report" — are deferred to Stage 4, behind the generator, the obligor
model and validation. That is the right call for demo risk: Stage 1 puts a
working CLO on screen in two weeks.

One consequence, accepted deliberately: three of the four required charts *are*
report figures drawn over time, so charts must follow metrics. Stage 5 sits
behind Stage 4, and until both land the CLO results screen shows the generic
mortgage charts, which are meaningless for company loans. Building the chart
aggregations inline in Stage 1 and rewiring them later costs roughly a week of
rework and is not recommended.

## Dependencies

```
0 Spikes → 1 Pack v0 → 3 Groups → 4 Report → 5 Charts → 6 Release
                ↕
           2 Targets  (parallel — no dependency either way)
```

## Effort

| Stage | Scope | Effort | Risk | Ships |
|---|---|---|---|---|
| 0 · Spikes | Five unknowns resolved before committing | 3 days | Low | Estimates you can trust |
| 1 · Pack v0 | The CLO recipe, simplified | 2–2.5 wk | Med | Demoable CLO pack |
| 2 · Targets | Aggregate targets — hit €500m par | 0.5–1 wk | Low | Realistic portfolio size |
| 3 · Groups | Obligors: one company, many facilities | 3–4 wk | **High** | Concentration analytics |
| 4 · Report | Per-period portfolio metrics | 1–1.5 wk | Low | Monthly report card |
| 5 · Charts | Metadata-driven charts | 1.5–2 wk | Med | CLO results screen |
| 6 · Release | Deployment, docs, release tests | 1 wk | Low | Public Space |
| 7 · Rating migration | A second Markov chain, coupled to the credit chain | 2–3 wk | **High** | CCC-bucket testing |

Estimates assume one engineer familiar with the codebase and include tests and
an artefact validator per stage. They do not include design iteration on
Stage 3, which is likely.

---

## Stage 0 — Spikes (3 days)

Five things that could not be settled by reading the code. Each moves an
estimate; two could change the pack's shape.

- **S0.1 Sticky exit dates.** Can a `when` derivation stamp `default_date` on
  first entry to Defaulted and never overwrite it? Derivations recompute every
  period, so a naive rule resets the date each cut-off. Affects ~12 columns
  (`default_date`, `recovery_date`, sale date). If not expressible, design an
  `on_entry` extension to `state_fields`.
- **S0.2 Transition matrix shape.** Confirm 8 states with 4 terminal works:
  matrix over the 4 non-terminal states, exits to terminal inside each row.
- **S0.3 Custom-SQL reach.** Write the three hardest §20 invariants as
  `CustomInvariant` SQL: portfolio reconciliation, no acquisitions after
  reinvestment end, obligor concentration. Proves ~25 checks are YAML.
- **S0.4 Reproducibility claim.** Same spec and seed twice; diff parquet bytes.
  Decides whether §29's "identical output" is byte- or logically identical.
- **S0.5 Rating as derivation.** Confirm a `bucket`/`when` derivation can produce
  BB…D from `credit_state` plus noise. This is the §7 escape; saves 3 weeks.

**Done when:** spike report on the branch answering all five with code that ran;
Stage 1–3 estimates revised; explicit go/no-go on the §7 simplification.

---

## Stage 1 — CLO pack v0 (2–2.5 weeks)

A working, demoable pack. Real lifecycle, reinvestment, defaults, recoveries and
scenarios. Deliberately missing: obligor grouping, target par, portfolio
metrics, CLO charts.

### Engine — small additions only

- **E1.1 `Scenario.recovery_multiplier`.** §15 requires adverse/severe to show
  lower recoveries; `Scenario` has no recovery knob. Mirrors
  `prepayment_multiplier`. ~30 lines plus tests.
- **E1.2 Exit-date stamping**, only if S0.1 says derivations cannot do it.

### Pack YAML — `packs/clo_eu_leveraged_loans.yaml`

- **P1.1** Deal and reporting fields (§6A) — 7 columns. Names must read as
  obviously synthetic: *Synthetic CLO Europe 2026-1*.
- **P1.2** Facility identifiers (§6B) — 9 columns. `facility_id` from a sequence
  generator; `months_to_maturity` and `months_in_portfolio` as period-stage
  derivations.
- **P1.3** Obligor characteristics (§6C) — 9 columns. *v0 simplification:*
  `obligor_id` is 1:1 with `facility_id`. Corrected in Stage 3.
- **P1.4** Facility economics (§6D) — 15 columns. Bullet amortisation dominant;
  `all_in_coupon_pct` derived from reference rate, spread and floor.
- **P1.5** Credit fields (§7) — 12 columns. `credit_state` from the lifecycle;
  ratings derived per S0.5.
- **P1.6** Eligibility flags (§8) — 10 required, 3 optional. All derivations.
- **P1.7** Lifecycle (§9–10) — 8 states; `terminal: [Prepaid, Matured, Sold,
  Recovered]`; matrix over the 4 non-terminal states;
  `DwellTimeHazard(Defaulted → Recovered)` for the recovery lag.
- **P1.8** Reinvestment (§11) — `originations` with `end_period: 24`. The stop
  date already exists in the engine; §11 assumed otherwise.
- **P1.9** Recovery — `dynamics.recovery` with rate, balance column, `on_states`.
- **P1.10** Market price — an `Index` with drift and `volatility` on
  `current_market_price`; `market_value` derived from par × price.
- **P1.11** Industry and geography (§13–14) — weighted categoricals, documented
  as configurable demo assumptions, not market estimates.
- **P1.12** Scenarios (§15) — base/adverse/severe via `default_multiplier`,
  `prepayment_multiplier`, `index_shift` on price, new `recovery_multiplier`.
- **P1.13** Emit — `synthetic_clo_{yyyymm}.csv` plus the consolidated panel.

### Validation

- **V1.1** Built-in toggles. `closed_pool` off — this pool is open.
- **V1.2** ~22 custom SQL invariants (§20). Concentration deferred to Stage 3.

### Tests — `tests/test_clo_pack.py`

Pack loads and validates; 500 × 36 completes; all invariants pass. Scenario
ordering regression (§15, §31 B–C). Reproducibility (§31 D). Terminal states, no
resurrection (§31 F). Reinvestment window (§31 E). RMBS and Auto still green.

**Done when:** §29 items 1–11 and 15–19 pass; default run completes inside the
Space limits; pack card loadable with no file; suite green, ruff clean.

---

## Stage 2 — Aggregate targets (0.5–1 week, parallelisable)

Generate facility sizes summing to a stated total. Today balances are drawn
independently and the portfolio total lands wherever it lands.

```yaml
entity:
  targets:
    - column: current_par
      total: 500_000_000
      method: proportional   # or: iterative
      tolerance: 0.005
```

- **A2.1** `Target` model on `entity`, validated against declared numeric columns.
- **A2.2** Apply in `book.py` after sampling and *after* randomness, before
  derivations. Rescale before noise and the noise breaks the total.
- **A2.3** Decide and document whether reinvestment cohorts re-target or inherit
  the opening scale. Recommendation: inherit, so the portfolio can shrink.
- **A2.4** Derived columns (`market_value`, `original_par_acquired`) must be
  computed after rescaling.

**Done when:** 500 facilities sum to €500m within tolerance across seeds; derived
columns consistent; RMBS and Auto unaffected.

---

## Stage 3 — Group entities (3–4 weeks) — the blocker

One company, many loans. 500 facilities across ~225 obligors, where facilities of
the same obligor agree on industry, country, revenue, EBITDA and leverage.
Roughly half the total engineering.

**Why it is hard:** `build_book()` runs twice — once for the opening portfolio,
again inside `originate()` for every reinvestment cohort. The obligor table
cannot be a build-time artefact; it must be run state carried through ageing,
because a facility acquired in month 20 may belong to an obligor created in
month 1.

```yaml
groups:
  obligor:
    key: obligor_id
    count: 225                 # or ratio: 0.45
    id_format: "OBL{seq:05d}"
    assignment: {kind: zipf, concentration: 1.15}
    new_group_rate: 0.6        # share of acquisitions that are new obligors
    columns:
      - {name: industry,    generator: {...}}
      - {name: revenue_eur, generator: {...}}
constraints:
  - kind: max_group_share
    group: obligor
    column: current_par
    max: 0.025
```

- **G3.1** `Group` model plus `DesignSpec.groups`, with validation.
- **G3.2** `book.py` — build the group table, sample assignments, left-join.
  Accept an existing table so later cohorts reuse it.
- **G3.3** `panel.py` — carry the group table as run state alongside `dwell` and
  `accrual_counters`; pass to `originate()`; attach-or-create per
  `new_group_rate`.
- **G3.4** `api.py` — thread through; record group summary in `run_manifest.json`.
- **G3.5** New invariant toggle `group_columns_stable` — group attributes
  identical across every facility of a group, every period.
- **G3.6** Concentration cap — post-generation rebalancing. Without it the 2.5%
  invariant simply fails on skewed facility sizes.
- **G3.7** Concentration invariants (§20), now meaningful.
- **G3.8** **Profiler support** — detect a repeated key, measure the
  facilities-per-group distribution, identify group-constant columns, emit a
  `groups:` block in the learned spec. Not optional: without it, profiling a real
  CLO tape silently discards the obligor structure.
- **G3.9** Round-trip test mirroring `tests/test_originations.py`.
- **G3.10** CLO pack update — `obligor_id` from groups, 225 obligors, 2.5% cap.

**Done when:** ~225 obligors with consistent attributes; no obligor over 2.5% of
par across seeds and scenarios; reinvestment attaches and creates; profile →
spec → generate recovers the group structure; §12 satisfied.

---

## Stage 4 — The standard report (1–1.5 weeks)

*Deferred by request.* A generic per-period metrics layer, and the 19 P0 figures
the CLO pack needs from it. Today the engine emits only a per-period state count.

```yaml
metrics:
  - {name: collateral_par,      kind: sum,             column: current_par}
  - {name: wa_spread,           kind: weighted_mean,   column: spread_bps, weight: current_par}
  - {name: ccc_par_pct,         kind: share_where,     column: current_par, where: "ccc_flag == 'Y'"}
  - {name: largest_obligor_pct, kind: max_group_share, group: obligor, column: current_par}
  - {name: cumulative_defaults, kind: cumulative,      column: defaulted_par}
```

- **M4.1** Seven metric kinds — `sum`, `count`, `distinct_count`,
  `weighted_mean`, `share_where`, `max_group_share`, `cumulative` — cover all 19
  P0 figures in §17.
- **M4.2** New module `src/sdd/metrics.py`.
- **M4.3** Hook in `run_ageing` alongside `mix`, inside the existing per-period
  loop. No second traverse.
- **M4.4** Output `portfolio_metrics.parquet` and `.csv`.
- **M4.5** Metric series in the `api.run` payload.
- **M4.6** Sixth download format.
- **M4.7** CLO metrics (§17) — all 19 P0. ✅ The three P1 metrics landed later,
  once §17's warning off proprietary agency calculations could be honoured by
  deriving them generically rather than by omitting them. 22 metrics.
- **M4.8** Reconciliation invariant (§20) — portfolio totals tie to
  facility-level totals. This is what makes the report trustworthy.
- **M4.9** Prove genericity — add three metrics to the RMBS pack (WA coupon, WA
  seasoning, arrears share). If they do not fall out naturally, the abstraction
  is wrong. ✅ Done with the P1 tail, and wider than planned: all three packs now
  carry a full report, including the two metric kinds added for the CLO applied
  to manufacturers and provinces.

**Done when:** all 19 P0 metrics per cut-off; reconciliation invariant passes;
RMBS carries metrics with no CLO-specific code; §29 item 12 passes.

---

## Stage 5 — CLO charts (1.5–2 weeks)

Charts as configuration. Today `build_charts()` returns four fixed keys and the
browser has four hardcoded hosts with bespoke renderers — mortgage-shaped.

```yaml
results:
  charts:
    - {type: series,         title: "Portfolio par",          metric: collateral_par}
    - {type: stacked_series, title: "Credit state",           states: [Performing, Watchlist, Distressed, Defaulted]}
    - {type: series,         title: "CCC share",              metric: ccc_par_pct}
    - {type: category_bar,   title: "Industry concentration", group: industry, column: current_par}
```

- **C5.1** Four types — `series`, `stacked_series`, `histogram`, `category_bar`.
  Only `category_bar` is genuinely new.
- **C5.2** `charts.py` — generic builders. Series charts become thin reads of
  Stage 4 metrics rather than fresh aggregations.
- **C5.3** `app.js` — render by declared type into dynamic hosts, replacing the
  four fixed `#chart-*` divs and their bespoke renderers.
- **C5.4** `index.html` — four fixed containers become one loop target.
- **C5.5** RMBS and Auto migrated to configuration. This is the regression risk:
  shipped, screenshotted behaviour must not change.
- **C5.6** Graceful absence — a missing metric degrades with a readable message.

**Done when:** four CLO charts render from configuration; RMBS and Auto visually
unchanged with screenshots compared; no `if pack == "clo"` in the UI (§18).

---

## Stage 6 — Deployment, docs, release tests (1 week)

- **R6.1** `push_to_space.sh` already copies the whole `packs/` directory —
  verify rather than change. No new runtime deps, no PyTorch, CPU only.
- **R6.2** Resource check — 500 × 36 is ~20–25k rows, far inside
  `SDD_MAX_RECORDS=50000` and `SDD_MAX_ROWS=2500000`. Confirm on the live Space
  for all three scenarios.
- **R6.3** Privacy copy (§25) — use the specified wording. Nothing may weaken the
  existing shared-instance warning.
- **R6.4** Space card mentions CLO support.
- **R6.5** Documentation (§26) — README, User Guide, pack comparison table, and a
  new section explaining CLO concepts for readers who have never seen one.
- **R6.6** Scope statement — v1 models the collateral portfolio, not tranche
  waterfalls, and produces no investment recommendations.
- **R6.7** Screenshots — re-run the existing Playwright capture script.
- **R6.8** Release tests A–F (§31). ✅ One named suite, `pytest -m release`,
  runnable against the working tree or a deployment via `--release-target`.
  See `docs/clo/RELEASE-TESTS.md`.

**Done when:** every §29 checkbox ticked; release tests pass on the deployed
Space, not just locally; deployed *from `main`*, after merge.

---


## Profiler catch-up

The profiler reads a real tape and writes the spec that would reproduce it. That
backward direction has fallen behind the spec model: every feature added since
Phase 1 is something it cannot read back.

This was written down as a single ~1.5-week item attached to Phase 3. That was
wrong, and measurably so — the piece that breaks the product is not the piece
that was scheduled. Tested by adding each half by hand to a relearned CLO spec:

| Added by hand | Relearned spec valid? |
|---|---|
| nothing | ✗ |
| **groups** — exactly the Phase 3 leftover | ✗ **still broken** |
| **exit hazards**, no groups | ✓ **fixed** |

It is four separate pieces, and only one of them belongs to Phase 3.

| Piece | Effect if missing | Size | Status |
|---|---|---|---|
| **Exit hazards** — how entities leave | `/api/analyse` returns 500 on any tape where loans are sold, mature or recover | ~3 days | ✅ done |
| **Targets** | portfolio size not recorded; regenerating at another scale lands anywhere | ~2 days | ✅ done |
| **Condition hazards** | maturity comes back as a flat monthly chance — reachable, but a different rule | ~3 days | ✅ done |
| **Secondary chains** | the rating chain is discarded; a relearned CLO has no rating migration | ~3 days | ✅ done |
| **Groups** | obligor structure discarded; a relearned tape has one company per facility | ~1.5 wk | ✅ done |

All six shipped, across `PROFILER-BACKWARDS-1-TO-3.md` and `PROFILER-BACKWARDS-4-TO-6.md`.
Metrics and charts are also now emitted, though as a *proposal* rather than a
recovery — which figures matter is a judgement no tape records.

**One limit worth carrying forward:** a rule cannot be learned from a panel that
does not contain the event. At 24 periods a book of 60–96 month facilities
matures a handful, and relearning twice exhausts them. The detection is sound;
the evidence has to be there.

**Also found, and since fixed:** the auto pack relearned to a *valid* spec that
then failed to generate — `could not convert string to float: 'Fixed'`. The name
match picked `interest_rate_type` over `current_interest_rate_pct`. Columns are
now judged on their values rather than their declared dtype.

## Working agreements

1. **One PR per stage, merged before the next begins.** Twice now, work has been
   stranded on a branch behind an already-merged PR and silently missed `main`.
   Always branch from a freshly pulled `main`.
2. **Deploy only from `main`.** The Space once ran code that existed on no merged
   branch, and input validation everyone believed was live had never shipped.
3. **Every stage gets unit tests and an artefact validator** that re-derives the
   output independently. `pytest` green and `ruff` clean before commit.
4. **Generic before specific.** No `if pack == "clo"` branches — the spec asks
   for this explicitly, and it is what makes the fourth pack cheap.

## Risk register

| Risk | Stage | Impact | Mitigation |
|---|---|---|---|
| Group design needs a second pass | 3 | High | Most likely single overrun. Spike assignment and persistence on paper first; timebox design to two days |
| Sticky exit dates not expressible | 0–1 | Med | S0.1 exists for this. ~3 days for an `on_entry` extension if it fails |
| Chart refactor breaks RMBS or Auto | 5 | Med | Migrate existing charts to config first and prove them unchanged |
| Concentration invariant fails on skewed sizes | 3 | Med | G3.6 rebalancing is a requirement. Test across seeds and scenarios |
| Profiler support deferred, product inconsistent | 3 | Med | Saves ~1.5 weeks and leaves profiling silently lossy. Decide explicitly |
| Scenario ordering not deterministic enough to test | 1 | Low | Test ordering with margin, not exact values, per §15 |
| "Byte-identical" reproducibility unachievable | 0 | Low | S0.4 settles it; soften §29 wording if parquet embeds metadata |

## Explicitly out of scope

Per §28, v1 excludes the entire liability side: tranches, interest and principal
waterfalls, OC and IC diversion mechanics, management fees, tranche pricing and
cash flows, equity IRR, call economics, and any replication of rating-agency
models.

**§21 fidelity is done**, as plausibility bands rather than as a distance from a
reference tape — see `docs/clo/FIDELITY-AND-GROUP-CORRELATION.md`. The custom
calibration §21 describes stays out until a licensed vendor dataset exists,
which is the condition the section itself puts on it.

**No longer deferred:** the P1 metrics (§17) — rating factor, portfolio turnover,
diversity proxy — and the sector stress overlay (§16). §17's warning against
reproducing agency calculations is honoured by computing the credit factor from
this pack's own rating chain and the diversity proxy as an inverse Herfindahl,
rather than by leaving them out. See `docs/clo/P1-TAIL-REPORT.md` and
`docs/clo/GENERIC-CREDIT-MEASURES.md`.

**Rating migration is no longer out of scope.** It moved to Phase 7 — see
`docs/clo/ADR-001-second-markov-chain.md` for why, and for the tests that keep
the architecture able to take it.

These belong to the Phase 2 CLO Laboratory, and starting them before the
collateral generator is validated would be the wrong order.
