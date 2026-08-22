# Tranche 0 — spike report

**Branch:** `clo-pack` · **Scope:** the five unknowns from the build plan ·
**Reproduce:** `python3 docs/clo/spikes/<script>.py`

Five things the build plan could not settle by reading code. Each was run as a
real experiment against a real pack, not reasoned about. Four came back
favourable. One found an engine gap the plan did not have.

## Verdict

| # | Question | Answer | Effect on the plan |
|---|---|---|---|
| S0.1 | Can a date be stamped once and never overwritten? | **Yes** — `coalesce()` | No engine change. −3 days |
| S0.2 | Does an 8-state / 4-terminal lifecycle work? | **Partly** — maturity cannot be expressed | **New engine item. +3–5 days** |
| S0.3 | Can the 25 quality checks be config, not code? | **Yes** — all three hardest work | ~25 checks stay YAML |
| S0.4 | Is "identical output" achievable? | **Yes — byte-identical** | §29 wording stands |
| S0.5 | Can ratings be derived instead of migrated? | **Yes** — 9/9 grades | −3 weeks confirmed |

**Net:** Stage 1 stays at 2–2.5 weeks, with one new engine item folded in. The
uncertainty is what shrank, not the estimate.

---

## S0.1 — Sticky exit dates ✅

**The worry.** The tool recalculates columns every period. A rule like *"if the
loan is in default, write today's date"* rewrites that date every month, so a
loan that defaulted in March claims December by December. The real event date is
lost. Twelve columns depend on this — default date, recovery date, sale date, and
everything computed from them.

**What I did.** Built two versions of the same rule on the auto pack and aged 400
loans over 8 periods, then asked: for each loan that was ever stamped, how many
*different* dates did it end up showing? More than one means the date drifted.

**Result.**

```
(trigger state: 'Defaulted', time column: 'data_cut_off_date')

naive where()        DRIFTED ✗  7/8   entities stamped: 8
coalesce()           STICKY  ✓        entities stamped: 8
```

**What it means.** The risk was real — the naive rule corrupted 7 of 8 records.
But the expression language already has the fix. `coalesce(a, b)` returns the
first non-empty value, so `coalesce(default_date, <today if defaulted>)` writes
only into an empty cell and leaves a stamped one alone.

**No engine change needed.** The working idiom:

```yaml
columns:
  - name: default_date
    dtype: str
    generator: {kind: constant, value: null}
derivations:
  - target: default_date
    kind: expr
    stage: period
    expr: "coalesce(default_date, where(credit_state == 'Defaulted', reporting_date, None))"
```

---

## S0.2 — Lifecycle shape ⚠️ **new engine item**

**The worry.** Both shipped packs use two terminal states. CLO needs four
(Prepaid, Matured, Sold, Recovered). Does the machinery stretch?

**What I did.** Built the real CLO lifecycle — 8 states, a 4×4 transition matrix
over the non-terminal states, three hazards — and aged 600 facilities for 24
periods.

**Result.**

```
Performing     8,524  via matrix
Watchlist      1,061  via matrix
Distressed       570  via matrix
Defaulted        326  via matrix
Recovered         52  via hazard
Prepaid          180  via hazard
Sold              87  via hazard
Matured            0  via hazard   <-- never reached
```

**What it means.** The shape works. Terminal states are reached by *hazards*, not
by the matrix — the matrix covers only the states a loan can sit in, and exits
are separate rules. Three of the four exits are fine:

- **Prepaid** — a flat yearly chance of refinancing (`bernoulli`)
- **Sold** — a flat yearly chance of the manager trading it (`bernoulli`)
- **Recovered** — a fixed lag after default (`dwell_time`), which is exactly the
  recovery-lag requirement in §9

**Maturity is the gap.** A loan matures when *its own* maturity date arrives.
That is neither a flat probability nor a fixed number of periods — it depends on
a column value that differs per loan. Neither hazard kind can express it, and
attempting a third kind is rejected outright:

```
condition hazard: REJECTED — union_tag_invalid
```

The loader itself proves the gap is real, and catches it at load time rather than
producing quietly wrong data:

```
states ['Matured'] can never be reached:
they are not in transition_states and no hazard targets them
```

**What is needed.** A third hazard kind:

```yaml
- kind: condition
  name: maturity
  when: "months_to_maturity <= 0"
  to_state: Matured
  from_states: [Performing, Watchlist, Distressed]
```

Roughly 100 lines plus tests — the restricted expression evaluator it needs
already exists and is used by derivations. **Estimate: 3–5 days.**

This matters beyond CLO. With 24–72 month terms over a 36-period run, maturity is
a *material* exit route, not an edge case. And neither shipped pack models loan
maturity at all today — arguably a latent modelling gap in RMBS and Auto too.

---

## S0.3 — Quality checks as configuration ✅

**The worry.** The spec lists ~25 checks. Written in Python they are weeks;
written as database queries in the config file they are hours.

**What I did.** Wrote the three hardest as `CustomInvariant` SQL, then ran each
one **twice** — once as written, and once with the threshold moved so it *must*
fail. A check that cannot be made to fail is not a check; it is decoration.

**Result.**

```
as written (cap 25%, window 4)
  custom::reconcile            pass
  custom::no_late_acquisition  pass
  custom::concentration        pass

negative control (cap 0.1%, window 0)
  custom::reconcile            pass
  custom::no_late_acquisition  FAIL 160 rows
  custom::concentration        FAIL 140 rows
```

**What it means.** Two of the three provably fire when they should — they are
really executing, not passing by accident. `reconcile` passes in both runs
because it has no threshold to move; it is a weaker check by nature and worth
strengthening once real portfolio totals exist in Stage 4.

The framework also surfaces broken SQL as a *failure with an error message*
rather than a silent pass. I hit this genuinely: my first concentration query had
a syntax error and was reported as failing, not passing. That is the right
behaviour and worth knowing it works.

**~25 checks stay in YAML.** Concentration and reconciliation both become
meaningful only after Stage 3 and Stage 4 respectively, but the mechanism holds.

---

## S0.4 — Reproducibility ✅

**The worry.** §29 promises *identical output* from the same settings. There are
two versions of that: the numbers match, or the files match byte for byte. File
formats often stamp a creation time inside them, which would break the stronger
claim.

**Result.**

```
data identical  : True
bytes identical : True   (168,789 vs 168,789 bytes)
spec_hash equal : True
```

**What it means.** The strong claim holds. §29 needs no softening, and a
byte-level comparison is a legitimate regression test for the CLO pack.

---

## S0.5 — Ratings without a second state machine ✅

**The worry.** CLO loans carry letter grades that drift over time. Modelling that
properly means a second machine coupled to the health-state machine — three
weeks. §7 permits deriving the letter instead.

**What I did.** Declared a random number per loan, then a rule mapping health
state plus that number onto the nine grades. Ran 500 facilities over 10 periods.

**Result.**

```
grades produced : 9/9  -> ['BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'D']
facilities whose grade changed over time: 61/500
```

**What it means.** All nine grades appear, and grades move as health changes — 61
of 500 facilities migrated within ten periods. It is not a true rating-migration
model and must not be described as one, but it is a defensible v1 under the
spec's own escape clause. **Three weeks saved, confirmed rather than assumed.**

---

## Seven traps for whoever writes the pack

Found the hard way. Each cost real time.

1. **A derived column must also be *declared*.** `output_columns()` reads the
   declared column list, so a derivation target that is not a column never
   reaches disk. It fails silently — the run succeeds, the column is absent.
2. **If the pack pins `emit.column_order`, add the column there too.** Same
   silent failure.
3. **`dtype` is `str`, not `string`.**
4. **Use `and` / `or`, not `&` / `|`.** The expression evaluator supports the
   keywords and rejects the bitwise operators.
5. **Custom SQL needs an explicit `AS` after arithmetic.** `g.par/t.total share`
   is a DuckDB parser error; `g.par/t.total as share` is fine.
6. **Custom checks are namespaced `custom::<name>`** in the validation result.
7. **The loader catches unreachable states.** A real safety net — it found the
   maturity gap before any data was generated.

---

## What changes in the plan

**Add to Stage 1** — a `condition` hazard kind (3–5 days), built generically. It
belongs to the engine, not the CLO pack, and RMBS and Auto should adopt it.

**Remove from the risk register** — sticky exit dates (solved, no engine change)
and the byte-identical reproducibility question (confirmed achievable).

**Confirm** — the §7 rating simplification is viable and should be taken.

**Unchanged** — Stage 1 remains 2–2.5 weeks. The new engine item is offset by the
contingency that sticky dates no longer need.
