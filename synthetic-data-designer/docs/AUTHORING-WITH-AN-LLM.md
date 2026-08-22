# Writing a spec with an LLM

A procedure for turning a source document — a CLO prospectus, an ESMA template, a
data dictionary, a term sheet — into a working spec.

Written to be **pasted into an LLM as context**, or followed by a person. It assumes
the reader has [`SPEC-REFERENCE.md`](SPEC-REFERENCE.md) available for field detail and
does not repeat it.

---

## The one rule that matters

**Never hand over a spec you have not run.**

A spec is data, and an LLM will produce plausible-looking YAML that fails on a
detail no amount of reading catches — a transition row summing to 0.9975, a hazard
pointing at a state that does not exist, a derivation referring to a column defined
below it. All of those are caught in under a second:

```python
from sdd import api

problems = api.check("my_spec.yaml")["problems"]
```

The loop is **draft → check → fix → repeat until empty → generate → look at the
output**. Every step below assumes it.

---

## What you are extracting

A prospectus is prose written for lawyers and investors. You are looking for five
things, and most of the document is not them.

```
   1. WHAT is the unit?          one facility? one loan? one account?
   2. WHAT is recorded per unit? the column list
   3. HOW does it repay?         bullet, annuity, interest-only
   4. HOW does it go wrong?      the arrears or credit ladder, and what follows
   5. HOW BIG is it?             deal size, count, reinvestment period
```

Everything else — the waterfall, the tranches, the covenants, the parties — belongs
to the liability side and **this tool does not model it.** A prospectus is mostly
liability side. Do not try to encode it.

---

## Step 1 — the unit and the calendar

```yaml
meta:
  name: my_clo_2026        # lowercase, no spaces; becomes filenames
  title: My CLO 2026-1     # what a person sees
  asset_class: clo
  entity_noun: facility    # the UI says "500 facilities", not "500 loans"

entity:
  id_column: facility_id
  time_column: reporting_date
  calendar: {start: "2026-01-31", periods: 36, freq: month_end}
```

**From the prospectus:** the reporting date convention gives `freq`; the deal's
expected life or reinvestment period plus a tail gives `periods`.

**Watch:** `entity_noun` is worth setting. Without it every screen says "loans",
which is wrong for a facility, an account or a lease.

---

## Step 2 — the columns

One entry per field. The three things to get right are `role`, `dtype`, and whether a
generator is needed.

| role | meaning | needs a generator? |
|---|---|---|
| `static` | fixed for the life of the entity — origination date, term | yes |
| `dynamic` | changes each cut-off — balance, arrears state | yes, for its opening value |
| `derived` | computed by a `derivations` entry | **no** — declaring one is an error |
| `constant` | same for every row in the deal — deal name, currency | no, put it in `constants` |
| `helper` | used during generation and **dropped before output** | yes |

```yaml
columns:
  - name: current_par
    role: dynamic
    dtype: float
    min: 0.0
    description: Par outstanding.
    generator:
      kind: scipy
      dist: lognorm
      params: {s: 0.55, loc: 0.0, scale: 3_200_000}
      clip_min: 500_000
      clip_max: 25_000_000
```

**From the prospectus:** minimum and maximum facility size give the clips. An average
or weighted-average facility size gives the centre. A "no obligor exceeds 2%" limit is
a concentration rule — see step 6, not here.

**The most common LLM mistake here** is giving a `derived` column a generator. If a
`derivations` entry produces the column, the column entry declares only its name,
role and dtype.

---

## Step 3 — how it repays

```yaml
dynamics:
  amortisation:
    kind: bullet              # annuity | linear | interest_only | bullet | custom
    balance: current_balance
    rate: all_in_coupon_pct
    term: months_to_maturity
    only_when_state: [Performing, Watchlist]
```

**From the prospectus:** leveraged loans are usually **bullet** — interest monthly,
principal in one lump at maturity. Consumer and auto loans are **annuity**. Dutch
mortgages are often **interest_only**.

`only_when_state` matters: a defaulted facility stops amortising, and without this it
keeps paying down while in default.

---

## Step 4 — how it goes wrong

This is the part that carries the most judgement and the part a prospectus states
least directly.

```yaml
lifecycle:
  state_column: credit_state
  states: [Performing, Watchlist, Distressed, Defaulted, Recovered, Prepaid, Sold, Matured]
  terminal: [Recovered, Prepaid, Sold, Matured]
  absorbing: [Defaulted]
  transitions:                  # rows over non-terminal states, each summing to 1
    - [0.9820, 0.0150, 0.0025, 0.0005]
    - [0.1800, 0.7600, 0.0500, 0.0100]
    - [0.0400, 0.1400, 0.7300, 0.0900]
    - [0.0000, 0.0000, 0.0000, 1.0000]
  hazards:
    - {kind: bernoulli, name: prepayment, annual_rate: 0.22, to_state: Prepaid}
    - {kind: dwell_time, name: recovery, from_state: Defaulted, periods: 9, to_state: Recovered}
    - {kind: condition, name: maturity, when: "months_to_maturity <= 1", to_state: Matured}
```

Four things to get right, in order of how often they are got wrong:

**1. `terminal` versus `absorbing`.** Terminal means the entity *leaves the pool* and
stops being reported. Absorbing means it stays and never leaves the state. A defaulted
facility is **absorbing**, not terminal — it sits in the book through the workout. Get
this backwards and defaults vanish from the panel.

**2. Rows sum to 1, over the non-terminal states only.** The matrix covers movement
*between states an entity sits in*; leaving the pool is a hazard, not a matrix cell.

**3. Rates are annual.** `annual_rate: 0.22` is 22% a year however often the calendar
ticks. Do not pre-convert to monthly.

**4. Pick the right hazard kind.**

| kind | use when | example |
|---|---|---|
| `bernoulli` | a flat chance each period | prepayment, trading |
| `dwell_time` | a fixed delay after entering a state | a nine-month workout |
| `condition` | a rule over the entity's own columns | maturity, when the term runs out |

A rule is not a probability. Maturity as a `bernoulli` gives a 96-month facility the
same chance of maturing in month three as a 60-month one.

---

## Step 5 — the size of the deal

```yaml
entity:
  targets:
    - {column: current_par, total: 500_000_000, entities: 500}
```

**From the prospectus:** the target par amount. Without this the portfolio totals
whatever the draws happen to sum to, and a deal has a *size*.

Reinvestment, if the deal has a reinvestment period:

```yaml
originations:
  per_period: 12        # new facilities bought each cut-off
  start_period: 1       # period 0 is the opening book itself
  end_period: 24        # the reinvestment period, in cut-offs
  fresh: true           # newly originated, not seasoned assets acquired
```

Use `rate` instead of `per_period` to express arrivals as a share of the opening
book. `fresh: false` models buying *seasoned* paper in the secondary market, which
is closer to what most CLOs actually do late in a reinvestment period.

---

## Step 6 — concentration, if the deal has obligor limits

A prospectus that says *"no single obligor may exceed 2% of the portfolio"* is telling
you the unit of concentration is the **obligor**, not the facility. That is a `groups`
block: one obligor, several facilities, sharing attributes.

```yaml
groups:
  - name: obligor
    key: obligor_id
    ratio: 0.45              # obligors per facility
    id_format: "OBL{seq:05d}"
    size: {kind: zipf, concentration: 1.6, max_members: 6}
    columns:                 # generated once per obligor, shared by its facilities
      - {name: industry, dtype: category, generator: {...}}
      - {name: obligor_country, dtype: category, generator: {...}}
```

Without this the same company appears in four industries at once and every
concentration figure is meaningless.

**Do not also declare the key in `columns`.** The group mints `obligor_id` from its
`id_format`, so a column of the same name is a duplicate and the loader rejects it:

```
group 'obligor' key 'obligor_id' is also declared as a column;
a group generates its own key, so remove the column
```

The same goes for the attributes under `columns:` in the group — they are declared
there and nowhere else. This is the first thing that goes wrong when following this
section, and it is caught by `api.check` in under a second.

---

## Step 7 — turn the limits into checks

The covenants in a prospectus are testable statements. Write them as invariants, and
the generated data is checked against the document it came from:

```yaml
validation:
  custom:
    - name: no_obligor_over_two_percent
      description: The indenture's single-obligor concentration limit.
      sql: |
        with by_obligor as (
          select reporting_date, obligor_id, sum(current_par) as par
          from panel group by 1, 2
        ), totals as (
          select reporting_date, sum(par) as total from by_obligor group by 1
        )
        select b.* from by_obligor b join totals t using (reporting_date)
        where b.par > 0.02 * t.total
```

A check returning **rows is a failure** — the query selects violations.

**Expect it to fail the first time, and treat that as the check working.** Writing
this guide, following its own steps produced a spec where that exact invariant failed
on the first run: `max_members: 6` lets one obligor hold six large facilities, which
in a 500-facility book breaches 2%. The generation settings and the covenant
disagreed, and the check said so.

That is the loop worth having. The limit from the document and the parameters you
chose are two independent statements, and an invariant is what makes them argue with
each other before a reader has to.

---

## The prompt

For an LLM doing this from a document:

> You are writing a spec for the synthetic-data-designer. The full field reference is
> in `docs/SPEC-REFERENCE.md` — use only fields that appear there.
>
> From the attached document, extract: the entity unit, the column list, the
> repayment mechanism, the credit or arrears ladder, the deal size, and any
> concentration limits. Ignore tranches, waterfalls, OC/IC tests and fees — this tool
> models the collateral, not the liabilities.
>
> Produce one YAML spec. Then:
>
> 1. run `api.check(spec)` and fix every problem, repeating until it returns none;
> 2. run `api.run(spec, 500, "./out")` and read the validation report;
> 3. **state which numbers you invented.** A prospectus gives sizes and limits; it
>    rarely gives transition probabilities. Anything you chose rather than read must
>    be flagged as an assumption, not presented as sourced.
>
> Where the document is silent, prefer a documented assumption over a plausible
> guess: put the reasoning in the field's `description` so the next reader sees it.

That last instruction is the one that matters. A spec whose invented numbers are
labelled is a starting point somebody can calibrate. A spec whose invented numbers
look sourced is a liability, and it will be believed.

---

## Checking the result

```python
from sdd import api

problems = api.check("my_spec.yaml")["problems"]  # must be empty
result = api.run("my_spec.yaml", 500, "./out")  # generate
result["validation"]["passed"]  # invariants
```

Then look at the data. The failures that survive validation are the ones that look
right in aggregate:

- does the arrears mix drift the way the document implies, or is it static?
- do balances amortise, or sit flat when they should not?
- does anything leave the pool? A book where nothing ever exits is usually a
  `terminal`/`absorbing` mistake from step 4.
- are the concentration figures plausible, or is every obligor exactly one facility?

## Worked examples

The shipped packs are the best reference, and each was written from real documents:

| pack | written from |
|---|---|
| `clo_eu_leveraged_loans` | a CLO specification — obligor groups, bullet repayment, a reinvestment period, a rating chain |
| `rmbs_nl_green_lion` | an ESMA Annex 2 template — 71 columns, interest-only mortgages, an HPI index |
| `auto_abs_esma_annex5` | an ESMA Annex 5 template — depreciating collateral, balloon payments |
| `credit_benchmark_known_ceiling` | nothing — built so the answer is known |

Read `packs/clo_eu_leveraged_loans.yaml` alongside this guide. It is heavily
commented, and the comments explain *why* each number is what it is — which is the
habit worth copying.

And [`reference_spec.yaml`](reference_spec.yaml) shows **every option in one runnable
file** — every generator kind, hazard kind, derivation kind, metric and chart. Use it
to answer "what can go here?"; use the packs to answer "what should go here?".
