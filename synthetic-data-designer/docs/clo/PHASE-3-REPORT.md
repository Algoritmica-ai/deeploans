# Phase 3 — groups

**Branch:** `phase-3-groups` · **Tests:** 510 passed, 1 skipped ·
**Lint & format:** clean · **CLO validation:** 45/45

Several entities sharing one parent record. Built generically: the CLO pack uses
it for the obligor behind several facilities, and the same shape is a household
holding a mortgage and a buy-to-let, a dealer behind a month of car loans, or a
company behind several SME facilities.

## What it does

```yaml
groups:
  - name: obligor
    key: obligor_id
    ratio: 0.45              # 225 obligors per 500 facilities
    id_format: "OBL{seq:05d}"
    new_group_rate: 0.6      # 3 in 5 acquisitions bring a new company
    size:
      kind: zipf             # lumpy, like a real book
      concentration: 1.6
      max_members: 6
    columns:                 # generated once per obligor, shared by every facility
      - {name: industry, ...}
      - {name: revenue_eur, ...}
```

A group is more than a category column because it carries its **own** attributes,
generated once and identical for every member. Three facilities lent to the same
company must agree about that company's industry. Generated per facility they
would disagree, and every figure computed by obligor would be wrong while looking
entirely plausible.

## The CLO pack now

| | Before | After |
|---|---|---|
| Facilities | 788 | 788 |
| Obligors | 788 | **242** |
| Facilities per obligor | 1.00 | **3.26**, max 6 |
| Distribution | flat | 63 hold one · 50 hold two · 59 at the cap |
| Invariants | 40 | **45** |

Industry, country, revenue, EBITDA margin, leverage and listing status moved from
the facility to the obligor. Verified constant within every obligor, in every
period.

## Generic, not CLO-shaped

`test_grouping_works_on_a_mortgage_pack` adds a **household** group to the RMBS
pack — several mortgages per household, with household income and a joint
application flag as group attributes — and asserts both stay constant across the
household's loans. Nothing in the feature knows what a CLO is.

**One shape it does not cover**, stated in the model's own docstring: an entity
belonging to *several* groups, such as two named borrowers on a single mortgage.
That is many-to-many and a different feature. Model those as columns of the loan,
or make the household the group and let it hold several loans — which is what the
test above does.

## Four bugs found by running it

**1. The member cap was not a cap.** `max_members: 6`, and an obligor held 11.
Counts were tracked per call, so a borrower filled to its limit in the opening
book quietly took more every time the pool reinvested. Running totals now live in
the group table itself, so they travel with it across cohorts.

**2. `pd.concat` left the counts missing on new groups**, and a missing count is
not a full group — every freshly minted obligor read as having NaN capacity and
the assignment crashed. Found immediately after fixing (1), which is the pattern:
the first fix exposed the second.

**3. The group key never reached the output.** Attributes were joined, the
identifier was not, so nothing in the panel said which facilities shared an
obligor. The whole point of a group, absent.

**4. The CLO pack could not be loaded in the web UI.** This one would have
shipped.

`GET /api/packs/{name}` dumps with `exclude_none=True`. `ConstantGen.value` was a
required field, so a column written `{kind: constant, value: null}` — the way a
column is seeded empty for a later derivation to stamp, which the event dates all
use — came back to the browser with the field missing and failed validation when
posted to run. The pack appeared in the wizard and was refused the moment anyone
pressed Generate.

Fixed at the model (`value: Any = None`), and
`test_a_pack_survives_the_web_api_round_trip` now checks every pack through that
path rather than trusting that parsing from disk is enough.

## The concentration decision, and a correction

Grouping exists so obligor concentration means something. Having measured it, I
am **not** asserting a limit — and I got this half wrong before correcting it.

I first added an invariant that no obligor exceeds 2.5% of par in the opening
portfolio, on the reasoning that a manager would not buy a book already in
breach. It passed on five seeds at 500 facilities. Then it failed at 400.

| Facilities | Obligors | Largest obligor |
|---|---|---|
| 200 | 66 | **4.02%** |
| 300 | 95 | 3.41% |
| 400 | 133 | 2.56% |
| 500 | 162 | 2.27% |
| 1,500 | 479 | 0.68% |

A smaller book is genuinely lumpier. Demanding 2.5% of a 200-facility portfolio
asks it to be flatter than 66 obligors can be. The same is true over time: the
pool runs down from 500 facilities to around 330 and survivors' shares rise as
the denominator shrinks — worst cut-off 2.9% on base, above 4.5% on severe.

**A percentage limit is a portfolio policy, not a property of the data.** Both
the size effect and the drift are what a real indenture limit exists to catch,
and asserting they cannot happen would model the world backwards and fail most
runs. What is asserted instead is *structural*: the grouping is real, stable, and
scales sensibly. Two tests carry the rest —
`the_pack_is_diversified_at_its_design_size` states 2.5% as a property of this
pack at 500 facilities, and `concentration_falls_as_the_portfolio_grows` pins the
size relationship. The figure itself belongs in Phase 4's metrics, as a number to
watch rather than a rule to enforce.

## Test report — 22 new

| Area | Tests |
|---|---|
| Shape | entities share parents · attributes agree across members · a facility keeps its obligor · the key reaches the output · bookkeeping does not leak |
| Size and caps | the cap holds across the whole run · members are lumpy not uniform · an impossible cap is refused |
| Reinvestment | later cohorts reuse existing parents · new parents also appear |
| Concentration | diversified at design size (4 seeds) · concentration falls as the book grows · drift rises as the pool runs down |
| Spec validation | count-or-ratio · attribute also declared as a column is refused · key also declared as a column is refused · a group cannot list its own key |
| Genericity | ungrouped packs byte-identical · grouping works on a mortgage pack · every pack survives the web round trip |

`test_concentration_drifts_up_as_the_pool_runs_down` deliberately asserts the
**opposite** of a limit: if it ever fails, the pack has started preventing
something it should be reporting.

## Two tests that were designed to fail, and did

`test_the_pack_does_not_overclaim` asserted the pack had *no* grouping, with a
message saying to update it rather than delete it when that changed. It fired
exactly as intended and is now inverted — the pack must genuinely have obligors.

`test_the_shape_matches_the_specification` counted declared columns, which fell
below 55 when seven moved onto the obligor. It now counts *output* columns, which
is what the specification's 55–70 range was about.

## Files

| File | Change |
|---|---|
| `src/sdd/spec/schema.py` | `Group`, `GroupSize`; `DesignSpec.groups`; group columns and key in `output_columns`; `ConstantGen.value` optional |
| `src/sdd/generate/groups.py` | new — build, assign, attach, member accounting |
| `src/sdd/generate/book.py` | join groups before randomness and derivations |
| `src/sdd/age/panel.py` | carry the group table through ageing into `originate` |
| `src/sdd/api.py` | one table shared by the opening book and every cohort |
| `src/sdd/spec/loader.py` | group validation; group names available to expressions |
| `src/sdd/validate/invariants.py` | `group_columns_stable` |
| `packs/clo_eu_leveraged_loans.yaml` | obligor group; three structural invariants |
| `tests/test_groups.py` | 22 tests |

## Not done in this phase

**Profiler support.** Pointing the profiler at a real tape still discards the
group structure — it will not detect a repeated key, measure the
facilities-per-obligor distribution, or identify which columns are
obligor-constant. The plan flagged this as non-optional and it remains the
outstanding half of Phase 3, worth roughly a week and a half. Until it lands,
grouping is something a spec author writes by hand, not something the tool learns.

## Next

Phase 4 — the standard report: 19 per-period portfolio metrics, including the
concentration figures this phase made measurable.
