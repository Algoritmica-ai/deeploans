# User guide

Every screen, every control, and every setting that exists — including the ones
the forms do not show, which live in the Advanced tab's YAML.

Start the app:

```bash
sdd ui
```

It serves on <http://127.0.0.1:8000>, binds to localhost only, and writes
everything it produces to a `.sdd-workspace` folder in the directory you launched
it from. Nothing is uploaded anywhere.

Every screen below is captured in [`screenshots/`](screenshots), taken from a
real run of the European CLO pack:

| | |
|---|---|
| [Step 1 — Upload](screenshots/01-upload.png) | schema and sample, and the three calibrated packs |
| [Step 2 — Review](screenshots/02-review.png) | detected columns, types, key, nullability, confidence |
| [Step 3 — Configure](screenshots/03-configure.png) | five groups, two open, and what the run will produce |
| [Step 3 — expanded](screenshots/04-configure-expanded.png) | every group open, showing what each holds |
| [Step 4 — Generate](screenshots/05-generate.png) | the seven stages, mid-run |
| [Step 5 — Results](screenshots/06-results.png) | summary, validation, and the pack's own six charts |
| [Step 6 — Download](screenshots/07-download.png) | five formats and the per-period files |

**Contents**

- [How the whole thing fits together](#how-the-whole-thing-fits-together)
- [Step 1 — Upload](#step-1--upload)
- [Step 2 — Review schema](#step-2--review-schema)
- [Step 3 — Configure](#step-3--configure)
  - [Scale](#tab-1-scale) · [Generation method](#tab-2-generation-method) ·
    [Randomness](#tab-3-randomness) · [Data aging](#tab-4-data-aging) ·
    [Schema](#tab-5-schema) · [Advanced](#tab-6-advanced)
- [Step 4 — Generate](#step-4--generate)
- [Step 5 — Results](#step-5--results)
- [Step 6 — Download](#step-6--download)
- [If you have never met a CLO](#if-you-have-never-met-a-clo)
- [Everything only the YAML exposes](#everything-only-the-yaml-exposes)
- [The command line](#the-command-line)
- [Troubleshooting](#troubleshooting)

---

## How the whole thing fits together

One idea explains the entire interface: **every control writes into a single
YAML document, and that document is what runs.** The forms are a friendly way to
edit it, the Advanced tab shows it, and the Download step hands it to you. There
is no hidden state — a saved configuration plus a seed reproduces a dataset
exactly.

```
Upload  →  Review  →  Configure  →  Generate  →  Results  →  Download
  │           │           │            │            │           │
  schema    columns    the YAML     the engine   charts +    5 formats
  + sample  + types    document                  data table
```

You can move backwards at any time using the rail on the left. Steps unlock as
you complete them.

---

## Step 1 — Upload

Two boxes and a shortcut. You need **at least one** of the two boxes; the
shortcut skips both.

### Schema (marked Required)

The columns your output must have.

| Accepted | What is read from it |
|---|---|
| `.csv` / `.tsv` | The header row — or, if the file looks like a **data dictionary**, one row per field |
| `.xlsx` / `.xlsm` / `.xls` | Same, from the first sheet |
| `.parquet` / `.pq` | The parquet schema: column names with their physical types |
| `.json` | An **ESMA/ECB taxonomy** (`field_name` + `format_hint`), or plain row objects whose keys are the columns |

A **data dictionary** is detected when the table has a name-ish column (`name`,
`field`, `field_name`, `column`, `column_name`) *and* something describing each
field (`type`, `dtype`, `data_type`, `format`, `format_hint`, `description`,
`content_to_report`, `comment`). Both markers are required — a tape read as a
dictionary would produce one field per row, and a dictionary read as a tape would
produce a two-column schema.

ESMA format hints are understood and become types: `{MONETARY}` → float,
`{PERCENTAGE}` → float bounded 0–100, `{DATEFORMAT}` → date, `{INTEGER}` → int,
`{ALPHANUM-28}` → string capped at 28 characters, `{Y/N}` → a category with
exactly two allowed values, `{NUTS}` / `{COUNTRYCODE}` / `{CURRENCYCODE}` →
categories.

### Sample data (Optional)

Real data to learn from — a loan tape, a transaction file, portfolio data.
Accepts everything the schema box does, plus `.jsonl`.

Uploading a sample changes what the tool can do, quite a lot:

| With a sample | Without |
|---|---|
| Every distribution is **measured** | Distributions are yours to choose |
| Static vs dynamic columns decided by counting | Everything assumed static |
| Lifecycle, transition matrix, attrition **learned** from repeated cut-offs | No lifecycle unless you write one |
| Correlation between columns measured and reproducible | Correlation control does nothing |
| **Sampling**, **CTGAN** and **Hybrid** methods unlocked | Only schema-only methods |
| Results show generated-vs-real comparison charts | Generated data only |

If your sample has the same entity at several cut-offs (the same `loan_id` in
January, February, March), it is a **panel** and behaviour over time can be
learned. A single snapshot still gives you every column's shape.

### Or start from a calibrated pack

| Pack | What it is |
|---|---|
| **European CLO — Leveraged Loans** (`clo_eu_leveraged_loans`) — *offered first* | 58 columns. Loans to *companies*, several per borrower. Bullet repayment, an open pool that keeps buying for two years, a watchlist-to-distress ladder, a nine-month workout after default, and credit ratings that drift on their own. Comes with a 22-figure monthly report and its own six charts |
| **Dutch Green Loans — Residential Mortgages** (`rmbs_nl_green_lion`) | 71 columns, ESMA Annex 2. Annuity mortgages, house-price index, 8-state delinquency ladder |
| **European Auto Loans — ESMA Annex 5** (`auto_abs_esma_annex5`) | 44 columns. Depreciating collateral, balloon payments on PCP contracts, fast write-off with recovery |

The grey monospace text is the pack's identifier, which is what `sdd run` takes
at a command line. Loading a pack jumps you to Review with everything already
configured.

### ➡ Analyze data

Reads the schema, profiles the sample, and builds a configuration. This is the
slow step on a large file — profiling reads up to 200,000 rows.

---

## Step 2 — Review schema

What was detected, and what you can change before anything is generated. A wrong
key or a mistyped column is cheap to fix here and expensive to find later.

### The summary tiles

| Tile | Meaning |
|---|---|
| **Columns** | Every declared column, including helpers |
| **Static** | Same value for an entity in every period (the region a property is in) |
| **Dynamic** | Changes over time (the outstanding balance) |
| **Derived** | Computed from other columns, never sampled — so it can never disagree with them |
| **Date columns** | Detected as dates, by type or by name |
| **Nullable** | Blank at least once in your sample. These are the columns the missing-values control may blank |
| **Need review** | Inferred with low confidence. Filter to these first |

Below the tiles, two notes name the **primary key** and the **date column**, each
with the reason it was chosen — matched by name, or by behaviour ("unique within
every cut-off and recurring across them").

### The column table

| Column | Editable | Notes |
|---|---|---|
| **Column** | ✅ rename | A rename travels: the entity, the emit order, the lifecycle, the dynamics and every expression referring to it are rewritten too |
| **Type** | ✅ `int` `float` `str` `category` `bool` `date` | |
| **Role** | — | static / dynamic / derived / constant / helper |
| **Key** | — | `key` marks the primary key, `date` a date column |
| **Required** | ✅ checkbox | Unticking makes the column **optional**, which is what the missing-values control in the next step is allowed to blank. Nothing else is ever blanked |
| **Distinct** | — | How many different values were seen |
| **Null in sample** | — | How often it was blank in your data |
| **Confidence** | — | How much to trust the inferred generator. Green ≥ 0.75, amber ≥ 0.5, red below |
| **Example** | — | Two real values from your sample |

**Filter columns…** narrows the table; **Needs review** shows only the low-confidence ones.

Edits are staged and applied when you press the button — the footer tells you how
many are pending.

### ➡ Generate configuration

Applies your edits, re-validates, and moves to Configure.

---

## Step 3 — Configure

**Five groups, two of them open.** The two that are open are the ones worth
checking; the rest have working defaults, and each says on its own line what it
currently holds — so an unopened group is an answered question rather than an
unread page.

A line at the top states what pressing Generate will produce:

> **500** facilities over **36** monthly cut-offs → about **18,000** rows across
> **56** columns.

That arithmetic is the thing to read. The box asks how many *loans*; the row
count is that times the number of cut-offs, and the two differ by a factor of
thirty-six.

The badge at the top right shows `valid · N cols · Np` or a problem count. You
cannot generate while it is red, and the problems are listed on screen.

### Group 1: Size and shape *(open)*

| Control | What it does |
|---|---|
| **How many …** | Loans in the opening pool. The label names the pack's own unit — *facilities* for the CLO pack, *contracts* for auto. A pack that states the size it was calibrated for opens at that size |
| **Seed** | Same seed, same data, every time |
| **Scenario** | A named stress overlay. All three packs ship `base`, `adverse`, `severe` |
| **Number of periods** | How many cut-offs to emit |
| **Frequency** | Weekly, fortnightly, monthly, quarterly or annually. Rates below are annual either way |
| **Lifecycle** | Read-only: the states this pack moves loans through |

### Group 2: How loans behave *(open)*

Three annual rates, each a slider with the calibrated value already set.

| Control | What it does |
|---|---|
| **Default rate** | Share of performing loans defaulting within a year. Rescales the transition matrix and renormalises every row, so it stays a matrix |
| **Prepayment rate** | Annual chance a healthy loan repays early |
| **Recovery rate** | Share of the balance recovered on write-off |
| **New business** | Whether the pool keeps lending, how many join each period, and when buying stops |

A rate the configuration cannot express is disabled and says why — a pack with no
write-off state has nowhere to book a recovery.

### Group 3: Realism *(collapsed)*

How each value is drawn, and how much imperfection to add.

**Six generation methods**, from moment-matched normals to a deep tabular model.
Whatever you pick is written into the configuration as generators you can read.
CTGAN and Hybrid need sample data and the `deep` extra, and are greyed out
without them.

**Four noise controls** — noise, correlation, outliers, missing values — applied
after sampling and before anything is derived, so a ratio recomputed from a
jittered balance still matches that balance. Identifiers, dates, states and the
inputs to amortisation are never touched.

### Group 4: Columns *(collapsed)*

The distribution behind each column, filterable by name. Edit a weight or a bound
and it is validated immediately.

### Group 5: The configuration itself *(collapsed)*

The **transition matrix** — each row a state, each column where a loan might be
next period. Rows must sum to 1; the total is live and turns red if it does not.

The **YAML**, which is the whole configuration and is what actually runs.
Anything the forms do not expose can be set here. **Apply** validates and adopts
it; **Revert** discards.

### ➡ Generate data

Queues the run. Disabled while the configuration is invalid.

---

## Step 4 — Generate

Seven named stages, a progress bar and an estimated finish:

```
Reading schema  ·  Profiling sample  ·  Building configuration
Generating synthetic data  ·  Ageing portfolio  ·  Running validation
Preparing downloads
```

The first three happened in earlier steps and show as already done. The run
happens on a worker thread, so nothing depends on the browser staying open — the
estimate is a straight-line extrapolation from elapsed time, honest about being
crude.

If it fails, the error appears here in full. The commonest cause is a
configuration that is valid but impossible — for example CTGAN selected with no
sample, which is refused before the run rather than minutes into it.

---

## Step 5 — Results

### Summary tiles

**Rows generated**, **Columns**, **Entities** (everything that ever entered the
pool), **Written later** (only for an open pool), **Periods**, **Surviving**,
**Time taken**, **Validation**.

### Validation

`N of M invariants passed`. These are derived from your own configuration: if a
column is declared static, a check appears asserting it never changes; if a state
is declared terminal, a check appears asserting nothing survives it. Failures are
listed with their violating row counts.

Below it, what the randomness controls actually did — "5 columns reordered to
match the sample's correlation, 2 columns blanked" — and the scenario overlay if
one was applied.

### Charts

**Distribution comparison** is always shown: a histogram of one column, generated
against your sample on shared bins. Pick the column from the dropdown. Without a
sample you get the generated series alone. It is the one chart genuinely about
the data rather than the asset class.

**The rest depend on the pack.** A pack can declare its own charts, and when it
does they replace the generic ones — a delinquency curve means nothing for a
corporate loan, which has no days-past-due ladder.

*Packs that declare nothing* get three generic charts:

| Chart | What it shows |
|---|---|
| **Delinquency curve** | Share of the *surviving* pool in each distressed state, period by period. Shares rather than counts, because a shrinking pool makes counts fall even when behaviour is unchanged |
| **LTV distribution** | Leverage at the first cut-off against the last. A current LTV is preferred over an original one, which never moves |
| **Pool balance over time** | Outstanding balance per cut-off, with the pool factor and surviving loan count |

*The CLO pack* declares four of its own:

| Chart | What it shows |
|---|---|
| **Portfolio par** | Collateral held, cut-off by cut-off. It runs down once reinvestment ends |
| **Credit state** | Share of facilities performing, on watchlist, distressed or defaulted, stacked over time |
| **CCC share of par** | The bucket every indenture caps. It rises as credit migrates, independently of how many facilities are visibly distressed |
| **Industry concentration** | Par by industry at the final cut-off, largest first |

A chart reads its figures from the portfolio report rather than recomputing them,
so the line on screen is the same number the download carries. One that cannot be
drawn says why rather than rendering something meaningless, and does not take the
others down with it.

### Generated data

A searchable, sortable window onto the panel. **Search** matches anywhere in any
column; click a header to sort; Previous/Next page through 25 rows at a time.
Search and sort run against the file, not the page, so a twelve-million-row panel
behaves like a twelve-thousand-row one. Blanked values show as a grey `null`.

---

## Step 6 — Download

| Button | Format | What it is |
|---|---|---|
| **Download CSV** | `.csv` | The whole panel as one file. Opens anywhere |
| **Download Parquet** | `.parquet` | Columnar and typed. The right choice for anything read by code |
| **Download Excel** | `.xlsx` | A workbook with the data on one sheet and its provenance — spec hash, seed, method, timings, validation verdict — on another. Capped at 1,000,000 rows, and the file says so if it truncated |
| **Download Configuration** | `.yaml` | The exact configuration that produced this data. Re-run it and get the same file back |
| **Download Validation report** | `.html` | A standalone page listing every invariant checked and its result, plus the pool composition per period. No external assets, no scripts — safe to email or attach to a review |

Below, **Per-period files**: the consolidated panel plus one file per cut-off,
exactly as the ageing engine wrote them.

**Start over** returns to Upload with a clean slate.

---

## If you have never met a CLO

The CLO pack is offered first and uses vocabulary the other two do not. None of
it is hard; all of it is unexplained if you have not worked in credit. This
section is the five minutes that makes the rest of the pack readable.

### What a CLO is

A **CLO** — collateralised loan obligation — is a fund that borrows money,
buys a few hundred loans made to *companies*, and pays its investors out of what
those loans repay. It is a mortgage fund with companies instead of households.

Two halves, and this tool only builds one:

| the **collateral** | the **liabilities** |
|---|---|
| the several hundred loans the fund owns | the notes the fund issued to raise the money |
| what this pack generates | **not modelled** |

Everything below is about the collateral. There are no tranches, no waterfall,
no OC or IC tests and no equity returns anywhere in this product — see
[what it deliberately leaves out](#what-the-pack-deliberately-leaves-out).

### Facility and obligor — the distinction everything rests on

A **facility** is one loan. An **obligor** is the company that borrowed it.

One company usually borrows several times, so a 500-facility portfolio might
have only 200 obligors behind it. That matters because **every limit an investor
cares about is measured per obligor, not per loan.** A fund holding six loans to
the same struggling retailer is not diversified, however many loans it counts.

This is why the pack declares a **group**: `obligor_id` is generated once with
its own industry, country and revenue, and every facility belonging to it
inherits those. Without that, the same company would come out in four industries
at once and every concentration figure would be meaningless.

The panel has one row per **facility** per month. The obligor's details repeat
across its facilities, which is what you should expect to see.

### The vocabulary you will meet in the columns

| Term | In plain words |
|---|---|
| **Par** | The face value of a loan — what the borrower owes. Not what the loan would fetch if sold |
| **Market price** | What the loan *would* fetch, quoted per 100 of par. 98.5 means a small discount; 70 means the market is worried |
| **Bullet** | The loan pays interest monthly and repays the whole principal in one lump at the end. Unlike a mortgage, the balance sits flat for years and then drops to zero |
| **Spread** | The margin over a floating base rate. A riskier borrower pays a wider spread |
| **Senior secured** | First in line if the company fails. There is no house or car behind these loans — the claim is on the business itself |
| **Second lien** | Behind the senior lenders. Higher yield, worse outcome in a default |

### The credit ladder

A company loan does not fall behind in 30-day steps the way a mortgage does. It
moves through a judgement ladder instead:

```
Performing → Watchlist → Distressed → Defaulted → Recovered
                                          │
                                          └──→ (stays in the pool through workout)
```

- **Watchlist** — the manager has noticed something. Still paying.
- **Distressed** — the business is in real trouble. Still, usually, paying.
- **Defaulted** — it has stopped paying or restructured.
- **Recovered** — the **workout** finished. A default is not an instant loss:
  lawyers and administrators spend months recovering what they can, and the
  pack models that as nine months followed by roughly 62% of par coming back.
  The rest is the **realised loss**.

A facility can also leave the pool without failing: **Prepaid** (repaid early,
usually refinanced elsewhere), **Sold** (the manager traded it), or **Matured**
(it reached the end of its term). Those four exits are terminal — once a
facility takes one it stops being reported.

### The reinvestment period

Unlike a mortgage pool, a CLO **trades**. For the first two years — the
**reinvestment period** — every loan that repays is replaced with a new one, so
the portfolio stays roughly the same size while its contents change completely.
After that the fund stops buying and the portfolio simply runs down.

You can see both phases in the *Portfolio par* chart: flat for 24 months, then
falling away. The `portfolio_turnover` metric measures the churn underneath that
flat line — how much actually changed hands each month.

### Credit ratings, and what CCC means

Every loan carries a letter grade saying how likely the borrower is to keep
paying. In this pack they run **BB** (best here) → **B** → **CCC** → **D** (the
borrower has already defaulted).

**CCC means "vulnerable"** — still paying, but the agency doubts it will
continue. It has outsized importance because a CLO's own rulebook caps how much
CCC it may hold, and breaching that cap diverts cash away from the investors at
the bottom.

The rating **migrates on its own chain**, separately from the credit state. That
is deliberate and it is the point: a company is usually downgraded *while still
paying every instalment*, and that early warning is the entire reason ratings
exist. If the rating were derived from the credit state it could only ever
confirm what was already obvious.

### Reading the monthly report

The Results screen and `portfolio_metrics.csv` carry 22 figures. Most are
self-explanatory sums and averages; four are worth a note.

**Averages are weighted by par, always.** An unweighted average would count a
€40m facility and a €400k one alike, and describe no portfolio anyone holds.

| Figure | What it tells you |
|---|---|
| `ccc_par_pct` | Share of the money rated CCC or worse. The number an indenture caps |
| `largest_obligor_pct` | The biggest single borrower's share. A concentration limit in one number |
| `wa_credit_factor` | Credit quality as a single number, **higher is worse**. Ratings are letters and cannot be averaged — there is no midpoint between B+ and CCC — so each grade is given a number standing for how likely it is to default, and those are averaged |
| `effective_obligors` | How many borrowers the portfolio *behaves* as. A hundred borrowers where two hold half the money is not a hundred-name portfolio; this is the count adjusted for that. Compare it against the plain obligor count — the gap **is** the concentration |

Both of the last two resemble measures the rating agencies publish, and neither
reproduces one. `wa_credit_factor` uses factors computed from this pack's own
rating chain — raise its transition matrix to the 60th power and read the
default column — and `effective_obligors` is an inverse Herfindahl, which is
ordinary statistics. Both are pack data you can replace. The reasoning is in
[`clo/GENERIC-CREDIT-MEASURES.md`](clo/GENERIC-CREDIT-MEASURES.md).

### Stress scenarios

Pick one in **Configure → Scale**. `base` runs the calibration as written;
`adverse` and `severe` shift it.

What is worth knowing is that they do **not** shift it evenly. A uniform
multiplier — every sector worsening by the same factor — cannot express
concentration risk at all: every portfolio of the same size behaves identically
however lopsided its industry mix. So the stressed scenarios lean harder on the
sectors a downturn actually hits. Under `adverse`, a retail borrower carries
about 2.4× the base stress while a healthcare borrower carries slightly less
than the book as a whole.

That is what makes holding a quarter of the money in one sector cost something
in the output rather than only in the commentary.

### What the pack deliberately leaves out

- **No tranches, no waterfall, no OC or IC tests, no fees, no equity returns.**
  This produces the collateral pool, not the bonds issued against it.
- **No rating agency model.** Where a figure resembles one it is computed from
  this project's own assumptions, and the pack file shows the arithmetic.
- **Nothing here is investment advice, and none of the data is real.** Every
  portfolio is synthetic, generated from declared assumptions calibrated to
  published market ranges. It is for testing systems, training models and
  demonstrating pipelines — not for valuing anything.

---

## Everything only the YAML exposes

The forms cover what most runs need. The Advanced tab reaches everything else.
Below is the full vocabulary, section by section.

### `meta`

| Field | Notes |
|---|---|
| `name` | Identifier. Short, lowercase, used in filenames |
| `title` | Human-readable name, shown wherever a person picks the spec |
| `asset_class` | Free text — `rmbs`, `auto`, `cre`, … |
| `regulatory_template` | Free text, e.g. `ESMA Annex 5 — Underlying exposures, automobile` |
| `description`, `source` | Free text |

### `entity`

| Field | Notes |
|---|---|
| `id_column` | Which column identifies an entity |
| `id_format` | Format string for generated identifiers, e.g. `GL{deal_year}_{seq:06d}`. `{seq}` is the 1-based row number; other placeholders resolve against `params` and `constants`. **Sequential identifiers cannot collide** — an 8-hex-character random id collides about 29 times in 500k draws |
| `time_column` | Which column holds the cut-off date |
| `calendar.start` | First cut-off, ISO 8601 |
| `calendar.periods` | How many cut-offs |
| `calendar.freq` | `month_end`, `quarter_end`, `year_end`, **`month_start`**, **`day`** — the last two are YAML-only |

### `params` and `constants`

`params` are free-form values usable in `id_format` and every expression.
`constants` are deal-level facts written to every row — originator name, currency,
country.

### `columns`

| Field | Notes |
|---|---|
| `name` | |
| `role` | `static` (fixed per entity) · `dynamic` (moves) · `derived` (computed, no generator) · `constant` · `helper` (intermediate, dropped before output) |
| `dtype` | `int` `float` `str` `category` `bool` `date` |
| `generator` | See below |
| `domain` | Allowed values. Enforced by the validator |
| `min` / `max` | Bounds. Respected by every generation method and by the randomness controls |
| `required` | Whether the missing-values control may blank it |
| `null_rate` | **Per-column** blank rate, overriding the global setting |
| `description`, `confidence`, `review` | Documentation and provenance |

**Generator kinds** — ten, of which the Schema tab edits six:

| Kind | Fields |
|---|---|
| `categorical` | `values`, `weights` |
| `conditional_categorical` | `parent`, `mapping` (parent value → candidate values), `weights`, `default`. Used for province → region: each parent value has its own pool |
| `scipy` | `dist` (any `scipy.stats` name), `params`, `decimals`, `clip_min`, `clip_max` |
| `gaussian` | `mean`, `stddev`, `decimals`, `clip_min`, `clip_max` |
| `uniform` | `low`, `high`, `decimals` |
| `bernoulli` | `p`, `true_value`, `false_value` |
| `empirical` | `values`, `weights`, `decimals` — resamples observed values, preserving point masses exactly |
| `sequence` | `prefix`, `start`, `width` |
| `uuid` | `prefix`, `short`, `uppercase` |
| `constant` | `value` |

### `derivations`

Deterministic columns computed from others. Four kinds:

| Kind | Fields |
|---|---|
| `expr` | A vectorised expression: `original_balance / (oltomv_original / 100.0)` |
| `when` | Ordered `rules` of `if` / `then`, plus `else`. First match wins |
| `bucket` | `bucket` (a named binning) + `source` (the column to bin) |
| `format` | `template` (a format string) + `args` (placeholder → expression), for date-proxy columns |

Each also takes `round`, `dtype`, and **`stage`**:

- `book` — period 0 only (origination facts)
- `period` — every ageing period (recomputed as balances move)
- `both`

Expressions are evaluated by a restricted AST walker — a spec is data, never
executable code. Available functions: `abs min max round floor ceil clip log
log10 exp sqrt power where isin coalesce isnull notnull int float str`, plus the
constants `pi` and `e`. `and` / `or` / `not` and `x if cond else y` work
element-wise.

### `buckets`

Reusable binning rules: `bins` (edges), `labels`, `right`, `include_lowest`.
`len(labels)` must equal `len(bins) - 1`.

### `lifecycle`

| Field | Notes |
|---|---|
| `states` | Ordered **best-first**. Scenarios read that order as severity |
| `state_column` | Which column holds the label |
| `transitions` | Square matrix; each row sums to 1 |
| `transition_states` | Which states the matrix covers. Defaults to states minus terminal |
| `absorbing` | Cannot be left, but stays in the pool — a defaulted loan still gets reported |
| `terminal` | Ends the entity's life. The row is written once, then drops out |
| `hazards` | See below |
| `state_fields` | state → `{column: forced value}`, applied after every transition. A forced value always wins |
| `initial_distribution` | Optional state mix at period 0 |

**Hazards** — events decided outside the matrix:

- `bernoulli`: `name`, `to_state`, and exactly one of `annual_rate` or
  `period_rate`; optional `from_states`, `excluded_states`
- `dwell_time`: `name`, `from_state`, `to_state`, `periods` — fires after N
  consecutive periods in a state

### `dynamics`

**`amortisation`** — `kind` is one of `annuity` · `linear` · `bullet` ·
`interest_only` · `revolving` · `depreciation` · `none`. Plus `balance`, `rate`,
`payment`, `term`, `only_when_state` (states in which a payment is assumed —
anything else freezes the balance), `flat_when` (an expression selecting rows that
never amortise), `rate_per_period`, `floor`.

**`indices`** — multiplicative overlays: `name`, `applies_to`, `kind`
(`constant_drift` or `series`), `annual`, `series`, `volatility`.

**`counters`** — `column`, and exactly one of `step` (fixed increment) or `expr`
(recomputed each period), plus `clip_min`, `clip_max`, `dtype`.

**`accruals`** — `column`, `add` (a column or a number), `when`
(`not_performing` · `in_states` · `always`), `states`, `reset_states`,
`performing_state`.

**`recovery`** — `rate`, `balance`, `target`, `on_states`.

### `generation`

| Field | Forms? | Notes |
|---|---|---|
| `method` | ✅ | |
| `noise`, `correlation`, `outliers`, `missing` | ✅ | |
| **`outlier_sigma`** | ❌ | How far into the tail an outlier is pushed. Default 4.0 |
| **`correlation_target`** | ❌ | `columns` + `matrix`, written by the profiler |
| **`polish_model`** | ❌ | `ctgan` or `tvae` |
| **`polish_epochs`** | ❌ | Default 300 |

### `originations`

| Field | Forms? | Notes |
|---|---|---|
| `rate` **or** `per_period` | ✅ (rate) | Exactly one. A rate is a share of the opening book |
| **`start_period`** | ❌ | First cut-off new entities appear at. Default 1 |
| **`end_period`** | ❌ | Last one. Default: every period |
| **`fresh`** | ❌ | Newly written rather than acquired. Default true |
| **`reset`** | ❌ | column → literal value forced on a new entity |
| **`reset_expr`** | ❌ | column → expression, with `period`, `period_year`, `period_month`, `period_day` available. This is how a loan written in June gets June's origination date — reset the date a derivation *reads*, and everything computed from it follows. Resetting a derived column directly does not work: the derivation recomputes it |

### `scenarios`

Named overlays. `default_multiplier` scales every transition that worsens an
entity's state; `prepayment_multiplier` scales hazard rates; `index_shift` is an
index name → additive change to its annual rate; `rate_shift` + `rate_columns`
add percentage points to interest-rate columns.

### `emit`

`filename` (placeholders `{name}` `{yyyy}` `{mm}` `{yyyymm}` `{yyyy_mm_dd}`
`{period}`), `column_order`, `formats` (`csv`, `parquet`), `panel_filename`,
`write_panel`, `float_format`.

### `validation`

**`checks`** — eight toggles, all on by default:
`static_columns_stable`, `ids_unique_per_period`, `closed_pool`,
`terminal_states_absorb`, `domains_respected`, `counters_step_correctly`,
`state_fields_applied`, `non_negative_balances`.

**`non_negative_columns`** — columns asserted never to go below zero. The
generation methods respect this as a floor, not just the validator.

**`custom`** — your own checks: `name`, `description`, and `sql`, a SELECT over
the relation `panel` returning violating rows. Zero rows means it passed.

---

## The command line

Everything the UI does, `sdd.api` does, and the CLI is a thin wrapper over it.

```bash
sdd ui                                open the web UI (--host, --port, --reload)
sdd packs                             list the bundled packs
sdd profile SAMPLE                    analyse a tape, print what it found
sdd design SAMPLE -o spec.yaml        analyse it and write a runnable spec
sdd check SPEC                        validate a spec without running it
sdd run SPEC -n 50000 -o ./out        generate and age
sdd validate SPEC PANEL               check a panel against its spec
sdd fidelity REFERENCE SYNTHETIC      score synthetic against real
```

`sdd run` takes `--num-records/-n`, `--out/-o`, `--seed`, `--periods`,
`--scenario`, `--backend` (`numpy` or `nemo`), `--skip-validation` and
`--quiet/-q`.

A configuration downloaded from the UI runs unchanged:

```bash
sdd run ~/Downloads/green_lion.yaml -n 100000 -o ./out
```

---

## Troubleshooting

**The page looks wrong after an update.** Static files are served with
`Cache-Control: no-cache`, so a hard reload is not normally needed. If you are
running an older build, restart `sdd ui`.

**"This configuration will not run."** The red badge lists every problem. They
are written to be actionable — *"transition matrix row 0 ('Performing') sums to
0.857500, expected 1.0"* — and none of the underlying validation library's
plumbing reaches you.

**A method is greyed out.** Sampling needs a sample; CTGAN and Hybrid need a
sample *and* `pip install '.[deep]'`.

**The missing-values slider does nothing.** Every column is marked required. Go
back to Review and untick Required on the columns that may be blank.

**The correlation slider does nothing.** No sample was analysed, so no
relationship between columns was ever measured.

**Validation failed.** Open the report from the Download step: it names each
check, what it asserts, and how many rows violated it. A failure is usually a
configuration that contradicts itself — two rules writing the same column, or a
declared bound the generator can exceed.

**Excel says the file is truncated.** Excel cannot hold more than ~1,048,576
rows. The workbook's `about` sheet records how many were generated; use CSV or
parquet for the whole panel.
