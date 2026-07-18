# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Generate ``notebooks/00_data_bible.ipynb`` — the Fannie Mae dataset 'bible'.

Kept as a builder (not a hand-written .ipynb) so the notebook is regenerated deterministically and
reviewed as plain Python. Run from anywhere::

    python notebooks/build_data_bible.py

The notebook itself reads ``reports/fannie_dataset_profile.json`` (produced by
``scripts/profile_fannie_dataset.py``); glossary + include/exclude lists are derived live from
``reference_implementations/fannie_mae/fannie_glossary.py`` and ``configs/fannie_mae/``.
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "notebooks" / "00_data_bible.ipynb"


def md(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_markdown_cell(text.strip("\n"))


def code(text: str) -> nbf.NotebookNode:
    return nbf.v4.new_code_cell(text.strip("\n"))


CELLS = [
    md(r"""
# The Fannie Mae Loan-Performance Data Bible

**One notebook that documents the entire dataset**: every column's meaning, which columns the model
is allowed to see (and which are held out as leakage), and the headline risk signal — the
**delinquency / default rate by year** across the whole loan book.

It reads a pre-computed statistics artifact so it renders instantly; regenerate that artifact with:

```bash
# representative 4% panel (fast; the deterministic hash sample is unbiased for rates)
python scripts/profile_fannie_dataset.py \
    --panel gs://sriram-credit-fm-data/output/raw/fannie_mae/panel_2000_2024.parquet \
    --out reports/fannie_dataset_profile.json

# OR the TRUE whole loan book, straight from the raw source
python scripts/profile_fannie_dataset.py \
    --raw-root gs://sriram-credit-fm-data/fannie_by_reporting \
    --out reports/fannie_dataset_profile.json --no-vintage
```

**Contents**
1. Setup &amp; overview
2. Column glossary (all 113 source fields + 6 derived)
3. Columns *included* in training vs *excluded* / *leakage*
4. Delinquency &amp; default rate by year
5. Per-column statistics
6. How to (re)produce this data — ingest
7. Notes &amp; caveats
"""),

    # ---------------------------------------------------------------- setup
    md("## 1. Setup &amp; overview"),
    code(r"""
import importlib.util
import json
from pathlib import Path

import pandas as pd
import yaml

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", 100)

# find the repo root (walk up until we see configs/)
ROOT = Path.cwd()
while not (ROOT / "configs" / "fannie_mae").exists() and ROOT != ROOT.parent:
    ROOT = ROOT.parent
assert (ROOT / "configs" / "fannie_mae").exists(), "run inside the credit-foundation-model repo"

# import the glossary module directly (avoids importing credit_fm/__init__, which pulls in torch)
_gspec = importlib.util.spec_from_file_location(
    "fannie_glossary", ROOT / "reference_implementations" / "fannie_mae" / "fannie_glossary.py")
G = importlib.util.module_from_spec(_gspec)
_gspec.loader.exec_module(G)

# resolve the ingested-panel output path from the recipe that produced it (config is the truth)
_common = yaml.safe_load((ROOT / "configs" / "fannie_mae" / "common.yaml").read_text())
_ingest = yaml.safe_load((ROOT / "configs" / "fannie_mae" / "ingest_2000_2024.yaml").read_text())
_raw_dir = _common["paths"]["raw"].replace("${gcs_root}", _common["gcs_root"])
PANEL_PATH = f"{_raw_dir}/{_ingest['combined_name']}"

# Load the pre-computed statistics profile. The full profile (`fannie_dataset_profile.json`, with
# per-column stats) drives every section; if it is absent we fall back to the lightweight
# `delinquency_4pct.json` so the overview + delinquency sections still render (only the per-column
# stats in section 5 need the full profile).
FULL_PROFILE = ROOT / "reports" / "fannie_dataset_profile.json"
DLQ_PROFILE = ROOT / "reports" / "delinquency_4pct.json"
if FULL_PROFILE.exists():
    PROFILE = json.loads(FULL_PROFILE.read_text())
    PROFILE_SRC = FULL_PROFILE.name
elif DLQ_PROFILE.exists():
    PROFILE = json.loads(DLQ_PROFILE.read_text())
    PROFILE_SRC = DLQ_PROFILE.name + " (delinquency-only — section 5 needs the full profile)"
else:
    PROFILE = None
    PROFILE_SRC = None
HAS_COLUMN_STATS = bool(PROFILE and PROFILE.get("columns"))
print("glossary fields:", len(G.ALL_FIELDS), "| profile:",
      PROFILE_SRC or "MISSING (run scripts/profile_fannie_dataset.py — see section 4d for commands)")
"""),
    code(r"""
print(f"panel path : {PANEL_PATH}")
if PROFILE:
    print(f"source     : {PROFILE['source']}  ({PROFILE['source_kind']})")
    print(f"generated  : {PROFILE['generated_utc']}")
    print(f"rows       : {PROFILE['n_rows']:,}  (loan-months)")
    print(f"loans      : {PROFILE['n_loans']}")
    print(f"columns    : {PROFILE['n_columns']}  (0 = delinquency-only profile; run a full profile "
          f"for per-column stats)")
    print(f"reporting  : {PROFILE['reporting_range'][0]} .. {PROFILE['reporting_range'][1]}")
    print(f"origination: {PROFILE['origination_range'][0]} .. {PROFILE['origination_range'][1]}")
else:
    print("No profile artifact yet — generate one to populate the overview, delinquency, and "
          "per-column sections (see the commands in section 4d).")
"""),

    # ---------------------------------------------------------------- glossary
    md(r"""
## 2. Column glossary

Every field in the dataset, condensed from the official *Single-Family Loan Performance Dataset and
Credit Risk Transfer — Glossary and File Layout* (© 2026 Fannie Mae). `position` is the 1-based
field position in the published layout; the last 6 rows (`position = derived`) are columns our
ingest step adds so the rest of the pipeline stays asset-generic.
"""),
    code(r"""
def glossary_frame(fields):
    rows = []
    for name, (pos, plain, dtype, desc, enums) in fields.items():
        rows.append({"position": pos if pos is not None else "derived", "column": name,
                     "name": plain, "type": dtype, "description": desc,
                     "enumerations": enums or ""})
    df = pd.DataFrame(rows)
    # raw fields first (by position), derived last
    df["_sort"] = [p if isinstance(p, int) else 10_000 + i for i, p in enumerate(df["position"])]
    return df.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

GLOSSARY = glossary_frame(G.ALL_FIELDS)
print(f"{len(GLOSSARY)} fields ({len(G.RAW_FIELDS)} source + {len(G.DERIVED_FIELDS)} derived)")
GLOSSARY
"""),
    md("**The 6 derived columns** (labels &amp; keys the pipeline computes) in detail:"),
    code(r"""
glossary_frame(G.DERIVED_FIELDS)
"""),

    # ---------------------------------------------------------------- include / exclude
    md(r"""
## 3. Columns *included* in training vs *excluded* / *leakage*

The included/excluded split is **derived live** from `configs/fannie_mae/raw_schema.yaml` (all 113
source fields) and `configs/fannie_mae/baseline.yaml` (the `exclude` / `leakage` lists + the
id/time/label/gate roles), so this table can never drift from the config the model actually uses.

### The golden rule

We predict: **"Will this loan — healthy today — go bad in the next 12 months?"** So the model may
use only what you'd genuinely know **today, about a healthy loan**. One test sorts almost every
column:

> **Imagine a loan that is perfectly healthy right now. Would this column already have a meaningful
> value?**
> - **No** — it only fills in *after* trouble starts → **leakage** (using it = peeking at the answer).
> - **Yes, but it's a name / ID / duplicate / too-fine geography** → not cheating, just useless or
>   risky → **excluded**.

* **Model features** — everything not excluded and not leakage.
* **Excluded (non-features)** — ids, raw dates (superseded by derived ISO dates), high-cardinality
  geo, and non-tabular strings.
* **Leakage** — outcome / contemporaneous-state / post-default servicing columns.

### "If the delinquency columns are removed, how does training know a loan failed?"

The leakage rules apply to the model's **inputs**, not to the **answer key**. Two separate tracks:

* **Features (the question)** — what the model reads at the observation date; every delinquency /
  outcome column is stripped from this.
* **Label (the answer)** — `default_event`, which is *computed from* those same delinquency markers
  (`dlq_num >= 6`, or a credit-event zero-balance code) but kept on a separate track and pointed at
  the **future**: at observation date T we look forward `horizon_months` and ask "did this loan
  default by T+12?". That forward-looking yes/no is the label.

So the delinquency data **defines the answer key — it is just banned from the question paper.** It's
like a medical study: you record today's blood pressure and cholesterol (features), wait a year and
note who had a heart attack (label, which of course needs the outcome), then train the model to
predict the heart attack from *today's* vitals. Feeding the heart-attack record back in as an input
would be the leak — the model would score ~perfectly in testing and be useless on a living patient.
(This is exactly why the leaky config scores ROC 0.93 and the honest, gated one scores 0.73.)
"""),
    code(r"""
def load_yaml(path):
    import yaml
    return yaml.safe_load(Path(path).read_text())

schema = load_yaml(ROOT / "configs" / "fannie_mae" / "raw_schema.yaml")
base = load_yaml(ROOT / "configs" / "fannie_mae" / "baseline.yaml")

all_cols = [c["name"] for c in schema["columns"]]
exclude = set(base.get("exclude", []))
leakage = set(base.get("leakage", []))
roles = {base.get("id_col"), base.get("time_col"), base.get("label_col"), base.get("gate_col")}
roles.discard(None)
# the id is published as `loan_identifier` and renamed to `loan_id` (the configured id_col) at ingest
ID_RAW = "loan_identifier"

features = [c for c in all_cols
            if c not in exclude and c not in leakage and c not in roles and c != ID_RAW]
excluded = [c for c in all_cols if c in exclude]
leak_raw = [c for c in all_cols if c in leakage]

print(f"source fields          : {len(all_cols)}")
print(f"  model features       : {len(features)}")
print(f"  excluded (non-feature): {len(excluded)}")
print(f"  leakage (held out)   : {len(leak_raw)}")
print(f"  role cols (id/time/label/gate): {sorted(roles)}")
"""),
    md("### 3a. Model features (what the model *is* trained on)"),
    code(r"""
pd.DataFrame([{"column": c, "name": G.ALL_FIELDS[c][1], "type": G.ALL_FIELDS[c][2],
               "description": G.ALL_FIELDS[c][3]} for c in features]).reset_index(drop=True)
"""),
    md(r"""
### 3b. Excluded non-feature columns — "not cheating, just not good features"

These wouldn't leak the answer, but they'd add noise, duplicate something we already keep, or create
fairness risk. Four groups:

1. **Names & IDs** (`reference_pool_id`, `seller_name`, `servicer_name`, `master_servicer`,
   `deal_name`) — random labels; which pool or bank serviced the loan says nothing about the
   borrower's risk, and "which lender" can proxy for neighborhood/race (**fair-lending risk**).
2. **Raw dates we already use elsewhere** (`monthly_reporting_period` → derived `reporting_date`;
   `origination_date` → the temporal **split key**, not a feature; `first_payment_date`,
   `maturity_date`, and the IO/ARM schedule dates) — redundant with loan age / term / rate, and
   feeding the calendar date would let the model memorize "2007 loans defaulted" instead of learning
   credit risk.
3. **Too-fine geography** (`metropolitan_statistical_area`, `zip_code_short`) — thousands of codes →
   overfitting, and a classic redlining proxy. We keep `property_state` (coarse, safe granularity).
4. **A non-tabular string** (`loan_payment_history`) — a 24-character coded string of past-due
   status: not a clean number *and* basically a compressed delinquency history.
"""),
    code(r"""
pd.DataFrame([{"column": c, "name": G.ALL_FIELDS[c][1], "why": G.ALL_FIELDS[c][3]}
              for c in excluded]).reset_index(drop=True)
"""),
    md(r"""
### 3c. Leakage columns — "these secretly contain the answer"

Every one is **blank or zero for a healthy loan** and only gets a value once the loan is already in
trouble or already ended. If the model sees a value here, it's reading the outcome, not predicting
it. Six groups:

1. **The outcome itself** — `current_loan_delinquency_status`, `dlq_num` ("how many months behind"),
   plus our derived labels `default_event` / `prepay_event` / `is_performing`.
2. **Loan termination / zero-balance** (only filled when the loan *ends*) — `zero_balance_code` (+
   dates), `upb_at_the_time_of_removal`, `repurchase_date`.
3. **Foreclosure & loss dollars** (only exist *after* default) — `foreclosure_date`,
   `disposition_date`, `last_paid_installment_date`, and every cost/proceeds/write-off/accrued-
   interest amount booked while repossessing and selling a defaulted home.
4. **Modifications & workouts** ("the treatment reveals the disease") — `modification_flag`,
   principal forgiveness, modification/credit-event losses, `borrower_assistance_plan`, deferrals,
   and alternative delinquency resolutions: you only get these once you're *already* missing payments.
5. **REO listing** (`original_list_*`, `current_list_*`) — the property is being sold post-foreclosure.
6. **Loan holdback** (`loan_holdback_indicator` + date) — a distress hold Fannie places on loans
   about to hit a credit event.

**The subtle bit:** `current_loan_delinquency_status` is banned but `current_actual_upb` and
`current_interest_rate` are **kept** — same word "current," opposite verdict. Every healthy loan has
a current balance and rate (safe); only a *sick* loan has a non-zero delinquency status (leak).
Contemporaneous is fine; contemporaneous-*and-only-exists-when-things-go-wrong* is not.
"""),
    code(r"""
pd.DataFrame([{"column": c, "name": G.ALL_FIELDS[c][1], "why": G.ALL_FIELDS[c][3]}
              for c in leak_raw]).reset_index(drop=True)
"""),
    md(r"""
### 3d. Complete panel schema — every column in the ingested output

The final ingest artifact and its full column list. Ingest keeps **all** source fields (renaming
`loan_identifier` → `loan_id`, rewriting `origination_date` to an ISO string) and appends the six
derived columns, so the panel has all 113 source fields + 5 new derived = **118 columns**. Each row
below is annotated with the role the pipeline assigns it (`feature` / `excluded` / `leakage` /
`id` / `time` / `label` / `gate`).
"""),
    code(r"""
print("final ingest output:", PANEL_PATH)

DERIVED_APPENDED = ["reporting_date", "dlq_num", "default_event", "prepay_event", "is_performing"]
# panel columns in file order: raw fields (loan_identifier shown as its renamed loan_id) + derived
raw_in_order = [("loan_id" if c["name"] == "loan_identifier" else c["name"], c["index"])
                for c in schema["columns"]]
panel = [(name, f"raw#{idx}") for name, idx in raw_in_order] + [(c, "derived") for c in DERIVED_APPENDED]

def role_of(col):
    roles_map = {base.get("id_col"): "id", base.get("time_col"): "time",
                 base.get("label_col"): "label", base.get("gate_col"): "gate"}
    if col in roles_map:
        return roles_map[col]
    if col in leakage:
        return "leakage"
    if col in exclude:
        return "excluded"
    return "feature"

SCHEMA = pd.DataFrame([{
    "column": col, "source": src, "role": role_of(col),
    "type": G.ALL_FIELDS.get(col, (None, None, "?"))[2],
    "name": G.ALL_FIELDS.get(col, (None, col))[1],
} for col, src in panel])
print(f"{len(SCHEMA)} columns  |  " + "  ".join(f"{r}={n}" for r, n in SCHEMA.role.value_counts().items()))
SCHEMA
"""),
    md("**Verify this contract against the actual file.** The profiler already read the real panel "
       "and recorded every column it saw, so we cross-check the config-derived schema above against "
       "the profiler's columns — no GCS access needed here (it's in the profile artifact). This only "
       "works with the *full* profile; a delinquency-only profile doesn't carry column names."),
    code(r"""
if HAS_COLUMN_STATS:
    actual = set(PROFILE["columns"])
    expected = set(SCHEMA["column"])
    missing, extra = expected - actual, actual - expected
    print(f"profiled panel columns: {len(actual)}  |  expected from config: {len(expected)}")
    print("MATCH ✓ — the real panel schema matches this contract" if not missing and not extra
          else f"MISMATCH — missing from file: {missing or '{}'}  |  extra in file: {extra or '{}'}")
else:
    print("Load the FULL profile (reports/fannie_dataset_profile.json) to verify these columns "
          "against the real file — the profiler records the actual panel schema when it runs.\\n"
          "Generate it with:  python scripts/profile_fannie_dataset.py "
          "--panel " + PANEL_PATH + " --out reports/fannie_dataset_profile.json")
"""),

    # ---------------------------------------------------------------- delinquency by year
    md(r"""
## 4. Delinquency &amp; default rate by year

Two complementary views of credit risk across the whole book:

* **By reporting (calendar) year** — of all loan-months observed in year *Y*, the share that were
  30+ days past due, 180+ days past due (**D180**, our default threshold), in a **default_event**
  (D180 *or* a credit-event zero-balance code), and **performing**.
* **By vintage (origination year)** — of all loans *originated* in year *Y*, the share that **ever**
  hit a default_event over their observed life (loan-level lifetime default rate).
"""),
    code(r"""
if PROFILE and PROFILE["delinquency_by_reporting_year"]:
    dlq = pd.DataFrame(PROFILE["delinquency_by_reporting_year"]).set_index("year")
    display(dlq)
else:
    dlq = None
    print("No profile — generate reports/fannie_dataset_profile.json first.")
"""),
    code(r"""
if dlq is not None:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(dlq.index, dlq["dpd30_plus_pct"], marker="o", label="30+ days past due")
    ax.plot(dlq.index, dlq["d180_plus_pct"], marker="s", label="180+ days (D180)")
    ax.plot(dlq.index, dlq["default_event_pct"], marker="^", label="default_event (D180 or credit ZBC)")
    ax.set_xlabel("reporting year")
    ax.set_ylabel("% of loan-months")
    ax.set_title("Delinquency & default rate by reporting year — whole loan book")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.show()
"""),
    code(r"""
if PROFILE and PROFILE["vintage_default_by_origination_year"]:
    vint = pd.DataFrame(PROFILE["vintage_default_by_origination_year"]).set_index("origination_year")
    display(vint)
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.bar(vint.index, vint["lifetime_default_pct"], color="#b3122f")
    ax.set_xlabel("origination (vintage) year")
    ax.set_ylabel("% of loans ever in default")
    ax.set_title("Lifetime default rate by vintage — whole loan book")
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()
else:
    print("No vintage table (profile missing or built with --no-vintage).")
"""),

    md(r"""
### 4d. Is the 4% sample representative? — 4% panel vs 100% book

The pretraining panel is a **deterministic 4% hash sample on `loan_id`** (whole loan histories kept
or dropped together). This section proves the sample reproduces the whole book's 26-year default
curve — including the 2008–2012 crisis and the 2020–2021 COVID spike — not just its average.

Generate the two delinquency profiles (each streams ~5 columns, so the full-book pass is cheap),
then re-run this notebook:

```bash
# 4% sample (your pretraining panel)
python scripts/profile_fannie_dataset.py \
    --panel gs://sriram-credit-fm-data/output/raw/fannie_mae/panel_2000_2024.parquet \
    --out reports/delinquency_4pct.json --delinquency-only

# 100% whole loan book, straight from the raw source
python scripts/profile_fannie_dataset.py \
    --raw-root gs://sriram-credit-fm-data/fannie_by_reporting \
    --out reports/delinquency_100pct.json --delinquency-only --no-vintage --no-loan-count
```

The **pooled** (loan-month-weighted) default rate is the robust headline; per-year gaps in thin
years (very low base rates) are just sampling noise. Note the panel stops at 2024 while the raw book
includes partial-2025, so part of any pooled gap is that **window mismatch**, not sampling bias.
"""),
    code(r"""
DLQ_4PCT = ROOT / "reports" / "delinquency_4pct.json"
DLQ_100PCT = ROOT / "reports" / "delinquency_100pct.json"
if DLQ_4PCT.exists() and DLQ_100PCT.exists():
    CMP = importlib.util.spec_from_file_location("cmp", ROOT / "scripts" / "compare_profiles.py")
    cmp = importlib.util.module_from_spec(CMP)
    CMP.loader.exec_module(cmp)
    prof_4, prof_100 = json.loads(DLQ_4PCT.read_text()), json.loads(DLQ_100PCT.read_text())
    LA, LB = "4% sample", "100% book"
    yt = cmp._year_table(prof_4, prof_100, LA, LB)
    pa, pb = cmp._pooled(prof_4), cmp._pooled(prof_100)
    rel = cmp._rel(pa["default_event_pct"], pb["default_event_pct"])
    verdict = "REPRESENTATIVE" if (rel is not None and abs(rel) <= 5.0) else "REVIEW"
    print(f"{LA}: {prof_4['n_rows']:,} rows   {LB}: {prof_100['n_rows']:,} rows")
    print(f"pooled default rate — {LA}: {pa['default_event_pct']}%   {LB}: {pb['default_event_pct']}%"
          f"   (Δ {round(pa['default_event_pct'] - pb['default_event_pct'], 4)} pp, {rel}% rel)")
    print(f"VERDICT: {verdict}  (pooled |rel| <= 5%)")
    display(yt[[f"default_event_pct__{LA}", f"default_event_pct__{LB}",
                "default_event_pct__diff_pp", "default_event_pct__diff_rel%"]])
else:
    yt = None
    print("Provide reports/delinquency_4pct.json and reports/delinquency_100pct.json "
          "(see the commands above) to activate this comparison.")
"""),
    code(r"""
if yt is not None:
    import matplotlib.pyplot as plt
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.2))
    yr = yt.index
    ax1.plot(yr, yt["default_event_pct__4% sample"], marker="o", label="4% sample")
    ax1.plot(yr, yt["default_event_pct__100% book"], marker="s", label="100% book")
    ax1.set_title("Default rate by year — sample vs book")
    ax1.set_xlabel("reporting year")
    ax1.set_ylabel("default_event %")
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    ax2.bar(yr, yt["default_event_pct__diff_pp"], color="#6a51a3")
    ax2.axhline(0, color="k", lw=0.8)
    ax2.set_title("Gap: sample − book (percentage points)")
    ax2.set_xlabel("reporting year")
    ax2.set_ylabel("Δ pp")
    ax2.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()
"""),

    # ---------------------------------------------------------------- per-column stats
    md(r"""
## 5. Per-column statistics

Computed by `scripts/profile_fannie_dataset.py` in a single memory-bounded streaming pass over the
dataset. Numeric columns report min/mean/std and quantiles (quantiles from a 200k reservoir sample);
categorical columns report their top values; distinct counts are exact up to a 200k cap.

**These tables need the *full* profile** (with per-column stats), not a delinquency-only one. If the
cells below are empty, generate it once (a few minutes on the 4% panel) and re-run this notebook:

```bash
python scripts/profile_fannie_dataset.py \
    --panel gs://sriram-credit-fm-data/output/raw/fannie_mae/panel_2000_2024.parquet \
    --out reports/fannie_dataset_profile.json
```
"""),
    code(r"""
def stats_frames(profile):
    numeric, categ = [], []
    for name, s in profile["columns"].items():
        base = {"column": name, "n": s["n"], "nulls": s["nulls"], "null_%": s["null_pct"],
                "n_unique": s["n_unique"]}
        if s["kind"] == "numeric" and s.get("numeric"):
            numeric.append({**base, **s["numeric"]})
        else:
            top = s.get("top_values") or []
            base["min"], base["max"] = s.get("min"), s.get("max")
            base["top_values"] = ", ".join(f"{v}={p}%" for v, c, p in top[:6])
            categ.append(base)
    return (pd.DataFrame(numeric).set_index("column") if numeric else pd.DataFrame(),
            pd.DataFrame(categ).set_index("column") if categ else pd.DataFrame())

if HAS_COLUMN_STATS:
    NUM, CAT = stats_frames(PROFILE)
    print(f"{len(NUM)} numeric + {len(CAT)} categorical/date columns profiled")
else:
    NUM = CAT = pd.DataFrame()
    print("Loaded profile has no per-column stats (delinquency-only). "
          "Generate the full profile above, then re-run — sections 5a-5c will populate.")
"""),
    md("### 5a. Numeric columns"),
    code("NUM if not NUM.empty else 'no per-column stats — generate the full profile (see above)'"),
    md("### 5b. Categorical &amp; date columns (top values)"),
    code("CAT if not CAT.empty else 'no per-column stats — generate the full profile (see above)'"),
    md("### 5c. Missingness — most-null columns"),
    code(r"""
if PROFILE and PROFILE["columns"]:
    miss = pd.DataFrame([{"column": n, "null_%": s["null_pct"]}
                         for n, s in PROFILE["columns"].items()])
    miss = miss.sort_values("null_%", ascending=False).set_index("column")
    display(miss.head(30))
    import matplotlib.pyplot as plt
    top = miss[miss["null_%"] > 0].head(25)
    if not top.empty:
        fig, ax = plt.subplots(figsize=(11, max(3, 0.32 * len(top))))
        ax.barh(top.index[::-1], top["null_%"][::-1], color="#4c72b0")
        ax.set_xlabel("% null")
        ax.set_title("Most-missing columns")
        plt.tight_layout()
        plt.show()
"""),

    # ---------------------------------------------------------------- caveats
    md(r"""
## 6. How to (re)produce this data — ingest

The panel this bible documents is written by **stage 1, `ingest_fannie_mae.py`**: it reads the raw
Hive-partitioned source, derives the modelling columns (`reporting_date`, `origination_date`,
`default_event`, `is_performing`, …), optionally loan-samples, and writes one combined panel.

```bash
# the 10% panel used for the scaling run (a deterministic loan-hash sample)
python scripts/ingest_fannie_mae.py -c configs/fannie_mae/ingest_2000_2024.yaml \
    --sample_pct 10 --combined_name panel_2000_2024_10pct.parquet

# the default 4% reference panel
python scripts/ingest_fannie_mae.py -c configs/fannie_mae/ingest_2000_2024.yaml

# always audit the result (re-derives the labels + checks the sample rate)
python scripts/validate_ingest.py \
    --panel gs://sriram-credit-fm-data/output/raw/fannie_mae/panel_2000_2024_10pct.parquet \
    --sample-pct 10
```

`--sample_pct` sets the loan-hash sample (4% is proven representative in section 4d; a bigger sample
just adds loans, it doesn't shift the rates). `--combined_name` names the output file so a new
sample never overwrites the old one. Then regenerate this notebook's stats artifact with the
`profile_fannie_dataset.py` command from the top of the notebook. Pipeline position:

```
[ INGEST ] → prepare_data (split) → train_tokenizer → encode → pretrain
```
"""),

    md(r"""
## 7. Notes &amp; caveats

* **Sampling & representativeness.** The default profile runs on the ingested **4% panel**, a
  *deterministic hash sample on `loan_id`* — whole loan histories are kept or dropped together, so
  observed rates are **unbiased estimates** of the whole book. Point `--raw-root` at the raw source
  for exact whole-book numbers.
* **Label definition.** `default_event = (dlq_num >= 6, i.e. D180) OR zero_balance_code ∈
  {02, 03, 09, 15}`. `is_performing` gates the task to loans current at the observation date so the
  model predicts **new** defaults, not ones already in progress.
* **Leakage discipline.** Section 3c columns are never features. Splits are **by `loan_id`** (never
  by row) and temporal by **origination date** — see `docs/data/fannie_mae.md` and the decision log.
* **Unknown delinquency.** `current_loan_delinquency_status = 'XX'` (or blank after removal) becomes
  `dlq_num = <NA>`; every downstream consumer resolves it with `.fillna(False)`.
* **Field availability drifts over time.** Some fields are populated only after certain releases
  (e.g. `*_classic_fico` from Dec-2025, several loss/servicing fields post-2020) — high null % on
  those columns is expected, not a data error.
"""),
]


def main() -> None:
    nb = nbf.v4.new_notebook()
    nb["cells"] = CELLS
    nb["metadata"] = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    }
    OUT.write_text(nbf.writes(nb))
    print(f"wrote {OUT}  ({len(CELLS)} cells)")


if __name__ == "__main__":
    main()
