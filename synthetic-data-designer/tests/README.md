# SQL Test Suite — Synthetic Dutch RMBS panel

53 SQL tests covering schema, domain constraints, lifecycle correctness,
static-field coherence across cutoffs, temporal consistency, pool integrity
(closed-pool semantics) and distribution sanity. Tests are written for
DuckDB but use only portable SQL (with the exception of `read_csv_auto`'s
glob loading — replace with a `CREATE TABLE … AS SELECT` step for BigQuery
/ Snowflake / Postgres).

## Run

Install DuckDB if you haven't:

```bash
pip install duckdb
```

Then run the suite against any directory of cutoff CSVs:

```bash
python tests/run_sql_tests.py --cutoff-dir ./out/cutoffs
```

CI / scripted mode (non-zero exit on any failure):

```bash
python tests/run_sql_tests.py --cutoff-dir ./out/cutoffs --fail-on-violation
```

Or run the raw SQL via the DuckDB CLI:

```bash
duckdb -c "SET VARIABLE cutoff_glob = './out/cutoffs/green_lion_*.csv'; .read tests/test_panel.sql"
```

## What's tested

| Section | Tests | What it checks |
|---|---|---|
| **A — Schema** | 4 | Row count, 24 distinct cutoffs, 71 columns, loan_id uniqueness per cutoff. |
| **B — Deal-level constants** | 8 | Currency, country, originator, servicer, single closing date / transaction name / ESMA ID, ISO 8601 date format. |
| **C — Domain constraints** | 15 | Arrears bucket, DPD midpoints, performing_status, flag enums, EPC labels, legal maturity values, LTV ranges, original/current balance bounds, NHG implication, NHG balance cap. |
| **D — Static-field coherence** | 7 | origination_year / original_balance / province / NHG flag / legal_maturity / IO flag / scheduled_monthly_payment must be identical for the same loan_id across cutoffs. |
| **E — Temporal consistency** | 3 | Reporting date strictly increases, seasoning increments by +1, remaining_term decrements by 1. |
| **F — Lifecycle properties** | 11 | Performing & amortising loans show monotonic balance ↓; non-Performing balances frozen between consecutive months; Redeemed/Charged-Off rows have balance = 0; Performing rows have arrears_amount = 0 and DPD = 0; cures reset arrears to 0; arrears_amount accrues exactly +1 scheduled payment per non-Performing month; IO Performing balance constant; default_crr_flag and foreclosure_flag align with state. |
| **G — Pool integrity** | 2 | No new loan_id after first cutoff (closed pool); once Redeemed or Charged-Off, the loan does not reappear. |
| **H — Distribution sanity** | 3 | First cutoff is ≥ 90% Non-defaulted; final cutoff has some Defaulted loans; some attrition occurred. |

## Output sample

```
  #  STATUS  TEST                                                       VIOLATIONS
-----------------------------------------------------------------------------------
  1  ✓ PASS  A01 panel has rows                                                  0
  2  ✓ PASS  A02 expected 24 distinct cutoffs                                    0
  ...
 38  ✓ PASS  F01 Performing & amortising: balance ≤ previous (monotonic ↓)       0
 39  ✓ PASS  F02 Any DPD/Defaulted active: balance frozen vs previous            0
  ...

SECTION   TOTAL   PASS   FAIL
-----------------------------
A             4      4      0
B             8      8      0
C            15     15      0
D             7      7      0
E             3      3      0
F            11     11      0
G             2      2      0
H             3      3      0

TOTAL: 53 tests, 53 passed, 0 failed, 0 total violations
```

## Extending the suite

Add new tests by appending a `UNION ALL SELECT` row inside the `tests` CTE
in `tests/test_panel.sql`. Each row needs three columns: a unique `test_id`,
a `test_name` prefixed with the section letter (so the summary groups
correctly), and a sub-query that returns the count of violations.
A passing test returns 0.

```sql
UNION ALL SELECT 54,
        'X01 my new test',
        (SELECT COUNT(*) FROM panel WHERE …)
```

## Porting to other databases

`read_csv_auto(getvariable('cutoff_glob'))` is DuckDB-specific. For other
engines, load the CSVs into a real table first:

```sql
-- BigQuery
LOAD DATA INTO `proj.dataset.panel`
FROM FILES (format = 'CSV', uris = ['gs://bucket/cutoffs/green_lion_*.csv']);

-- Snowflake
COPY INTO panel FROM @stage/cutoffs PATTERN = 'green_lion_.*\.csv'
    FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE);

-- Postgres
\copy panel FROM PROGRAM 'cat ./out/cutoffs/green_lion_*.csv | tail -n +2' CSV
```

Then drop the `read_csv_auto(...)` line from `test_panel.sql` and reference
the loaded `panel` table directly. Everything else is portable SQL.
