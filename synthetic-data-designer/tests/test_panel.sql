-- ============================================================================
-- Synthetic Dutch RMBS panel — SQL test suite
-- ============================================================================
-- Tests the 71-column Hypoport-parity output for: schema integrity, domain
-- constraints, lifecycle correctness, cross-cutoff coherence, and pool
-- integrity (closed-pool semantics).
--
-- Designed for DuckDB (because it can read CSV/parquet directly via glob).
-- Most queries are portable; BigQuery / Snowflake / Postgres will need:
--   - replace `read_csv_auto` glob with a CREATE TABLE AS SELECT step
--   - replace `FILTER (WHERE …)` with `SUM(CASE WHEN … THEN 1 ELSE 0 END)`
--   - replace `LAG(...) OVER (...)` is supported by all of the above
--
-- Run:
--   duckdb -c "
--     SET VARIABLE cutoff_glob = './out/cutoffs/green_lion_*.csv';
--     .read tests/test_panel.sql
--   "
--   -- or:
--   python tests/run_sql_tests.py --cutoff-dir ./out/cutoffs
--
-- A row in `test_results` is a passing test iff `violations = 0`.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Load the panel
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE panel AS
SELECT *,
       CAST(reporting_date AS DATE) AS report_dt
FROM read_csv_auto(getvariable('cutoff_glob'), header=true);

-- Minimal lagged table — only the columns the lifecycle / temporal tests
-- require.  Static-field coherence (Section D) uses GROUP BY HAVING
-- COUNT(DISTINCT ...) > 1, which doesn't need LAG and is faster.
CREATE OR REPLACE TABLE panel_lagged AS
SELECT
    loan_id, report_dt,
    current_balance, arrears_amount, performing_status, arrears_bucket,
    seasoning_months, remaining_term_months,
    interest_only_flag, scheduled_monthly_payment, days_past_due,
    LAG(current_balance)        OVER w AS prev_balance,
    LAG(arrears_amount)         OVER w AS prev_arrears,
    LAG(performing_status)      OVER w AS prev_status,
    LAG(arrears_bucket)         OVER w AS prev_bucket,
    LAG(seasoning_months)       OVER w AS prev_seasoning,
    LAG(remaining_term_months)  OVER w AS prev_remaining_term,
    LAG(report_dt)              OVER w AS prev_report_dt
FROM panel
WINDOW w AS (PARTITION BY loan_id ORDER BY report_dt);

-- ----------------------------------------------------------------------------
-- 2. Test suite — every row should have violations = 0
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE test_results AS
WITH tests AS (

----- SECTION A: SCHEMA & STRUCTURE -----

SELECT  1 AS test_id,
        'A01 panel has rows' AS test_name,
        (SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM panel) AS violations
UNION ALL SELECT  2,
        'A02 expected 24 distinct cutoffs',
        (SELECT CASE WHEN COUNT(DISTINCT report_dt) != 24 THEN 1 ELSE 0 END FROM panel)
UNION ALL SELECT  3,
        'A03 71-column Hypoport schema',
        (SELECT CASE WHEN COUNT(*) != 71 THEN 1 ELSE 0 END
         FROM information_schema.columns
         WHERE LOWER(table_name) = 'panel' AND column_name <> 'report_dt')
UNION ALL SELECT  4,
        'A04 loan_id is unique within a cutoff',
        (SELECT COUNT(*) FROM (SELECT report_dt, loan_id, COUNT(*) c
                               FROM panel GROUP BY 1,2 HAVING c > 1))

----- SECTION B: DEAL-LEVEL CONSTANTS -----

UNION ALL SELECT  5,
        'B01 currency = EUR for every row',
        (SELECT COUNT(*) FROM panel WHERE currency <> 'EUR')
UNION ALL SELECT  6,
        'B02 country = NL for every row',
        (SELECT COUNT(*) FROM panel WHERE country <> 'NL')
UNION ALL SELECT  7,
        'B03 originator_name = ING',
        (SELECT COUNT(*) FROM panel WHERE originator_name <> 'ING')
UNION ALL SELECT  8,
        'B04 servicer_name = ING',
        (SELECT COUNT(*) FROM panel WHERE servicer_name <> 'ING')
UNION ALL SELECT  9,
        'B05 closing_date single value across panel',
        (SELECT CASE WHEN COUNT(DISTINCT closing_date) != 1 THEN 1 ELSE 0 END FROM panel)
UNION ALL SELECT 10,
        'B06 transaction_name single value across panel',
        (SELECT CASE WHEN COUNT(DISTINCT transaction_name) != 1 THEN 1 ELSE 0 END FROM panel)
UNION ALL SELECT 11,
        'B07 esma_transaction_identifier single value',
        (SELECT CASE WHEN COUNT(DISTINCT esma_transaction_identifier) != 1 THEN 1 ELSE 0 END FROM panel)
UNION ALL SELECT 12,
        'B08 closing_date is ISO 8601 (YYYY-MM-DD)',
        (SELECT COUNT(*) FROM panel WHERE NOT regexp_matches(CAST(closing_date AS VARCHAR), '^\d{4}-\d{2}-\d{2}$'))

----- SECTION C: DOMAIN CONSTRAINTS -----

UNION ALL SELECT 13,
        'C01 arrears_bucket ∈ allowed set',
        (SELECT COUNT(*) FROM panel WHERE arrears_bucket NOT IN
            ('Performing','1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD',
             'Defaulted','Charged-Off','Redeemed'))
UNION ALL SELECT 14,
        'C02 days_past_due ∈ bucket midpoints',
        (SELECT COUNT(*) FROM panel WHERE days_past_due NOT IN (0,15,45,75,120,200))
UNION ALL SELECT 15,
        'C03 performing_status ∈ allowed set',
        (SELECT COUNT(*) FROM panel WHERE performing_status NOT IN
            ('Non-defaulted','Defaulted','Charged-Off','Redeemed'))
UNION ALL SELECT 16,
        'C04 nhg_flag ∈ {Y,N}',
        (SELECT COUNT(*) FROM panel WHERE nhg_flag NOT IN ('Y','N'))
UNION ALL SELECT 17,
        'C05 interest_only_flag ∈ {Y,N}',
        (SELECT COUNT(*) FROM panel WHERE interest_only_flag NOT IN ('Y','N'))
UNION ALL SELECT 18,
        'C06 epc_label ∈ Dutch EPC labels',
        (SELECT COUNT(*) FROM panel WHERE epc_label NOT IN
            ('A+++','A++','A+','A','B','C','D','E','F','G'))
UNION ALL SELECT 19,
        'C07 legal_maturity_months ∈ {240,300,330,360}',
        (SELECT COUNT(*) FROM panel WHERE legal_maturity_months NOT IN (240,300,330,360))
UNION ALL SELECT 20,
        'C08 oltomv_original between 0 and 200',
        (SELECT COUNT(*) FROM panel WHERE oltomv_original < 0 OR oltomv_original > 200)
UNION ALL SELECT 21,
        'C09 cltomv_current between 0 and 200',
        (SELECT COUNT(*) FROM panel WHERE cltomv_current < 0 OR cltomv_current > 200)
UNION ALL SELECT 22,
        'C10 cltimv_current between 0 and 200',
        (SELECT COUNT(*) FROM panel WHERE cltimv_current < 0 OR cltimv_current > 200)
UNION ALL SELECT 23,
        'C11 original_balance in plausible range',
        (SELECT COUNT(*) FROM panel WHERE original_balance < 500 OR original_balance > 5000000)
UNION ALL SELECT 24,
        'C12 current_balance ≤ original_balance',
        -- Allow tiny float slack
        (SELECT COUNT(*) FROM panel WHERE current_balance > original_balance + 1.0)
UNION ALL SELECT 25,
        'C13 nhg_flag=Y implies guarantee_type=NHG',
        (SELECT COUNT(*) FROM panel WHERE nhg_flag = 'Y' AND guarantee_type <> 'NHG')
UNION ALL SELECT 26,
        'C14 nhg_flag=Y implies original_balance ≤ NHG cap (€435k, 2024)',
        (SELECT COUNT(*) FROM panel WHERE nhg_flag = 'Y' AND original_balance > 435000)
UNION ALL SELECT 27,
        'C15 country, currency, originator are mutually consistent',
        (SELECT COUNT(*) FROM panel
            WHERE NOT (country = 'NL' AND currency = 'EUR' AND originator_name = 'ING'))

----- SECTION D: STATIC-FIELD COHERENCE (same loan_id across cutoffs) -----

UNION ALL SELECT 28,
        'D01 origination_year identical per loan_id across cutoffs',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT origination_year) > 1))
UNION ALL SELECT 29,
        'D02 original_balance identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT original_balance) > 1))
UNION ALL SELECT 30,
        'D03 province identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT province) > 1))
UNION ALL SELECT 31,
        'D04 nhg_flag identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT nhg_flag) > 1))
UNION ALL SELECT 32,
        'D05 legal_maturity_months identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT legal_maturity_months) > 1))
UNION ALL SELECT 33,
        'D06 interest_only_flag identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING COUNT(DISTINCT interest_only_flag) > 1))
UNION ALL SELECT 34,
        'D07 scheduled_monthly_payment identical per loan_id',
        (SELECT COUNT(*) FROM (SELECT loan_id FROM panel GROUP BY loan_id
                               HAVING (MAX(scheduled_monthly_payment) - MIN(scheduled_monthly_payment)) > 0.01))

----- SECTION E: TEMPORAL CONSISTENCY (consecutive cutoffs of same loan) -----

UNION ALL SELECT 35,
        'E01 reporting_date strictly increases per loan_id',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_report_dt IS NOT NULL AND report_dt <= prev_report_dt)
UNION ALL SELECT 36,
        'E02 seasoning_months increments by +1 between consecutive cutoffs',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_seasoning IS NOT NULL
              AND (seasoning_months - prev_seasoning) <> 1)
UNION ALL SELECT 37,
        'E03 remaining_term_months decrements by 1 between consecutive cutoffs',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_remaining_term IS NOT NULL
              AND (prev_remaining_term - remaining_term_months) <> 1
              AND remaining_term_months > 0)        -- floor case allowed

----- SECTION F: LIFECYCLE PROPERTIES -----

UNION ALL SELECT 38,
        'F01 Performing & amortising: balance ≤ previous (monotonic ↓)',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_balance IS NOT NULL
              AND arrears_bucket = 'Performing'
              AND prev_bucket    = 'Performing'
              AND interest_only_flag = 'N'
              AND current_balance > prev_balance + 0.01)
UNION ALL SELECT 39,
        'F02 Any DPD/Defaulted active: balance frozen vs previous',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_balance IS NOT NULL
              AND arrears_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted')
              AND prev_bucket    IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted')
              AND ABS(current_balance - prev_balance) > 0.01)
UNION ALL SELECT 40,
        'F03 Redeemed rows have current_balance = 0',
        (SELECT COUNT(*) FROM panel
            WHERE performing_status = 'Redeemed' AND ABS(current_balance) > 0.01)
UNION ALL SELECT 41,
        'F04 Charged-Off rows have current_balance = 0',
        (SELECT COUNT(*) FROM panel
            WHERE performing_status = 'Charged-Off' AND ABS(current_balance) > 0.01)
UNION ALL SELECT 42,
        'F05 Performing rows have arrears_amount = 0',
        (SELECT COUNT(*) FROM panel
            WHERE arrears_bucket = 'Performing' AND ABS(arrears_amount) > 0.01)
UNION ALL SELECT 43,
        'F06 Performing rows have days_past_due = 0',
        (SELECT COUNT(*) FROM panel
            WHERE arrears_bucket = 'Performing' AND days_past_due <> 0)
UNION ALL SELECT 44,
        'F07 Cure: prev non-Performing AND current Performing → arrears_amount = 0',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted')
              AND arrears_bucket = 'Performing'
              AND arrears_amount <> 0)
UNION ALL SELECT 45,
        'F08 arrears_amount accrues +1 scheduled_monthly_payment per non-Perf month',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted')
              AND arrears_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted')
              AND ABS((arrears_amount - prev_arrears) - scheduled_monthly_payment) > 0.5)
UNION ALL SELECT 46,
        'F09 IO Performing: balance constant between consecutive Performing cutoffs',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_balance IS NOT NULL
              AND interest_only_flag = 'Y'
              AND arrears_bucket = 'Performing' AND prev_bucket = 'Performing'
              AND ABS(current_balance - prev_balance) > 0.01)
UNION ALL SELECT 47,
        'F10 Defaulted active loans have foreclosure_flag = Y',
        (SELECT COUNT(*) FROM panel
            WHERE performing_status = 'Defaulted' AND foreclosure_flag <> 'Y')
UNION ALL SELECT 48,
        'F11 default_crr_flag = Y when arrears_bucket in (90+ DPD, Defaulted, Charged-Off)',
        (SELECT COUNT(*) FROM panel
            WHERE arrears_bucket IN ('90+ DPD','Defaulted','Charged-Off') AND default_crr_flag <> 'Y')

----- SECTION G: POOL INTEGRITY (closed pool — no new loans, terminal stays terminal) -----

UNION ALL SELECT 49,
        'G01 no new loan_id appears after first cutoff (closed pool)',
        (SELECT COUNT(*) FROM (
            SELECT loan_id FROM panel
            GROUP BY loan_id
            HAVING MIN(report_dt) > (SELECT MIN(report_dt) FROM panel)
        ))
UNION ALL SELECT 50,
        'G02 once Redeemed or Charged-Off, loan_id does not reappear',
        (SELECT COUNT(*) FROM panel_lagged
            WHERE prev_status IN ('Redeemed','Charged-Off'))

----- SECTION H: DISTRIBUTION SANITY (informative, not strict) -----

UNION ALL SELECT 51,
        'H01 first cutoff has >= 90% Non-defaulted (origination snapshot)',
        (SELECT CASE WHEN
            (SUM(CASE WHEN performing_status = 'Non-defaulted' THEN 1 ELSE 0 END) * 1.0
              / NULLIF(COUNT(*),0)) < 0.90 THEN 1 ELSE 0 END
         FROM panel WHERE report_dt = (SELECT MIN(report_dt) FROM panel))
UNION ALL SELECT 52,
        'H02 final cutoff has any Defaulted loans (ageing produced defaults)',
        (SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
         FROM panel
         WHERE report_dt = (SELECT MAX(report_dt) FROM panel) AND performing_status = 'Defaulted')
UNION ALL SELECT 53,
        'H03 cumulative attrition (Redeemed + Charged-Off) > 0',
        (SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
         FROM panel
         WHERE performing_status IN ('Redeemed','Charged-Off'))

)
SELECT
    test_id,
    test_name,
    violations,
    CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    -- group by section letter (first char of test_name)
    SUBSTR(test_name, 1, 1) AS section
FROM tests
ORDER BY test_id;

-- ----------------------------------------------------------------------------
-- 3. Show results
-- ----------------------------------------------------------------------------
SELECT * FROM test_results ORDER BY test_id;

SELECT
    section,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
FROM test_results
GROUP BY section
ORDER BY section;

SELECT
    COUNT(*)                                                        AS total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)                AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)                AS failed,
    SUM(violations)                                                 AS total_violations,
    CASE WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) = 0
         THEN 'ALL PASS' ELSE 'FAILURES' END                        AS overall
FROM test_results;
