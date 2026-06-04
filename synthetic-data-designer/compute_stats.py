"""
Compute portfolio statistics from a multi-cutoff RMBS panel parquet and write
a markdown report.

Usage:
    python compute_stats.py \
        --parquet /Users/.../out_500k/all_cutoffs.parquet \
        --first-cutoff 2024-01-31 \
        --last-cutoff  2025-12-31 \
        --out PORTFOLIO_STATS.md
"""

from __future__ import annotations

import argparse
import time

import duckdb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--parquet", required=True)
    p.add_argument("--first-cutoff", default="2024-01-31")
    p.add_argument("--last-cutoff", default="2025-12-31")
    p.add_argument("--out", required=True)
    p.add_argument("--memory-limit", default="2GB")
    args = p.parse_args()

    t0 = time.time()
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")

    # Use a view: DuckDB reads parquet lazily, keeping RAM bounded.
    con.execute(f"""
        CREATE VIEW panel AS
        SELECT *, CAST(reporting_date AS DATE) AS report_dt
        FROM read_parquet('{args.parquet}')
    """)

    first = args.first_cutoff
    last = args.last_cutoff

    out_lines: list[str] = []
    w = out_lines.append

    # --- Section 1: Pool overview ---
    w("# Synthetic Dutch RMBS Portfolio — Statistics")
    w("")
    n_rows = con.execute("SELECT COUNT(*) FROM panel").fetchone()[0]
    n_loans = con.execute("SELECT COUNT(DISTINCT loan_id) FROM panel").fetchone()[0]
    n_cuts = con.execute("SELECT COUNT(DISTINCT report_dt) FROM panel").fetchone()[0]
    minmax = con.execute("SELECT MIN(report_dt), MAX(report_dt) FROM panel").fetchone()
    w(f"Computed against the full panel of **{n_rows:,}** loan-month observations across **{n_loans:,}** distinct loan IDs and **{n_cuts}** monthly cutoffs ({minmax[0]} to {minmax[1]}). All monetary figures are in EUR. Snapshot statistics use the first cutoff ({first}) unless stated otherwise.")
    w("")
    w("## 1. Pool overview")
    w("")
    sums = con.execute(f"""
        SELECT
            SUM(original_balance)  FILTER (WHERE report_dt = DATE '{first}') / 1e9,
            SUM(current_balance)   FILTER (WHERE report_dt = DATE '{first}') / 1e9,
            SUM(current_balance)   FILTER (WHERE report_dt = DATE '{last}')  / 1e9,
            SUM(original_market_value_at_origination) FILTER (WHERE report_dt = DATE '{first}') / 1e9
        FROM panel
    """).fetchone()
    w(f"- Aggregate original balance at origination:    **€{sums[0]:.2f} B**")
    w(f"- Aggregate current balance at first cutoff:    **€{sums[1]:.2f} B**")
    w(f"- Aggregate current balance at last cutoff:     **€{sums[2]:.2f} B**")
    w(f"- Aggregate original market value at origination: **€{sums[3]:.2f} B**")
    w("")

    # --- Section 2: Monthly composition ---
    w("## 2. Monthly pool composition")
    w("")
    w("| Cutoff | Active loans | Performing | Defaulted | Redeemed | Charged-Off | Δ vs prior |")
    w("|---|---:|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        SELECT report_dt,
               COUNT(*) AS active,
               SUM(CASE WHEN arrears_bucket = 'Performing' THEN 1 ELSE 0 END) AS performing,
               SUM(CASE WHEN performing_status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted,
               SUM(CASE WHEN performing_status = 'Redeemed' THEN 1 ELSE 0 END) AS redeemed,
               SUM(CASE WHEN performing_status = 'Charged-Off' THEN 1 ELSE 0 END) AS chargedoff
        FROM panel GROUP BY 1 ORDER BY 1
    """).fetchall()
    prev = None
    for dt, a, pf, d, r, co in rs:
        delta = f"{a - prev:+,}" if prev is not None else "—"
        w(f"| {dt} | {a:,} | {pf:,} | {d:,} | {r:,} | {co:,} | {delta} |")
        prev = a
    w("")

    # Helpers to format snapshot tables
    def snap_count(col_expr: str, order_clause: str = "ORDER BY 2 DESC") -> list[tuple]:
        return con.execute(f"""
            SELECT {col_expr}, COUNT(*),
                   100.0 * COUNT(*) / (SELECT COUNT(*) FROM panel WHERE report_dt = DATE '{first}') AS pct
            FROM panel WHERE report_dt = DATE '{first}'
            GROUP BY 1 {order_clause}
        """).fetchall()

    def quantile_table(col: str) -> list[tuple]:
        r = con.execute(f"""
            SELECT
                QUANTILE_CONT({col}, 0.05),
                QUANTILE_CONT({col}, 0.25),
                QUANTILE_CONT({col}, 0.50),
                QUANTILE_CONT({col}, 0.75),
                QUANTILE_CONT({col}, 0.95),
                AVG({col})
            FROM panel WHERE report_dt = DATE '{first}'
        """).fetchone()
        return list(zip(["p5", "p25", "p50 (median)", "p75", "p95", "mean"], r))

    # --- Section 3: Borrower demographics ---
    w("## 3. Borrower demographics (origination snapshot)")
    w("")
    w("### 3.1 Province distribution")
    w("")
    w("| Province | Loans | Share |")
    w("|---|---:|---:|")
    for p_, n_, pct in snap_count("province"):
        w(f"| {p_} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 3.2 Employment status")
    w("")
    w("| Status | Loans | Share |")
    w("|---|---:|---:|")
    for s_, n_, pct in snap_count("employment_status"):
        w(f"| {s_} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 3.3 Borrower annual income (EUR)")
    w("")
    w("| Quantile | Income |")
    w("|---|---:|")
    for q, v in quantile_table("borrower_annual_income"):
        w(f"| {q} | €{v:,.0f} |")
    w("")

    # --- Section 4: Property characteristics ---
    w("## 4. Property characteristics")
    w("")
    w("### 4.1 Property type")
    w("")
    w("| Type | Loans | Share |")
    w("|---|---:|---:|")
    for p_, n_, pct in snap_count("property_type"):
        w(f"| {p_} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 4.2 Construction year buckets")
    w("")
    w("| Bucket | Loans | Share |")
    w("|---|---:|---:|")
    for b, n_, pct in snap_count("construction_year_bucket", "ORDER BY 1"):
        w(f"| {b} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 4.3 Occupancy")
    w("")
    w("| Status | Loans | Share |")
    w("|---|---:|---:|")
    for s, n_, pct in snap_count("occupancy"):
        w(f"| {s} | {n_:,} | {pct:.2f}% |")
    w("")

    # --- Section 5: Loan economics ---
    w("## 5. Loan economics (origination snapshot)")
    w("")
    w("### 5.1 Original balance (EUR)")
    w("")
    w("| Quantile | Amount |")
    w("|---|---:|")
    for q, v in quantile_table("original_balance"):
        w(f"| {q} | €{v:,.0f} |")
    w("")
    w("### 5.2 Balance buckets")
    w("")
    w("| Bucket | Loans | Share |")
    w("|---|---:|---:|")
    for b, n_, pct in snap_count("balance_bucket", "ORDER BY 1"):
        w(f"| {b} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 5.3 Repayment type")
    w("")
    w("| Type | Loans | Share |")
    w("|---|---:|---:|")
    for s, n_, pct in snap_count("repayment_type"):
        w(f"| {s} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 5.4 Rate type")
    w("")
    w("| Type | Loans | Share |")
    w("|---|---:|---:|")
    for s, n_, pct in snap_count("rate_type"):
        w(f"| {s} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 5.5 Interest rate (%)")
    w("")
    w("| Quantile | Rate |")
    w("|---|---:|")
    for q, v in quantile_table("current_interest_rate_pct"):
        w(f"| {q} | {v:.2f}% |")
    w("")
    w("### 5.6 Legal maturity (months)")
    w("")
    w("| Term | Loans | Share |")
    w("|---|---:|---:|")
    for t, n_, pct in snap_count("legal_maturity_months", "ORDER BY 1"):
        w(f"| {t} | {n_:,} | {pct:.2f}% |")
    w("")

    # --- Section 6: LTV & affordability ---
    w("## 6. LTV and affordability (origination snapshot)")
    w("")
    w("### 6.1 OLTV bucket")
    w("")
    w("| Bucket | Loans | Share |")
    w("|---|---:|---:|")
    for b, n_, pct in snap_count("oltomv_original_bucket", "ORDER BY 1"):
        w(f"| {b} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 6.2 Loan-to-income (LTI) bucket")
    w("")
    w("| Bucket | Loans | Share |")
    w("|---|---:|---:|")
    for b, n_, pct in snap_count("loan_to_income_bucket", "ORDER BY 1"):
        w(f"| {b} | {n_:,} | {pct:.2f}% |")
    w("")
    w("### 6.3 Payment-due-to-income (DSTI) bucket")
    w("")
    w("| Bucket | Loans | Share |")
    w("|---|---:|---:|")
    for b, n_, pct in snap_count("payment_due_to_income_pct_bucket", "ORDER BY 1"):
        w(f"| {b} | {n_:,} | {pct:.2f}% |")
    w("")

    # --- Section 7: Risk markers ---
    w("## 7. Risk markers (origination snapshot)")
    w("")
    rm = con.execute(f"""
        SELECT
            SUM(CASE WHEN nhg_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS nhg_share,
            SUM(CASE WHEN buy_to_let_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS btl_share,
            SUM(CASE WHEN self_employed_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS se_share,
            SUM(CASE WHEN interest_only_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS io_share,
            SUM(CASE WHEN oltomv_original > 90 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS high_oltv,
            SUM(CASE WHEN loan_to_income > 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS high_lti,
            SUM(CASE WHEN forbearance_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS forb,
            SUM(CASE WHEN restructuring_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS restr,
            SUM(CASE WHEN construction_deposit_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cd
        FROM panel WHERE report_dt = DATE '{first}'
    """).fetchone()
    w("| Risk marker | Share |")
    w("|---|---:|")
    w(f"| NHG-insured                       | {rm[0]:.2f}% |")
    w(f"| Buy-to-let                        | {rm[1]:.2f}% |")
    w(f"| Self-employed borrower            | {rm[2]:.2f}% |")
    w(f"| Interest-only or bullet           | {rm[3]:.2f}% |")
    w(f"| High OLTV (>90%)                  | {rm[4]:.2f}% |")
    w(f"| High LTI (>5.0)                   | {rm[5]:.2f}% |")
    w(f"| Forbearance flag set              | {rm[6]:.2f}% |")
    w(f"| Restructuring flag set            | {rm[7]:.2f}% |")
    w(f"| Construction deposit (bouwdepot)  | {rm[8]:.2f}% |")
    w("")

    # --- Section 8: Performance trajectory ---
    w("## 8. Performance trajectory — arrears-bucket share by cutoff")
    w("")
    w("| Cutoff | Performing | 1-29 DPD | 30-59 DPD | 60-89 DPD | 90+ DPD | Defaulted | Redeemed | Charged-Off |")
    w("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        WITH dist AS (
            SELECT report_dt, arrears_bucket, COUNT(*) AS n
            FROM panel GROUP BY 1, 2
        )
        SELECT report_dt,
            SUM(CASE WHEN arrears_bucket = 'Performing'  THEN n ELSE 0 END) AS perf,
            SUM(CASE WHEN arrears_bucket = '1-29 DPD'    THEN n ELSE 0 END) AS d129,
            SUM(CASE WHEN arrears_bucket = '30-59 DPD'   THEN n ELSE 0 END) AS d3059,
            SUM(CASE WHEN arrears_bucket = '60-89 DPD'   THEN n ELSE 0 END) AS d6089,
            SUM(CASE WHEN arrears_bucket = '90+ DPD'     THEN n ELSE 0 END) AS d90,
            SUM(CASE WHEN arrears_bucket = 'Defaulted'   THEN n ELSE 0 END) AS def,
            SUM(CASE WHEN arrears_bucket = 'Redeemed'    THEN n ELSE 0 END) AS red,
            SUM(CASE WHEN arrears_bucket = 'Charged-Off' THEN n ELSE 0 END) AS co,
            SUM(n) AS total
        FROM dist GROUP BY 1 ORDER BY 1
    """).fetchall()
    for row in rs:
        dt, perf, d129, d3059, d6089, d90, defa, red, co, tot = row
        fmt = lambda x: f"{100 * x / tot:5.2f}%"
        w(f"| {dt} | {fmt(perf)} | {fmt(d129)} | {fmt(d3059)} | {fmt(d6089)} | {fmt(d90)} | {fmt(defa)} | {fmt(red)} | {fmt(co)} |")
    w("")

    # --- Section 9: Vintage ---
    w("## 9. Vintage analysis (by origination year)")
    w("")
    w("| Origination year | Loans | Share | Mean original balance | Mean OLTV |")
    w("|---|---:|---:|---:|---:|")
    rs = con.execute(f"""
        SELECT origination_year, COUNT(*),
               100.0 * COUNT(*) / (SELECT COUNT(*) FROM panel WHERE report_dt = DATE '{first}'),
               AVG(original_balance), AVG(oltomv_original)
        FROM panel WHERE report_dt = DATE '{first}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for y, n_, pct, bal, oltv in rs:
        w(f"| {y} | {n_:,} | {pct:.2f}% | €{bal:,.0f} | {oltv:.1f}% |")
    w("")

    # --- Section 10: EPC ---
    w("## 10. Energy efficiency (EPC labels, origination snapshot)")
    w("")
    w("| EPC label | Loans | Share | Mean energy demand (kWh/m²/yr) |")
    w("|---|---:|---:|---:|")
    rs = con.execute(f"""
        SELECT epc_label, COUNT(*),
               100.0 * COUNT(*) / (SELECT COUNT(*) FROM panel WHERE report_dt = DATE '{first}'),
               AVG(primary_energy_demand_kwh_m2)
        FROM panel WHERE report_dt = DATE '{first}'
        GROUP BY 1
        ORDER BY CASE epc_label
            WHEN 'A+++' THEN 1 WHEN 'A++' THEN 2 WHEN 'A+' THEN 3 WHEN 'A' THEN 4
            WHEN 'B' THEN 5 WHEN 'C' THEN 6 WHEN 'D' THEN 7 WHEN 'E' THEN 8
            WHEN 'F' THEN 9 WHEN 'G' THEN 10 END
    """).fetchall()
    for lbl, n_, pct, dem in rs:
        w(f"| {lbl} | {n_:,} | {pct:.2f}% | {dem:.0f} |")
    w("")

    # --- Section 11: Terminal-exit accounting ---
    w("## 11. Terminal-exit accounting (over the full 24 months)")
    w("")
    counts = con.execute(f"""
        SELECT
            (SELECT COUNT(DISTINCT loan_id) FROM panel WHERE report_dt = DATE '{first}'),
            (SELECT COUNT(DISTINCT loan_id) FROM panel WHERE report_dt = DATE '{last}' AND performing_status = 'Non-defaulted'),
            (SELECT COUNT(DISTINCT loan_id) FROM panel WHERE report_dt = DATE '{last}' AND performing_status = 'Defaulted'),
            (SELECT COUNT(DISTINCT loan_id) FROM panel WHERE performing_status = 'Redeemed'),
            (SELECT COUNT(DISTINCT loan_id) FROM panel WHERE performing_status = 'Charged-Off')
    """).fetchone()
    start, sp, sd, er, eco = counts
    w(f"- Starting pool (month 0):       **{start:,}**")
    w(f"- Still performing at month 23:  **{sp:,}** ({100 * sp / start:.2f}%)")
    w(f"- Still defaulted at month 23:   **{sd:,}** ({100 * sd / start:.2f}%)")
    w(f"- Ever Redeemed (terminal):      **{er:,}** ({100 * er / start:.2f}%)")
    w(f"- Ever Charged-Off (terminal):   **{eco:,}** ({100 * eco / start:.2f}%)")
    total = sp + sd + er + eco
    w(f"- **Total accounted for:**        **{total:,}** ({100 * total / start:.2f}%)")
    w("")

    # --- Section 12: Headline rates ---
    w("## 12. Headline cumulative rates over 24 months")
    w("")
    w("| Metric | Value |")
    w("|---|---:|")
    w(f"| Cumulative prepayment rate            | **{100 * er / start:.2f}%** |")
    w(f"| Cumulative charge-off rate            | **{100 * eco / start:.2f}%** |")
    w(f"| End-of-period default rate            | **{100 * sd / start:.2f}%** |")
    w(f"| Total pool attrition (Red + CO)       | **{100 * (er + eco) / start:.2f}%** |")
    peak_def = con.execute("""
        SELECT MAX(d) FROM (
            SELECT 100.0 * SUM(CASE WHEN arrears_bucket = 'Defaulted' THEN 1 ELSE 0 END) / COUNT(*) AS d
            FROM panel GROUP BY report_dt)
    """).fetchone()[0]
    w(f"| Peak Defaulted share in active pool   | **{peak_def:.2f}%** |")
    w("")

    # === RISK ANALYTICS — added in extension =================================

    # --- Section 13: Delinquency rates over time ---
    w("## 13. Delinquency rates over time")
    w("")
    w("All percentages are shares of the *active* pool (excluding terminal Redeemed and Charged-Off).")
    w("")
    w("| Cutoff | Active loans | Total delinquent | 30+ DPD | 90+ DPD | Defaulted | Foreclosure |")
    w("|---|---:|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        WITH active AS (
            SELECT * FROM panel
            WHERE arrears_bucket NOT IN ('Redeemed','Charged-Off')
        )
        SELECT report_dt,
            COUNT(*) AS n_active,
            100.0 * SUM(CASE WHEN arrears_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD','Defaulted') THEN 1 ELSE 0 END) / COUNT(*) AS total_delq,
            100.0 * SUM(CASE WHEN arrears_bucket IN ('30-59 DPD','60-89 DPD','90+ DPD','Defaulted') THEN 1 ELSE 0 END) / COUNT(*) AS d30,
            100.0 * SUM(CASE WHEN arrears_bucket IN ('90+ DPD','Defaulted') THEN 1 ELSE 0 END) / COUNT(*) AS d90,
            100.0 * SUM(CASE WHEN arrears_bucket = 'Defaulted' THEN 1 ELSE 0 END) / COUNT(*) AS def,
            100.0 * SUM(CASE WHEN foreclosure_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS fcl
        FROM active GROUP BY 1 ORDER BY 1
    """).fetchall()
    for dt, n, td, d30, d90, df, fcl in rs:
        w(f"| {dt} | {n:,} | {td:.2f}% | {d30:.2f}% | {d90:.2f}% | {df:.2f}% | {fcl:.2f}% |")
    w("")

    # --- Section 14: Compliance and regulatory metrics over time ---
    w("## 14. Compliance and regulatory metrics over time")
    w("")
    w("Tracks underwriting and servicing policy compliance across the 24 cutoffs. NHG cap and high-LTV columns flag violations of NL prime conventions.")
    w("")
    w("| Cutoff | NHG share | NHG cap violations (>€435k) | LTV >100% (underwater) | Forbearance | Restructuring | Default flag (Y) |")
    w("|---|---:|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        WITH active AS (
            SELECT * FROM panel
            WHERE arrears_bucket NOT IN ('Redeemed','Charged-Off')
        )
        SELECT report_dt,
            100.0 * SUM(CASE WHEN nhg_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS nhg,
            SUM(CASE WHEN nhg_flag = 'Y' AND original_balance > 435000 THEN 1 ELSE 0 END) AS nhg_viol,
            100.0 * SUM(CASE WHEN cltomv_current > 100 THEN 1 ELSE 0 END) / COUNT(*) AS uw,
            100.0 * SUM(CASE WHEN forbearance_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS forb,
            100.0 * SUM(CASE WHEN restructuring_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS restr,
            100.0 * SUM(CASE WHEN default_crr_flag = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS defflag
        FROM active GROUP BY 1 ORDER BY 1
    """).fetchall()
    for dt, nhg, viol, uw, forb, restr, df in rs:
        w(f"| {dt} | {nhg:.2f}% | {viol:,} | {uw:.2f}% | {forb:.2f}% | {restr:.2f}% | {df:.2f}% |")
    w("")

    # --- Section 15: Pool dynamics over time ---
    w("## 15. Pool dynamics over time")
    w("")
    w("Aggregate balance metrics across active loans, weighted by current balance where applicable.")
    w("")
    w("| Cutoff | Aggregate balance (€ B) | Pool factor | WA rate | WA seasoning (mo) | WA remaining term (mo) |")
    w("|---|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        WITH active AS (
            SELECT * FROM panel
            WHERE arrears_bucket NOT IN ('Redeemed','Charged-Off')
        )
        SELECT report_dt,
            SUM(current_balance) / 1e9 AS agg_bal,
            SUM(current_balance) / NULLIF(SUM(original_balance), 0) AS pool_factor,
            SUM(current_balance * current_interest_rate_pct) / NULLIF(SUM(current_balance), 0) AS war,
            SUM(current_balance * seasoning_months) / NULLIF(SUM(current_balance), 0) AS was,
            SUM(current_balance * remaining_term_months) / NULLIF(SUM(current_balance), 0) AS wart
        FROM active GROUP BY 1 ORDER BY 1
    """).fetchall()
    for dt, ab, pf, war, was, wart in rs:
        w(f"| {dt} | €{ab:,.2f} | {pf:.4f} | {war:.2f}% | {was:.1f} | {wart:.1f} |")
    w("")

    # --- Section 16: Roll rates and state transitions ---
    w("## 16. Roll rates — month-on-month state transitions")
    w("")
    w("Per-cutoff conditional probability of transitioning to a worse state, given the prior cutoff's state, plus aggregate cure rate from any DPD bucket back to Performing.")
    w("")
    w("| Cutoff | P → 1-29 | 1-29 → 30-59 | 30-59 → 60-89 | 60-89 → 90+ | 90+ → Default | Cure rate | New charge-offs |")
    w("|---|---:|---:|---:|---:|---:|---:|---:|")
    rs = con.execute("""
        WITH lagged AS (
            SELECT report_dt, loan_id, arrears_bucket,
                   LAG(arrears_bucket) OVER (PARTITION BY loan_id ORDER BY report_dt) AS prev_bucket
            FROM panel
        )
        SELECT report_dt,
            100.0 * SUM(CASE WHEN prev_bucket = 'Performing' AND arrears_bucket = '1-29 DPD'   THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket = 'Performing' THEN 1 ELSE 0 END), 0)             AS p_to_1_29,
            100.0 * SUM(CASE WHEN prev_bucket = '1-29 DPD'   AND arrears_bucket = '30-59 DPD'  THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket = '1-29 DPD' THEN 1 ELSE 0 END), 0)               AS d129_to_d3059,
            100.0 * SUM(CASE WHEN prev_bucket = '30-59 DPD'  AND arrears_bucket = '60-89 DPD'  THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket = '30-59 DPD' THEN 1 ELSE 0 END), 0)              AS d3059_to_d6089,
            100.0 * SUM(CASE WHEN prev_bucket = '60-89 DPD'  AND arrears_bucket = '90+ DPD'    THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket = '60-89 DPD' THEN 1 ELSE 0 END), 0)              AS d6089_to_d90,
            100.0 * SUM(CASE WHEN prev_bucket = '90+ DPD'    AND arrears_bucket = 'Defaulted'  THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket = '90+ DPD' THEN 1 ELSE 0 END), 0)                AS d90_to_def,
            100.0 * SUM(CASE WHEN prev_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD')
                              AND arrears_bucket = 'Performing'                                THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN prev_bucket IN ('1-29 DPD','30-59 DPD','60-89 DPD','90+ DPD') THEN 1 ELSE 0 END), 0) AS cure_rate,
            SUM(CASE WHEN arrears_bucket = 'Charged-Off' AND prev_bucket = 'Defaulted' THEN 1 ELSE 0 END) AS new_co
        FROM lagged WHERE prev_bucket IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for dt, p1, d1, d2, d3, d4, cure, co in rs:
        w(f"| {dt} | {p1 or 0:.3f}% | {d1 or 0:.2f}% | {d2 or 0:.2f}% | {d3 or 0:.2f}% | {d4 or 0:.2f}% | {cure or 0:.2f}% | {co or 0:,} |")
    w("")

    # --- Section 17: Vintage performance ---
    w("## 17. Vintage performance — cumulative outcomes by origination year")
    w("")
    w("Per-vintage cohort: total loans originated, share that ever defaulted, charged off, or prepaid over the 24-month observation window.")
    w("")
    w("| Origination year | Cohort size | Ever defaulted | Ever charged off | Ever redeemed |")
    w("|---|---:|---:|---:|---:|")
    rs = con.execute(f"""
        WITH base AS (
            SELECT origination_year, loan_id, performing_status FROM panel
        ),
        agg AS (
            SELECT origination_year,
                COUNT(DISTINCT loan_id) AS cohort,
                COUNT(DISTINCT CASE WHEN performing_status = 'Defaulted'   THEN loan_id END) AS ever_def,
                COUNT(DISTINCT CASE WHEN performing_status = 'Charged-Off' THEN loan_id END) AS ever_co,
                COUNT(DISTINCT CASE WHEN performing_status = 'Redeemed'    THEN loan_id END) AS ever_red
            FROM base GROUP BY 1
        )
        SELECT origination_year, cohort,
               100.0 * ever_def / cohort, 100.0 * ever_co / cohort, 100.0 * ever_red / cohort
        FROM agg ORDER BY 1
    """).fetchall()
    for y, n, ed, ec, er in rs:
        w(f"| {y} | {n:,} | {ed:.2f}% | {ec:.2f}% | {er:.2f}% |")
    w("")

    # --- Section 18: Geographic and vintage concentration risk (HHI) ---
    w("## 18. Concentration risk")
    w("")
    w("Herfindahl-Hirschman Index (HHI) — sum of squared shares. Higher values indicate higher concentration. Industry rule of thumb: HHI < 1500 = unconcentrated, 1500-2500 = moderately concentrated, > 2500 = highly concentrated.")
    w("")
    hhi_geo = con.execute(f"""
        WITH agg AS (
            SELECT province, COUNT(*) AS n FROM panel WHERE report_dt = DATE '{first}' GROUP BY 1
        ),
        tot AS (SELECT SUM(n) AS T FROM agg)
        SELECT SUM(POWER(100.0 * n / T, 2)) FROM agg, tot
    """).fetchone()[0]
    hhi_vintage = con.execute(f"""
        WITH agg AS (
            SELECT origination_year, COUNT(*) AS n FROM panel WHERE report_dt = DATE '{first}' GROUP BY 1
        ),
        tot AS (SELECT SUM(n) AS T FROM agg)
        SELECT SUM(POWER(100.0 * n / T, 2)) FROM agg, tot
    """).fetchone()[0]
    hhi_nuts3 = con.execute(f"""
        WITH agg AS (
            SELECT economic_region_nuts3, COUNT(*) AS n FROM panel WHERE report_dt = DATE '{first}' GROUP BY 1
        ),
        tot AS (SELECT SUM(n) AS T FROM agg)
        SELECT SUM(POWER(100.0 * n / T, 2)) FROM agg, tot
    """).fetchone()[0]
    w("| Dimension | HHI | Interpretation |")
    w("|---|---:|---|")
    w(f"| Province (NUTS-2) | {hhi_geo:,.0f} | {_classify_hhi(hhi_geo)} |")
    w(f"| NUTS-3 region     | {hhi_nuts3:,.0f} | {_classify_hhi(hhi_nuts3)} |")
    w(f"| Vintage (year)    | {hhi_vintage:,.0f} | {_classify_hhi(hhi_vintage)} |")
    w("")
    w("### 18.1 Top 10 NUTS-3 regions by exposure (origination snapshot)")
    w("")
    w("| NUTS-3 | Loans | Aggregate balance (€ M) | Share of pool |")
    w("|---|---:|---:|---:|")
    rs = con.execute(f"""
        SELECT economic_region_nuts3, COUNT(*) AS n, SUM(current_balance) / 1e6 AS bal,
               100.0 * COUNT(*) / (SELECT COUNT(*) FROM panel WHERE report_dt = DATE '{first}') AS pct
        FROM panel WHERE report_dt = DATE '{first}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    for r, n, bal, pct in rs:
        w(f"| {r} | {n:,} | €{bal:,.1f} | {pct:.2f}% |")
    w("")
    w("### 18.2 Single-borrower concentration (origination snapshot)")
    w("")
    top10 = con.execute(f"""
        SELECT SUM(current_balance) FROM (
            SELECT current_balance FROM panel
            WHERE report_dt = DATE '{first}'
            ORDER BY current_balance DESC LIMIT 10)
    """).fetchone()[0]
    top100 = con.execute(f"""
        SELECT SUM(current_balance) FROM (
            SELECT current_balance FROM panel
            WHERE report_dt = DATE '{first}'
            ORDER BY current_balance DESC LIMIT 100)
    """).fetchone()[0]
    top1pct = con.execute(f"""
        WITH ranked AS (
            SELECT current_balance, ROW_NUMBER() OVER (ORDER BY current_balance DESC) AS rn
            FROM panel WHERE report_dt = DATE '{first}'
        )
        SELECT SUM(current_balance) FROM ranked WHERE rn <= (SELECT COUNT(*) FROM panel WHERE report_dt = DATE '{first}') * 0.01
    """).fetchone()[0]
    pool_total = sums[1] * 1e9  # current_balance at first cutoff
    w("| Group | Aggregate balance | % of pool |")
    w("|---|---:|---:|")
    w(f"| Top 10 loans   | €{top10/1e6:,.1f} M | {100*top10/pool_total:.3f}% |")
    w(f"| Top 100 loans  | €{top100/1e6:,.1f} M | {100*top100/pool_total:.3f}% |")
    w(f"| Top 1% (5,000) | €{top1pct/1e9:,.2f} B | {100*top1pct/pool_total:.2f}% |")
    w("")

    # --- Section 19: Loss-given-default proxy ---
    w("## 19. Loss-given-default proxy")
    w("")
    w("For each Charged-Off loan we take the maximum balance observed while it was Defaulted (i.e. the principal at the start of the foreclosure timeline) as the gross loss-at-default. Aggregate this across all ever-charged-off loans for portfolio loss exposure. NHG coverage on charged-off loans is also broken out — in real NL prime, NHG would absorb most of these losses.")
    w("")
    lgd = con.execute("""
        WITH co_loans AS (
            SELECT DISTINCT loan_id, nhg_flag FROM panel WHERE performing_status = 'Charged-Off'
        ),
        balances AS (
            SELECT p.loan_id, p.nhg_flag,
                   MAX(CASE WHEN p.performing_status = 'Defaulted' THEN p.current_balance END) AS bal_at_def,
                   MAX(p.original_balance) AS orig
            FROM panel p JOIN co_loans c USING (loan_id)
            GROUP BY 1, 2
        )
        SELECT
            COUNT(*) AS n_co,
            SUM(bal_at_def) / 1e6 AS agg_loss_m,
            AVG(bal_at_def) AS avg_loss,
            AVG(bal_at_def / orig) * 100 AS avg_lgd_pct,
            SUM(CASE WHEN nhg_flag = 'Y' THEN bal_at_def ELSE 0 END) / 1e6 AS nhg_covered_m,
            SUM(CASE WHEN nhg_flag = 'Y' THEN 1 ELSE 0 END) AS n_nhg
        FROM balances
    """).fetchone()
    n_co, agg_m, avg_loss, avg_lgd, nhg_m, n_nhg = lgd
    w("| Metric | Value |")
    w("|---|---:|")
    w(f"| Charged-Off loans (total)             | {n_co:,} |")
    w(f"| Aggregate balance at default          | **€{agg_m:,.1f} M** |")
    w(f"| Mean balance at default               | €{avg_loss:,.0f} |")
    w(f"| Mean LGD ratio (bal-at-default ÷ original) | {avg_lgd:.1f}% |")
    w(f"| NHG-covered share of charge-offs       | {100*n_nhg/n_co:.2f}% |")
    w(f"| NHG-covered loss exposure              | **€{nhg_m:,.1f} M** |")
    w(f"| Net loss exposure (non-NHG)            | **€{agg_m - nhg_m:,.1f} M** |")
    w("")

    w(f"<!-- Generated in {time.time() - t0:.1f}s -->")
    w("")

    with open(args.out, "w") as f:
        f.write("\n".join(out_lines))
    print(f"Wrote {args.out} ({len(out_lines)} lines) in {time.time() - t0:.1f}s")


def _classify_hhi(value: float) -> str:
    if value < 1500:
        return "Unconcentrated"
    if value < 2500:
        return "Moderately concentrated"
    return "Highly concentrated"


if __name__ == "__main__":
    main()
