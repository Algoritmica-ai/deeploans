"""
Run tests/test_panel.sql against a directory of cutoff CSVs and pretty-print
results.

Usage:
    python tests/run_sql_tests.py --cutoff-dir ./out/cutoffs
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--cutoff-dir", required=True,
                   help="Directory containing green_lion_*_synthetic_loan_tape.csv files")
    p.add_argument("--sql", default=str(Path(__file__).parent / "test_panel.sql"))
    p.add_argument("--fail-on-violation", action="store_true",
                   help="Exit with code 1 if any test fails.")
    args = p.parse_args()

    cutoff_glob = os.path.join(args.cutoff_dir, "green_lion_*.csv")
    sql_text = Path(args.sql).read_text()

    con = duckdb.connect(":memory:")
    con.execute(f"SET VARIABLE cutoff_glob = '{cutoff_glob}'")
    # Strip line comments first, then split on `;` and run each statement
    # (DuckDB's python execute() takes a single statement at a time).
    no_comments = "\n".join(
        ln for ln in sql_text.splitlines() if not ln.lstrip().startswith("--")
    )
    statements = [s.strip() for s in no_comments.split(";") if s.strip()]
    for stmt in statements:
        con.execute(stmt)

    rows = con.execute("SELECT test_id, status, test_name, violations FROM test_results ORDER BY test_id").fetchall()
    print(f"{'#':>3}  {'STATUS':<6}  {'TEST':<70}  {'VIOLATIONS':>10}")
    print("-" * 95)
    for tid, status, name, viol in rows:
        marker = "✓" if status == "PASS" else "✗"
        print(f"{tid:>3}  {marker} {status:<4}  {name:<70}  {viol:>10}")

    print()
    section_rows = con.execute("""
        SELECT section, COUNT(*) total,
               SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) passed,
               SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) failed
        FROM test_results GROUP BY section ORDER BY section
    """).fetchall()
    print(f"{'SECTION':<10} {'TOTAL':>6} {'PASS':>6} {'FAIL':>6}")
    print("-" * 35)
    for sec, total, passed, failed in section_rows:
        print(f"{sec:<10} {total:>6} {passed:>6} {failed:>6}")

    summary = con.execute("""
        SELECT COUNT(*), SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END), SUM(violations)
        FROM test_results
    """).fetchone()
    total, passed, failed, violations = summary
    print()
    print(f"TOTAL: {total} tests, {passed} passed, {failed} failed, {violations} total violations")

    if args.fail_on_violation and failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
