"""
End-to-end orchestrator: generate the loan book via NeMo Data Designer, then
age it for 24 monthly cutoffs and write per-cutoff CSVs + a consolidated
parquet.

Usage:
    # Smoke test
    python run.py --num-records 5000

    # Full hackathon run
    python run.py --num-records 500000 --out-dir ./out_full
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import pandas as pd

from data_designer_loan_book import HYPOPORT_COLUMNS, generate_loan_book
from age_to_panel import run_ageing


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-records", type=int, default=500_000)
    p.add_argument("--n-cutoffs", type=int, default=24)
    p.add_argument("--first-cutoff", default="2024-01-31")
    p.add_argument("--deal-year", type=int, default=None,
                   help="Deal closing year for transaction_name / closing_date / "
                        "esma_transaction_identifier (default: first-cutoff year).")
    p.add_argument("--out-dir", default="./out")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--skip-consolidated", action="store_true",
                   help="Do not concatenate per-cutoff CSVs into a single parquet.")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cutoff_dir = out_dir / "cutoffs"
    book_path  = out_dir / "loan_book.parquet"

    t0 = time.time()
    print(f"\n=== Phase 1: NeMo Data Designer loan book ({args.num_records:,} records) ===")
    book = generate_loan_book(
        args.num_records, str(book_path),
        first_cutoff=args.first_cutoff, deal_year=args.deal_year,
    )
    print(f"    done in {time.time()-t0:.1f}s")

    t1 = time.time()
    print(f"\n=== Phase 2: ageing {args.n_cutoffs} cutoffs ===")
    run_ageing(book, n_cutoffs=args.n_cutoffs, first_cutoff=args.first_cutoff,
               out_dir=str(cutoff_dir), seed=args.seed)
    print(f"    done in {time.time()-t1:.1f}s")

    if not args.skip_consolidated:
        print(f"\n=== Phase 3: consolidating to parquet ===")
        frames = []
        for f in sorted(os.listdir(cutoff_dir)):
            if f.endswith(".csv"):
                frames.append(pd.read_csv(cutoff_dir / f))
        all_df = pd.concat(frames, ignore_index=True)
        all_df = all_df[HYPOPORT_COLUMNS]
        consolidated = out_dir / "all_cutoffs.parquet"
        all_df.to_parquet(consolidated, index=False)
        print(f"    wrote {len(all_df):,} loan-month rows -> {consolidated}")

    print(f"\n=== Done in {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    main()
