"""Compare this engine against upstream deeploans, side by side.

The RMBS pack claims to be a lossless re-expression of upstream's hardcoded
Python. This script tests that claim against the real thing rather than against
a copy of its constants.

Being straight about what parity can mean here: NeMo Data Designer and numpy do
not share a random number stream, so **byte-identical output is not achievable**
and is not claimed. Three bars are enforced instead:

1. **Schema — exact.** Same columns, same order, same filenames.
2. **Behaviour — exact.** Both panels satisfy the same invariants, and the
   surviving pool tracks upstream within 3% at every cut-off.
3. **Distribution — within tolerance.** Joint structure and dynamics inside
   their noise floors, and at most `--max-column-failures` marginals outside
   theirs.

Why that last bar is not zero, having been measured rather than assumed: at
30,000 loans a handful of columns sit 1.2x to 2x over their floors, and they
are not the same columns from run to run. Chased to the bottom, `nhg_flag` —
the most persistent — traces to the underlying Bernoulli draw landing 1.7
standard errors apart on the opening book, which is ordinary sampling noise.
What makes it *look* worse is the panel: a loan that survives 24 cut-offs
contributes 24 rows and one that redeems early contributes two, so any
difference in the opening book is re-weighted by differential survival, and the
per-column noise floor does not model that amplification. Two engines drawing
from different random streams will always show a few such columns. Demanding
zero would mean tuning thresholds until this particular seed passed, which
proves nothing.

Usage::

    pip install data-designer
    python scripts/parity_check.py --deeploans /path/to/deeploans -n 20000

Exits non-zero if any bar is missed.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from sdd import api  # noqa: E402
from sdd.validate import compare, validate_panel  # noqa: E402

PACK = REPO / "packs" / "rmbs_nl_green_lion.yaml"


def run_upstream(deeploans: Path, n: int, seed: int, out_dir: Path) -> pd.DataFrame:
    """Run upstream's own generator, unmodified, in its own directory."""
    module_dir = deeploans / "synthetic-data-designer"
    if not (module_dir / "data_designer_loan_book.py").exists():
        raise SystemExit(f"no upstream generator found under {module_dir}")

    sys.path.insert(0, str(module_dir))
    from age_to_panel import run_ageing as upstream_age  # type: ignore
    from data_designer_loan_book import (
        HYPOPORT_COLUMNS,  # type: ignore
        generate_loan_book,  # type: ignore
    )

    book_path = out_dir / "loan_book.parquet"
    book = generate_loan_book(n, str(book_path), first_cutoff="2024-01-31", deal_year=2024)
    cutoffs = out_dir / "cutoffs"
    upstream_age(book, n_cutoffs=24, first_cutoff="2024-01-31", out_dir=str(cutoffs), seed=seed)

    return read_panel(sorted(cutoffs.glob("*.csv")))[HYPOPORT_COLUMNS]


def read_panel(paths: list[Path]) -> pd.DataFrame:
    """Read per-period CSVs the same way for both sides.

    Both engines write CSV, so both are read from CSV — comparing one side's
    parquet against the other side's CSV is not a like-for-like test. It also
    matters that `guarantee_type` holds the literal string "None" for loans
    without a guarantee, which pandas parses as NaN by default; read one side
    that way and the other from parquet and the comparison reports a 63%
    distribution gap that does not exist.

    `keep_default_na=False` preserves those strings, and the numeric columns are
    then coerced back explicitly, because switching NA detection off also
    switches off numeric inference.
    """
    frames = [pd.read_csv(path, keep_default_na=False, na_values=[]) for path in paths]
    panel = pd.concat(frames, ignore_index=True)
    for column in panel.columns:
        if panel[column].dtype == object:
            converted = pd.to_numeric(panel[column], errors="coerce")
            if converted.notna().all():
                panel[column] = converted
    return panel


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--deeploans", type=Path, required=True, help="Path to a deeploans checkout."
    )
    parser.add_argument("-n", "--num-records", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keep", action="store_true", help="Do not delete the working directory.")
    parser.add_argument(
        "--max-column-failures",
        type=int,
        default=4,
        help="How many marginals may sit outside their noise floor. See the module docstring.",
    )
    args = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="sdd-parity-"))
    failures: list[str] = []
    try:
        print(f"working directory: {workdir}\n")

        print(f"=== upstream deeploans: {args.num_records:,} loans x 24 cut-offs ===")
        upstream = run_upstream(args.deeploans, args.num_records, args.seed, workdir / "upstream")

        print(f"\n=== this engine: {args.num_records:,} loans x 24 cut-offs ===")
        result = api.run(
            PACK, args.num_records, workdir / "ours", seed=args.seed, validate_output=True
        )
        ours = read_panel(sorted(Path(f) for f in result["files"]))

        # -- 1. schema, exactly -------------------------------------------
        print("\n--- 1. schema ---")
        if list(upstream.columns) == list(ours.columns):
            print(f"  PASS  {len(ours.columns)} columns, identical names and order")
        else:
            failures.append("schema differs")
            only_up = [c for c in upstream.columns if c not in ours.columns]
            only_ours = [c for c in ours.columns if c not in upstream.columns]
            print(f"  FAIL  upstream-only {only_up}\n        ours-only {only_ours}")

        upstream_files = sorted(p.name for p in (workdir / "upstream" / "cutoffs").glob("*.csv"))
        our_files = sorted(Path(f).name for f in result["files"])
        if upstream_files == our_files:
            print(f"  PASS  {len(our_files)} filenames match the Hypoport convention")
        else:
            failures.append("filenames differ")
            print(f"  FAIL  {our_files[:2]} vs {upstream_files[:2]}")

        # -- 2. distributions ---------------------------------------------
        print("\n--- 2. distributions ---")
        fidelity = compare(
            upstream,
            ours,
            id_column="loan_id",
            time_column="reporting_date",
            state_column="arrears_bucket",
        )
        for line in fidelity.summary().splitlines():
            print(f"  {line}")
        if len(fidelity.failures) > args.max_column_failures:
            failures.append(
                f"{len(fidelity.failures)} column(s) outside tolerance, "
                f"more than the {args.max_column_failures} allowed"
            )
        elif fidelity.failures:
            print(
                f"  NOTE  {len(fidelity.failures)} column(s) marginally over their floor; "
                "within the allowance for two independent random streams"
            )
        if (
            fidelity.correlation_delta is not None
            and fidelity.correlation_delta > fidelity.correlation_threshold
        ):
            failures.append("joint structure differs beyond its noise floor")
        if (
            fidelity.transition_delta is not None
            and fidelity.transition_delta > fidelity.transition_threshold
        ):
            failures.append("lifecycle dynamics differ beyond their noise floor")

        # -- 3. behaviour ---------------------------------------------------
        print("\n--- 3. behaviour ---")
        spec = api.load(PACK)
        for label, panel in (("upstream", upstream), ("ours", ours)):
            report = validate_panel(spec, panel)
            mark = "PASS" if report.passed else "FAIL"
            print(f"  {mark}  {label}: {report.summary().splitlines()[0]}")
            if not report.passed:
                for check in report.failures:
                    print(f"          {check.name}: {check.violations:,} row(s)")
                # Upstream failing our checks is a finding about the checks or
                # about upstream, not necessarily about this engine — report it
                # either way rather than quietly passing.
                failures.append(f"{label} panel failed its invariants")

        print("\n  period-by-period surviving pool:")
        up_counts = upstream.groupby("reporting_date").size()
        our_counts = ours.groupby("reporting_date").size()
        worst = 0.0
        for date in up_counts.index:
            a, b = up_counts[date], our_counts.get(date, 0)
            drift = abs(a - b) / a
            worst = max(worst, drift)
        print(f"    final pool  upstream {up_counts.iloc[-1]:,}  ours {our_counts.iloc[-1]:,}")
        print(f"    worst period-on-period drift  {worst:.2%}")
        if worst > 0.03:
            failures.append(f"pool size drifts {worst:.2%} from upstream")

        print("\n" + "=" * 60)
        if failures:
            print(f"PARITY NOT MET — {len(failures)} issue(s):")
            for problem in failures:
                print(f"  - {problem}")
            return 1
        print("PARITY MET on schema, distributions, and behaviour.")
        return 0
    finally:
        if args.keep:
            print(f"\nkept {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
