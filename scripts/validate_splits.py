# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Validate a prepared train/val/test split against the split invariants — read-only audit.

Proves the *produced split* is correct (not just the code): the three split outputs (single
parquets, or the G3.2 streamed ``<split>/bucket-*/`` directories — auto-detected) +
``splits.csv`` + ``splits.meta.json`` obey the loan-stratified temporal contract —

  A) train / val / test loan-sets are **disjoint** (the core leakage guard);
  B) each loan's whole history is in exactly one split (implied by A + C at loan level);
  C) **completeness** — the split membership matches ``splits.csv`` and the per-split loan counts
     match ``splits.meta.json``;
  D) **temporal order** — train originated <= val originated <= test originated (recomputed from the
     parquet when an explicit origination column exists, else from the manifest's ranges);
  E) manifest agreement — counts and origination ranges in ``splits.meta.json`` match the files;
  F) ``reporting_max`` (if it was set) is respected — no row past the cap in any split.

    python scripts/validate_splits.py --dir gs://.../output/processed/fannie_mae/run_2000_2024
    python scripts/validate_splits.py --dir data/processed
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from pathlib import Path

import pandas as pd

SPLITS = ("train", "val", "test")


def _read(path: str, columns=None) -> pd.DataFrame:
    # storage.read_parquet handles single files AND directories (the G3.2 bucketed split layout),
    # local or gs:// (this Arrow build has no native GCS; it reads via gcsfs)
    from credit_fm.utils import storage
    return storage.read_parquet(path, columns=columns)


def _split_source(d: str, s: str) -> str:
    """Where split ``s`` lives: ``<d>/<s>.parquet`` (v1.0) or the ``<d>/<s>/`` dir (G3.2 streamed)."""
    from credit_fm.utils import storage
    single = _join(d, f"{s}.parquet")
    return single if storage.exists(single) else _join(d, s)


def _read_text(path: str) -> str:
    if path.startswith("gs://"):
        import gcsfs
        with gcsfs.GCSFileSystem().open(path[len("gs://"):], "r") as f:
            return f.read()
    return Path(path).read_text()


def _join(d: str, name: str) -> str:
    return f"{d.rstrip('/')}/{name}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", required=True, help="split output dir (local or gs://) with the 3 "
                    "parquets + splits.csv + splits.meta.json")
    ap.add_argument("--key", default="/workspace/.gcloud/credit-fm-sa.json")
    args = ap.parse_args()
    if args.dir.startswith("gs://") and Path(args.key).exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.key)

    meta = json.loads(_read_text(_join(args.dir, "splits.meta.json")))
    id_col = meta["id_col"]
    orig_key = meta.get("origination_key", "")
    reporting_col = (meta.get("config") or {}).get("reporting_col")
    reporting_max = (meta.get("config") or {}).get("reporting_max")
    explicit_orig = orig_key if (orig_key and not orig_key.startswith("derived(")) else None

    results: list[tuple[bool, str, str]] = []

    def chk(name, cond, detail=""):
        results.append((bool(cond), name, detail))

    # read only the columns we need from each split parquet
    cols = [id_col] + [c for c in {explicit_orig, reporting_col} if c]
    frames = {}
    for s in SPLITS:
        frames[s] = _read(_split_source(args.dir, s), columns=cols)
    # normalise ids to str everywhere — Fannie loan_ids are numeric-looking, and CSV round-trips
    # coerce them to int, which would spuriously mismatch the parquet's string ids
    loans = {s: set(frames[s][id_col].astype(str).unique()) for s in SPLITS}
    chk("all three split parquets non-empty", all(len(frames[s]) for s in SPLITS),
        " ".join(f"{s}={len(frames[s]):,}rows/{len(loans[s]):,}loans" for s in SPLITS))

    # A) disjoint loan-sets — the leakage guard
    tv, tt, vt = loans["train"] & loans["val"], loans["train"] & loans["test"], loans["val"] & loans["test"]
    chk("A: train/val/test loan-sets are disjoint", not (tv or tt or vt),
        f"overlaps train∩val={len(tv)} train∩test={len(tt)} val∩test={len(vt)}")

    # C) completeness vs splits.csv (force id as str so numeric loan_ids don't parse to int)
    csv = pd.read_csv(io.StringIO(_read_text(_join(args.dir, "splits.csv"))), dtype={id_col: str})
    csv_by_split = {s: set(csv.loc[csv["split"] == s, id_col]) for s in SPLITS}
    chk("C: parquet membership matches splits.csv",
        all(loans[s] == csv_by_split[s] for s in SPLITS),
        " ".join(f"{s}:{'ok' if loans[s]==csv_by_split[s] else 'DIFF'}" for s in SPLITS))
    all_loans = set().union(*loans.values())
    chk("C: no loan lost or duplicated across splits (partition of csv)",
        all_loans == set(csv[id_col]) and len(all_loans) == sum(len(loans[s]) for s in SPLITS))

    # E) counts match the manifest
    chk("E: per-split loan counts match splits.meta.json",
        all(len(loans[s]) == meta["n_loans"][s] for s in SPLITS),
        " ".join(f"{s}:{len(loans[s])}vs{meta['n_loans'][s]}" for s in SPLITS))

    # D) temporal order — recompute per-split origination if an explicit column exists
    if explicit_orig:
        rng = {}
        for s in SPLITS:
            o = pd.to_datetime(frames[s].groupby(id_col)[explicit_orig].min())
            rng[s] = (o.min(), o.max())
        chk("D: train orig <= val orig <= test orig (recomputed)",
            rng["train"][1] <= rng["val"][0] and rng["val"][1] <= rng["test"][0],
            " ".join(f"{s}[{rng[s][0].date()}..{rng[s][1].date()}]" for s in SPLITS))
        # E) recomputed ranges agree with the manifest's reported ranges
        chk("E: origination ranges match splits.meta.json",
            all(str(rng[s][0].date()) == meta["origination_range"][s][0]
                and str(rng[s][1].date()) == meta["origination_range"][s][1] for s in SPLITS))
    else:
        mr = meta["origination_range"]
        chk("D: train orig <= val orig <= test orig (from manifest)",
            mr["train"][1] <= mr["val"][0] and mr["val"][1] <= mr["test"][0],
            " ".join(f"{s}{mr[s]}" for s in SPLITS))

    # F) reporting_max respected
    if reporting_max and reporting_col:
        cap = pd.to_datetime(str(reporting_max))
        worst = max(pd.to_datetime(frames[s][reporting_col], errors="coerce").max() for s in SPLITS)
        chk(f"F: no row past reporting_max {reporting_max}", worst <= cap, f"latest row = {worst}")

    ok = all(r[0] for r in results)
    for passed, name, detail in results:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
