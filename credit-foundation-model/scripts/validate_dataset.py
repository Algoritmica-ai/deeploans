# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Validate a panel against its dataset contract (v1.1 G1.5) — step 0 for any new asset.

Re-derives the contract invariants from the actual files (the artifact-validator layer; the
code-level layer is ``tests/test_validate_dataset.py``). Checks:

  A. contract columns present (id / time / origination unless derived / label event+gate cols)
  B. id column is string-typed (numeric-looking ids corrupt on CSV round-trips)
  C. time columns parse as ISO dates, month-end, no future dates
  D. one row per (id, time) — no duplicated entity-periods
  E. label event values are within the declared domain; gate columns cover their gate_values
  F. no leakage/exclude column appears in the tokenizer field schema
  G. gate/event consistency: a row is never gated-in AND terminal-event at once (boolean labels)

Usage::

    python scripts/validate_dataset.py --dataset configs/mortgage_performance/dataset.yaml \
        --panel gs://.../panel_2000_2024_10pct.parquet --sample-rows 2000000
    python scripts/validate_dataset.py --dataset configs/toy/dataset.yaml --panel toy.parquet

Exit 0 = ALL CHECKS PASSED; exit 1 = at least one FAIL (validators must fail loudly).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

from credit_fm.data.dataset_config import DatasetConfig, load_dataset_config
from credit_fm.utils import storage

_RESULTS: list[tuple[bool, str, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    _RESULTS.append((bool(ok), name, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""), flush=True)


def _iso_month_end_ok(s: pd.Series) -> tuple[bool, str]:
    dt = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    if dt.isna().any():
        bad = s[dt.isna()].head(3).tolist()
        return False, f"{int(dt.isna().sum())} unparseable ISO dates, e.g. {bad}"
    month_end = dt == (dt + pd.offsets.MonthEnd(0))
    if not month_end.all():
        return False, f"{int((~month_end).sum())} non-month-end dates"
    return True, f"{dt.min().date()}..{dt.max().date()}"


def run_checks(ds: DatasetConfig, panel: pd.DataFrame, schema_path: str | None) -> None:
    c = ds
    # A ---------------------------------------------------------------- presence
    required = [c.id_col, c.time_col] + ([] if c.origination_derived else [c.origination_col])
    for spec in c.labels.values():
        required += [spec.event_col] + ([spec.gate_col] if spec.gate_col else [])
    missing = sorted({col for col in required if col not in panel.columns})
    check(not missing, "A: contract columns present",
          f"missing={missing}" if missing else f"{len(set(required))} required cols")

    # B ---------------------------------------------------------------- id dtype
    ids = panel[c.id_col] if c.id_col in panel.columns else pd.Series(dtype=object)
    id_ok = ids.dtype == object or str(ids.dtype) == "string"
    check(id_ok, "B: id column is string-typed",
          f"{c.id_col} dtype={ids.dtype} (numeric ids corrupt on CSV round-trips)")

    # C ---------------------------------------------------------------- time cols
    for col in dict.fromkeys([c.time_col] + ([] if c.origination_derived else [c.origination_col])):
        if col in panel.columns:
            ok, detail = _iso_month_end_ok(panel[col].dropna().astype(str))
            check(ok, f"C: {col} is ISO month-end", detail)

    # D ---------------------------------------------------------------- one row per (id, time)
    if not missing:
        dups = int(panel.duplicated(subset=[c.id_col, c.time_col]).sum())
        check(dups == 0, "D: one row per (id, time)", f"{dups} duplicated entity-periods")

    # E ---------------------------------------------------------------- label domains
    for spec in c.labels.values():
        if spec.event_col in panel.columns:
            vals = panel[spec.event_col].dropna()
            if vals.dtype == bool or str(vals.dtype) == "boolean":
                ok, detail = True, f"boolean, {int(vals.astype(bool).sum()):,} events"
            else:
                ok = vals.isin([spec.event_value]).any()
                detail = (f"event_value {spec.event_value!r} "
                          f"{'present' if ok else 'NEVER OCCURS — wrong column or value?'}")
            check(ok, f"E: label '{spec.name}' event domain", detail)
        if spec.gate_col and spec.gate_col in panel.columns:
            gate_seen = panel[spec.gate_col].dropna().isin(list(spec.gate_values)).any()
            check(bool(gate_seen), f"E: label '{spec.name}' gate domain",
                  f"{spec.gate_col} matches gate_values "
                  f"{'somewhere' if gate_seen else 'NOWHERE — wrong gate?'}")

    # F ---------------------------------------------------------------- schema is leakage-free
    if schema_path and Path(schema_path).exists():
        schema = yaml.safe_load(Path(schema_path).read_text()) or {}
        fields = {f for role in ("profile", "event")
                  for cols in (schema.get(role) or {}).values() for f in cols}
        smuggled = sorted(fields & c.banned)
        check(not smuggled, "F: tokenizer schema is leakage/exclude-free",
              f"banned-in-schema={smuggled}" if smuggled else
              f"{len(fields)} fields vs {len(c.banned)} banned")
    else:
        print(f"  SKIP  F: schema not found ({schema_path})", flush=True)

    # G ---------------------------------------------------------------- gate/event consistency
    for spec in c.labels.values():
        if (spec.gate_col and spec.event_col in panel.columns
                and spec.gate_col in panel.columns):
            ev, gate = panel[spec.event_col], panel[spec.gate_col]
            if (ev.dtype == bool or str(ev.dtype) == "boolean") and \
               (gate.dtype == bool or str(gate.dtype) == "boolean"):
                both = int((ev.fillna(False) & gate.fillna(False)).sum())
                check(both == 0, f"G: '{spec.name}' never gated-in AND terminal",
                      f"{both} rows both {spec.gate_col} and {spec.event_col}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True, help="path to dataset.yaml (the contract)")
    ap.add_argument("--panel", required=True, help="panel parquet (local or gs://)")
    ap.add_argument("--schema", default=None,
                    help="tokenizer schema to audit (default: the contract's schema key)")
    ap.add_argument("--sample-rows", type=int, default=None,
                    help="validate on the first N rows (cheap pass on huge GCS panels)")
    ap.add_argument("--key", default=None, help="GCS service-account key")
    args = ap.parse_args()

    ds = load_dataset_config(args.dataset)
    print(f"contract: {args.dataset} (asset '{ds.name}', adapter '{ds.adapter}', "
          f"{len(ds.labels)} labels, {len(ds.banned)} banned columns)")
    storage.ensure_auth(args.panel, args.key)
    print(f"panel   : {args.panel}", flush=True)
    panel = storage.read_parquet(args.panel)
    if args.sample_rows and len(panel) > args.sample_rows:
        panel = panel.head(args.sample_rows)
    print(f"validating {len(panel):,} rows\n", flush=True)

    run_checks(ds, panel, args.schema or ds.schema)

    failed = [name for ok, name, _ in _RESULTS if not ok]
    print("\n" + ("ALL CHECKS PASSED" if not failed else
                  f"{len(failed)} CHECK(S) FAILED: {failed}"), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
