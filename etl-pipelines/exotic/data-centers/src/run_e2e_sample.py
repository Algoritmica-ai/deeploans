#!/usr/bin/env python3
"""Runnable end-to-end sample: raw CSV -> normalized JSON + validation report."""

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, List, Tuple

from data_center_etl_pipeline.pipeline import run_etl
from data_center_etl_pipeline.io import read_csv

REQUIRED_FIELDS = [
    "facility_id",
    "sponsor",
    "country",
    "report_date",
    "noi_eur",
    "dscr",
    "ltv_pct",
    "occupancy_pct",
    "energy_cost_ratio_pct",
    "junior_note_watch_status",
]


def _validate_rows(rows: List[Dict[str, object]]) -> Tuple[bool, Dict[str, object]]:
    missing_required = []
    for idx, row in enumerate(rows):
        missing = [field for field in REQUIRED_FIELDS if field not in row or row[field] in ("", None)]
        if missing:
            missing_required.append({"row_index": idx, "missing_fields": missing})

    allowed_status = {"ok", "watch", "breach"}
    bad_status_rows = [
        {"row_index": idx, "status": row.get("junior_note_watch_status")}
        for idx, row in enumerate(rows)
        if row.get("junior_note_watch_status") not in allowed_status
    ]

    report = {
        "is_valid": not missing_required and not bad_status_rows,
        "row_count": len(rows),
        "required_fields": REQUIRED_FIELDS,
        "missing_required": missing_required,
        "invalid_status_rows": bad_status_rows,
    }
    return report["is_valid"], report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run end-to-end data-center ETL sample")
    parser.add_argument("--input", default="sample_data/raw_facilities.csv", help="Raw input CSV path")
    parser.add_argument("--output", default="output", help="Output root path")
    args = parser.parse_args()

    paths = run_etl(args.input, args.output)
    silver_rows = read_csv(paths.silver)

    normalized_json_path = os.path.join(args.output, "normalized", "facility_normalized.json")
    validation_report_path = os.path.join(args.output, "reports", "validation_report.json")
    os.makedirs(os.path.dirname(normalized_json_path), exist_ok=True)
    os.makedirs(os.path.dirname(validation_report_path), exist_ok=True)

    with open(normalized_json_path, "w", encoding="utf-8") as f:
        json.dump(silver_rows, f, indent=2)

    is_valid, report = _validate_rows(silver_rows)
    report["artifacts"] = {
        "bronze_csv": paths.bronze,
        "silver_csv": paths.silver,
        "gold_dir": paths.gold,
        "normalized_json": normalized_json_path,
    }

    with open(validation_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Bronze: {paths.bronze}")
    print(f"Silver: {paths.silver}")
    print(f"Gold:   {paths.gold}")
    print(f"Normalized JSON: {normalized_json_path}")
    print(f"Validation report: {validation_report_path}")
    print(f"Validation status: {'PASS' if is_valid else 'FAIL'}")


if __name__ == "__main__":
    main()
