#!/usr/bin/env python3
"""Minimal ETL pipeline for exotic data-center-backed private debt deals."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List

NUMERIC_FIELDS = [
    "gross_revenue_eur",
    "energy_cost_eur",
    "opex_eur",
    "debt_service_eur",
    "market_value_eur",
    "outstanding_debt_eur",
    "it_load_mw",
    "leased_capacity_mw",
]

DSCR_WATCH = 1.35
DSCR_BREACH = 1.20
LTV_WATCH = 74.0
LTV_BREACH = 78.0


@dataclass
class Paths:
    bronze: str
    silver: str
    gold: str


def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: str, rows: Iterable[Dict[str, object]]) -> None:
    rows = list(rows)
    if not rows:
        raise ValueError(f"Refusing to write empty dataset to {path}")
    fieldnames = list(rows[0].keys())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_float(value: str) -> float:
    return float(value.strip())


def bronze_transform(raw_rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    now = datetime.now(timezone.utc).isoformat()
    output = []
    for row in raw_rows:
        combined = "|".join(str(row[k]) for k in sorted(row.keys()))
        row_hash = hashlib.sha256(combined.encode("utf-8")).hexdigest()
        is_numeric = True
        for field in NUMERIC_FIELDS:
            try:
                _parse_float(row[field])
            except (KeyError, ValueError, AttributeError):
                is_numeric = False
                break

        enriched = dict(row)
        enriched["_ingested_at"] = now
        enriched["_row_hash"] = row_hash
        enriched["_is_valid_numeric"] = str(is_numeric).lower()
        output.append(enriched)
    return output


def covenant_status(dscr: float, ltv: float) -> str:
    if dscr < DSCR_BREACH or ltv > LTV_BREACH:
        return "breach"
    if dscr < DSCR_WATCH or ltv > LTV_WATCH:
        return "watch"
    return "ok"


def silver_transform(bronze_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    output = []
    for row in bronze_rows:
        if row.get("_is_valid_numeric") != "true":
            continue

        gross_revenue = _parse_float(str(row["gross_revenue_eur"]))
        energy_cost = _parse_float(str(row["energy_cost_eur"]))
        opex = _parse_float(str(row["opex_eur"]))
        debt_service = _parse_float(str(row["debt_service_eur"]))
        market_value = _parse_float(str(row["market_value_eur"]))
        debt_out = _parse_float(str(row["outstanding_debt_eur"]))
        it_load = _parse_float(str(row["it_load_mw"]))
        leased = _parse_float(str(row["leased_capacity_mw"]))

        noi = gross_revenue - energy_cost - opex
        dscr = noi / debt_service if debt_service else 0.0
        ltv = (debt_out / market_value * 100.0) if market_value else 0.0
        occupancy = (leased / it_load * 100.0) if it_load else 0.0
        energy_ratio = (energy_cost / gross_revenue * 100.0) if gross_revenue else 0.0

        output.append(
            {
                "facility_id": row["facility_id"],
                "sponsor": row["sponsor"],
                "country": row["country"],
                "report_date": row["report_date"],
                "noi_eur": round(noi, 2),
                "dscr": round(dscr, 3),
                "ltv_pct": round(ltv, 2),
                "occupancy_pct": round(occupancy, 2),
                "energy_cost_ratio_pct": round(energy_ratio, 2),
                "junior_note_watch_status": covenant_status(dscr, ltv),
            }
        )
    return output


def gold_transform(silver_rows: List[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
    sponsor_rollup: Dict[str, Dict[str, object]] = {}
    breaches = 0
    watch = 0

    for row in silver_rows:
        sponsor = str(row["sponsor"])
        bucket = sponsor_rollup.setdefault(
            sponsor,
            {
                "sponsor": sponsor,
                "facility_count": 0,
                "avg_dscr": 0.0,
                "avg_ltv_pct": 0.0,
                "avg_occupancy_pct": 0.0,
                "breach_count": 0,
                "watch_count": 0,
            },
        )

        bucket["facility_count"] += 1
        bucket["avg_dscr"] += float(row["dscr"])
        bucket["avg_ltv_pct"] += float(row["ltv_pct"])
        bucket["avg_occupancy_pct"] += float(row["occupancy_pct"])

        status = str(row["junior_note_watch_status"])
        if status == "breach":
            bucket["breach_count"] += 1
            breaches += 1
        elif status == "watch":
            bucket["watch_count"] += 1
            watch += 1

    sponsor_rows = []
    for bucket in sponsor_rollup.values():
        count = bucket["facility_count"]
        bucket["avg_dscr"] = round(bucket["avg_dscr"] / count, 3)
        bucket["avg_ltv_pct"] = round(bucket["avg_ltv_pct"] / count, 2)
        bucket["avg_occupancy_pct"] = round(bucket["avg_occupancy_pct"] / count, 2)
        sponsor_rows.append(bucket)

    portfolio_row = {
        "as_of": datetime.now(timezone.utc).date().isoformat(),
        "facility_count": len(silver_rows),
        "avg_dscr": round(sum(float(r["dscr"]) for r in silver_rows) / len(silver_rows), 3),
        "avg_ltv_pct": round(sum(float(r["ltv_pct"]) for r in silver_rows) / len(silver_rows), 2),
        "avg_occupancy_pct": round(sum(float(r["occupancy_pct"]) for r in silver_rows) / len(silver_rows), 2),
        "watch_count": watch,
        "breach_count": breaches,
    }

    return {
        "sponsor_rollup": sponsor_rows,
        "portfolio_dashboard": [portfolio_row],
    }


def run_etl(input_csv: str, output_root: str) -> Paths:
    bronze_path = os.path.join(output_root, "bronze", "facility_bronze.csv")
    silver_path = os.path.join(output_root, "silver", "facility_silver.csv")
    gold_sponsor_path = os.path.join(output_root, "gold", "sponsor_rollup.csv")
    gold_dashboard_path = os.path.join(output_root, "gold", "portfolio_dashboard.csv")

    raw = read_csv(input_csv)
    bronze = bronze_transform(raw)
    silver = silver_transform(bronze)
    gold = gold_transform(silver)

    write_csv(bronze_path, bronze)
    write_csv(silver_path, silver)
    write_csv(gold_sponsor_path, gold["sponsor_rollup"])
    write_csv(gold_dashboard_path, gold["portfolio_dashboard"])

    return Paths(bronze=bronze_path, silver=silver_path, gold=os.path.dirname(gold_sponsor_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the data-center ETL MVP")
    parser.add_argument("--input", required=True, help="Path to raw facility CSV")
    parser.add_argument("--output", required=True, help="Output folder for bronze/silver/gold layers")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = run_etl(args.input, args.output)
    print(f"Bronze: {paths.bronze}")
    print(f"Silver: {paths.silver}")
    print(f"Gold:   {paths.gold}")


if __name__ == "__main__":
    main()
