import json
import os
from datetime import datetime
from typing import Dict, List

from .constants import NUMERIC_FIELDS

CRITICAL_FIELDS = [
    "facility_id",
    "sponsor",
    "country",
    "report_date",
    "gross_revenue_eur",
    "debt_service_eur",
    "market_value_eur",
]


def _is_missing(value: object) -> bool:
    return value is None or str(value).strip() == ""


def _is_invalid_date(value: object) -> bool:
    if _is_missing(value):
        return True
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return False
    except ValueError:
        return True


def _is_invalid_numeric(value: object) -> bool:
    if _is_missing(value):
        return True
    try:
        float(str(value).strip())
        return False
    except ValueError:
        return True


def _is_suspicious_numeric(field: str, value: object) -> bool:
    if _is_invalid_numeric(value):
        return False

    numeric_value = float(str(value).strip())
    if field in {"it_load_mw", "leased_capacity_mw", "debt_service_eur", "market_value_eur", "gross_revenue_eur"}:
        return numeric_value < 0
    if field == "occupancy_pct":
        return numeric_value < 0 or numeric_value > 100
    return False


def build_quality_report(rows: List[Dict[str, object]]) -> Dict[str, float | int]:
    row_count = len(rows)

    missing_key_identifiers = sum(1 for row in rows if _is_missing(row.get("facility_id")))
    missing_critical_fields = sum(1 for row in rows if any(_is_missing(row.get(field)) for field in CRITICAL_FIELDS))
    invalid_dates = sum(1 for row in rows if _is_invalid_date(row.get("report_date")))

    invalid_numeric_values = 0
    suspicious_values = 0
    for row in rows:
        for field in NUMERIC_FIELDS:
            value = row.get(field)
            if _is_invalid_numeric(value):
                invalid_numeric_values += 1
            if _is_suspicious_numeric(field, value):
                suspicious_values += 1

    denominator = max(row_count, 1)
    penalty = (
        missing_key_identifiers
        + missing_critical_fields
        + invalid_dates
        + invalid_numeric_values
        + suspicious_values
    ) / denominator
    overall_quality_score = round(max(0.0, 100.0 - penalty * 10.0), 2)

    return {
        "row_count": row_count,
        "missing_key_identifiers": missing_key_identifiers,
        "missing_critical_fields": missing_critical_fields,
        "invalid_dates": invalid_dates,
        "invalid_numeric_values": invalid_numeric_values,
        "suspicious_values": suspicious_values,
        "overall_quality_score": overall_quality_score,
    }


def write_quality_report(path: str, rows: List[Dict[str, object]]) -> Dict[str, float | int]:
    report = build_quality_report(rows)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return report
