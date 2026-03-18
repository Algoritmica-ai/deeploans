from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from .taxonomy import export_taxonomy_json, load_cre_taxonomy


def _normalize_value(value: str) -> Any:
    value = value.strip()
    if value in {"", "ND", "ND1", "ND2", "ND3", "ND4", "ND5", "NA"}:
        return None
    return value


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def transform_underlying_exposures(
    input_csv: str | Path,
    taxonomy_xlsx: str | Path,
    output_json: str | Path,
    dl_code: str,
) -> dict[str, Any]:
    taxonomy = load_cre_taxonomy(taxonomy_xlsx)
    allowed_fields = {field.field_code for field in taxonomy}

    output_rows: list[dict[str, Any]] = []
    missing_required = 0

    with Path(input_csv).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {key: _normalize_value(value or "") for key, value in raw_row.items() if key in allowed_fields}

            # Minimal harmonized metrics for app / API style consumption.
            balance = _safe_float(row.get("CREL41") or row.get("CREL40"))
            appraised_value = _safe_float(row.get("CREL32"))
            dscr = _safe_float(row.get("CREL44"))
            occupancy = _safe_float(row.get("CREL59"))

            if row.get("CREL1") is None or row.get("CREL4") is None:
                missing_required += 1

            output_rows.append(
                {
                    "deal_id": row.get("CREL1"),
                    "obligor_id": row.get("CREL3") or row.get("CREL2"),
                    "loan_id": row.get("CREL5") or row.get("CREL4"),
                    "property_type": row.get("CREL26"),
                    "country": row.get("CREL19"),
                    "current_balance": balance,
                    "appraised_value": appraised_value,
                    "dscr": dscr,
                    "occupancy_rate": occupancy,
                    "default_status": row.get("CREL68"),
                    "special_servicing": row.get("CREL71"),
                    "raw": row,
                }
            )

    payload = {
        "asset_class": "cre",
        "deeploans_code": dl_code,
        "record_count": len(output_rows),
        "quality": {
            "rows_missing_primary_identifiers": missing_required,
        },
        "records": output_rows,
    }

    output_path = Path(output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="CRE ESMA ETL for Deeploans integration.")
    parser.add_argument("--input_csv", required=True, help="Path to CRE underlying exposures CSV file")
    parser.add_argument("--taxonomy_xlsx", required=True, help="Path to Annex 3 CRE taxonomy workbook")
    parser.add_argument("--output_json", required=True, help="Path to output normalized JSON")
    parser.add_argument("--taxonomy_json", required=True, help="Path to output extracted taxonomy JSON")
    parser.add_argument("--dl_code", default="CRE-DEMO-001", help="Deeploans deal code")
    args = parser.parse_args()

    export_taxonomy_json(args.taxonomy_xlsx, args.taxonomy_json)
    result = transform_underlying_exposures(
        input_csv=args.input_csv,
        taxonomy_xlsx=args.taxonomy_xlsx,
        output_json=args.output_json,
        dl_code=args.dl_code,
    )
    print(json.dumps({"status": "ok", "record_count": result["record_count"]}, indent=2))


if __name__ == "__main__":
    main()
