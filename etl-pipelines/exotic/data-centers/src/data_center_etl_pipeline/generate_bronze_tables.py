import hashlib
from datetime import datetime, timezone
from typing import Dict, List

from .constants import NUMERIC_FIELDS


def _parse_float(value: str) -> float:
    return float(value.strip())


def generate_bronze_tables(raw_rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    bronze_rows: List[Dict[str, object]] = []

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
        enriched["_ingested_at"] = ingested_at
        enriched["_row_hash"] = row_hash
        enriched["_is_valid_numeric"] = str(is_numeric).lower()
        bronze_rows.append(enriched)

    return bronze_rows
