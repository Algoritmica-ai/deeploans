from datetime import datetime, timezone
from typing import Dict, List


def generate_gold_tables(silver_rows: List[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
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
