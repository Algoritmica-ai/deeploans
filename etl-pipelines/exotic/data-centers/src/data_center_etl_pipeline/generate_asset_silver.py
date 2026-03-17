from typing import Dict, List

from .constants import DSCR_BREACH, DSCR_WATCH, LTV_BREACH, LTV_WATCH


def _parse_float(value: str) -> float:
    return float(value.strip())


def _covenant_status(dscr: float, ltv: float) -> str:
    if dscr < DSCR_BREACH or ltv > LTV_BREACH:
        return "breach"
    if dscr < DSCR_WATCH or ltv > LTV_WATCH:
        return "watch"
    return "ok"


def generate_asset_silver(bronze_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    silver_rows: List[Dict[str, object]] = []

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

        silver_rows.append(
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
                "junior_note_watch_status": _covenant_status(dscr, ltv),
            }
        )

    return silver_rows
