import csv
import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from data_center_etl_pipeline.pipeline import run_etl


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_run_etl_generates_quality_report(tmp_path: Path) -> None:
    rows = [
        {"facility_id": "dc-001", "sponsor": "Alpha", "country": "DE", "report_date": "2026-03-01", "gross_revenue_eur": "100", "energy_cost_eur": "20", "opex_eur": "10", "debt_service_eur": "30", "market_value_eur": "500", "outstanding_debt_eur": "300", "it_load_mw": "10", "leased_capacity_mw": "8"},
        {"facility_id": "", "sponsor": "Beta", "country": "FR", "report_date": "03/01/2026", "gross_revenue_eur": "-90", "energy_cost_eur": "abc", "opex_eur": "10", "debt_service_eur": "20", "market_value_eur": "100", "outstanding_debt_eur": "50", "it_load_mw": "-1", "leased_capacity_mw": "0"},
    ]

    input_csv = tmp_path / "input.csv"
    output_root = tmp_path / "output"
    _write_csv(input_csv, rows)

    paths = run_etl(str(input_csv), str(output_root))

    report_path = Path(paths.quality_report)
    assert report_path.exists()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert set(report.keys()) == {
        "row_count",
        "missing_key_identifiers",
        "missing_critical_fields",
        "invalid_dates",
        "invalid_numeric_values",
        "suspicious_values",
        "overall_quality_score",
    }
    assert report["row_count"] == 2
    assert report["missing_key_identifiers"] == 1
    assert report["invalid_dates"] == 1
    assert report["invalid_numeric_values"] >= 1
    assert report["suspicious_values"] >= 1
