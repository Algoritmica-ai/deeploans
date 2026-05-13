import os
from dataclasses import dataclass

from .generate_asset_silver import generate_asset_silver
from .generate_bronze_tables import generate_bronze_tables
from .generate_gold_tables import generate_gold_tables
from .io import read_csv, write_csv
from .quality_report import write_quality_report


@dataclass
class Paths:
    bronze: str
    silver: str
    gold: str
    quality_report: str


def run_etl(input_csv: str, output_root: str) -> Paths:
    bronze_path = os.path.join(output_root, "bronze", "facility_bronze.csv")
    silver_path = os.path.join(output_root, "silver", "facility_silver.csv")
    gold_sponsor_path = os.path.join(output_root, "gold", "sponsor_rollup.csv")
    gold_dashboard_path = os.path.join(output_root, "gold", "portfolio_dashboard.csv")
    quality_report_path = os.path.join(output_root, "quality_report.json")

    raw_rows = read_csv(input_csv)
    bronze_rows = generate_bronze_tables(raw_rows)
    silver_rows = generate_asset_silver(bronze_rows)
    gold_tables = generate_gold_tables(silver_rows)

    write_csv(bronze_path, bronze_rows)
    write_csv(silver_path, silver_rows)
    write_csv(gold_sponsor_path, gold_tables["sponsor_rollup"])
    write_csv(gold_dashboard_path, gold_tables["portfolio_dashboard"])
    write_quality_report(quality_report_path, raw_rows)

    return Paths(bronze=bronze_path, silver=silver_path, gold=os.path.dirname(gold_sponsor_path), quality_report=quality_report_path)
