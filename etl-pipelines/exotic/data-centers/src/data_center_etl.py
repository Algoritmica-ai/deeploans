#!/usr/bin/env python3
"""Data-center ETL runner using bronze/silver/gold medallion stages."""

from __future__ import annotations

import argparse
import os

from data_center_etl_pipeline.generate_asset_silver import generate_asset_silver
from data_center_etl_pipeline.generate_bronze_tables import generate_bronze_tables
from data_center_etl_pipeline.generate_gold_tables import generate_gold_tables
from data_center_etl_pipeline.io import read_csv, write_csv
from data_center_etl_pipeline.pipeline import run_etl


def run_stage(input_csv: str, output_root: str, stage_name: str) -> None:
    bronze_path = os.path.join(output_root, "bronze", "facility_bronze.csv")
    silver_path = os.path.join(output_root, "silver", "facility_silver.csv")
    gold_sponsor_path = os.path.join(output_root, "gold", "sponsor_rollup.csv")
    gold_dashboard_path = os.path.join(output_root, "gold", "portfolio_dashboard.csv")

    if stage_name == "bronze":
        raw_rows = read_csv(input_csv)
        write_csv(bronze_path, generate_bronze_tables(raw_rows))
        print(f"Bronze: {bronze_path}")
        return

    if stage_name == "silver":
        bronze_rows = read_csv(bronze_path)
        write_csv(silver_path, generate_asset_silver(bronze_rows))
        print(f"Silver: {silver_path}")
        return

    if stage_name == "gold":
        silver_rows = read_csv(silver_path)
        gold_tables = generate_gold_tables(silver_rows)
        write_csv(gold_sponsor_path, gold_tables["sponsor_rollup"])
        write_csv(gold_dashboard_path, gold_tables["portfolio_dashboard"])
        print(f"Gold:   {os.path.dirname(gold_sponsor_path)}")
        return

    paths = run_etl(input_csv, output_root)
    print(f"Bronze: {paths.bronze}")
    print(f"Silver: {paths.silver}")
    print(f"Gold:   {paths.gold}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the data-center ETL pipeline")
    parser.add_argument("--input", required=True, help="Path to raw facility CSV")
    parser.add_argument("--output", required=True, help="Output folder for bronze/silver/gold layers")
    parser.add_argument(
        "--stage-name",
        default="all",
        choices=["bronze", "silver", "gold", "all"],
        help="Name of the ETL stage to run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_stage(args.input, args.output, args.stage_name)


if __name__ == "__main__":
    main()
