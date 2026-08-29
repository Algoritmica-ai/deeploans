# Exotic ETL · Data Centers

This folder contains an MVP ETL pipeline for **data-center-backed private debt deals**.

It follows the same medallion architecture patterns used by the other ETLs:

- **Bronze**: raw records + ingestion metadata + basic numeric validation
- **Silver**: normalized facility metrics (NOI, DSCR, LTV, occupancy, energy ratio) + covenant status
- **Gold**: investor-facing portfolio outputs (sponsor rollup and portfolio dashboard)

## Folder layout

- `sample_data/raw_facilities.csv`: sample raw facility-level input
- `src/data_center_etl.py`: ETL entrypoint with stage-based execution
- `src/data_center_etl_pipeline/`: stage modules (`generate_bronze_tables`, `generate_asset_silver`, `generate_gold_tables`)
- `output/`: generated bronze/silver/gold outputs

## Run

```bash
cd etl-pipelines/exotic/data-centers
make run
```

Or directly:

```bash
python src/data_center_etl.py --input sample_data/raw_facilities.csv --output output --stage-name all
```

To execute an individual stage:

```bash
python src/data_center_etl.py --input sample_data/raw_facilities.csv --output output --stage-name bronze
python src/data_center_etl.py --input sample_data/raw_facilities.csv --output output --stage-name silver
python src/data_center_etl.py --input sample_data/raw_facilities.csv --output output --stage-name gold
```

## End-to-end sample (raw CSV -> API-ready JSON + validation report)

Run one command to execute the full ETL and generate normalized JSON with a validation report:

```bash
cd etl-pipelines/exotic/data-centers
python src/run_e2e_sample.py
```

Generated artifacts:

- `output/bronze/facility_bronze.csv`
- `output/silver/facility_silver.csv`
- `output/gold/sponsor_rollup.csv`
- `output/gold/portfolio_dashboard.csv`
- `output/normalized/facility_normalized.json` (API-ready normalized payload)
- `output/reports/validation_report.json` (schema/quality checks summary)

Run tests for the sample:

```bash
pytest etl-pipelines/exotic/data-centers/tests/test_e2e_sample.py
```
