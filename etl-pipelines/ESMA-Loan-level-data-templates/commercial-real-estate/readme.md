# Commercial Real Estate (CRE) ETL for ESMA Annex 3

This folder now contains a working **CRE ETL pipeline** based on the
`annex3_underlying_exposures-commercial_real_estate (2).xlsx` taxonomy and designed for Deeploans integration.

## What it does

1. Extracts CRE taxonomy metadata (`CREL*` fields) from the Annex 3 workbook.
2. Validates and normalizes a CRE underlying-exposures CSV.
3. Produces a normalized JSON output consumable by apps and API adapters.

## Pipeline layout

- `src/cre_etl_pipeline/taxonomy.py`: taxonomy extraction from XLSX (stdlib only).
- `src/cre_etl_pipeline/pipeline.py`: normalization and quality checks.
- `src/cre_main.py`: CLI entrypoint.
- `sample_data/cre_underlying_exposures_sample.csv`: sample CRE tape for local runs.
- `output/`: generated artifacts.

## Run locally

```bash
cd etl-pipelines/ESMA-Loan-level-data-templates/commercial-real-estate
PYTHONPATH=src python src/cre_main.py \
  --input_csv sample_data/cre_underlying_exposures_sample.csv \
  --taxonomy_xlsx "annex3_underlying_exposures-commercial_real_estate (2).xlsx" \
  --output_json output/cre_normalized.json \
  --taxonomy_json output/cre_taxonomy.json \
  --dl_code BMARK_2026-CMBS1
```

## Output contract

`output/cre_normalized.json` includes:

- `asset_class`
- `deeploans_code`
- `record_count`
- `quality.rows_missing_primary_identifiers`
- `records[]` with harmonized fields (`deal_id`, `loan_id`, `current_balance`, `dscr`, etc.)

This output can be ingested by the CMBS app in `app-library/cmbs-data-provider-workbench`.
