# Exotic ETL · Data Centers

This folder contains an MVP ETL pipeline for **data-center-backed private debt deals**.

It is structured with the same bronze/silver/gold logic used in Deeploans:

- **Bronze**: raw records + ingestion metadata + basic numeric validation
- **Silver**: normalized facility metrics (NOI, DSCR, LTV, occupancy, energy ratio) + covenant status
- **Gold**: investor-facing portfolio outputs (sponsor rollup and portfolio dashboard)

## Folder layout

- `sample_data/raw_facilities.csv`: sample raw facility-level input
- `src/data_center_etl.py`: ETL script
- `output/`: generated bronze/silver/gold outputs

## Run

```bash
cd etl-pipelines/exotic/data-centers
make run
```

Or directly:

```bash
python src/data_center_etl.py --input sample_data/raw_facilities.csv --output output
```
