# data/  (contents gitignored — populated at runtime, never committed)

- `raw/`       source datasets as downloaded (e.g. the green-lion parquet)
- `processed/` canonical panel, tokenized corpora, embeddings

Pull the Dutch mortgages dataset:
    hf download Algoritmica/green-lion-2024-2025 \
      Overall_2024_2025_all_months.parquet --repo-type dataset --local-dir data/raw
