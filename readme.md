# deeploans

**Open infrastructure for granular credit data, from ingestion to modelling.**

deeploans is an Apache 2.0-licensed collection of tools for turning fragmented
loan-level data into consistent, analysis-ready datasets and using those datasets
in applications, AI integrations, synthetic-data workflows, and credit models.

The project began as an ETL framework for structured-finance data. It now covers
the wider credit-data lifecycle:

```text
raw loan data
    │
    ▼
ETL pipelines ──► validated, standardised datasets ──► API / analyst apps / MCP
                              │
                              ├──► synthetic panel generation
                              └──► credit foundation-model training and scoring
```

## What's in this repository

| Component | Purpose | Start here |
| --- | --- | --- |
| **ETL pipelines** | Ingest, validate, transform, and standardise granular asset data in a GCP-based lakehouse. | [`etl-pipelines/readme.md`](etl-pipelines/readme.md) |
| **Credit Foundation Model** | Config-driven framework for tokenising credit-event sequences, pretraining credit foundation models, fine-tuning them, and scoring portfolios. | [`credit-foundation-model/README.md`](credit-foundation-model/README.md) |
| **Synthetic Data Designer** | Reproducible generator for an ESMA Annex 2-aligned Dutch RMBS monthly panel, including longitudinal loan dynamics and SQL validation. | [`synthetic-data-designer/README.md`](synthetic-data-designer/README.md) |
| **API** | FastAPI backend and OpenAPI specification for programmatic access to processed credit data. | [`api/api-backend-main/readme.md`](api/api-backend-main/readme.md) |
| **Application library** | Browser-based reference applications for data quality, CMBS data-provider workflows, data-centre junior-note analysis, and capital-structure modelling. | [`app-library/`](app-library/) |
| **MCP server** | Standalone Model Context Protocol server that lets AI clients discover the platform, inspect schemas, build filters, and sample API data. | [`mcp-server/README.md`](mcp-server/README.md) |

### Supported structured-finance datasets

The ETL collection currently covers:

- auto loans;
- SME loans;
- consumer loans;
- residential mortgages; and
- commercial mortgages.

The individual pipeline directories contain the relevant source-specific setup,
schemas, and validation guidance.

## Recent additions

### Credit Foundation Model framework

The repository now includes a schema-agnostic, configuration-driven framework for
training encoder-only models over month-by-month borrower histories. It provides
key-value-time tokenisation, data preparation, pretraining, downstream fine-tuning,
portfolio scoring, artifact validators, reference recipes, notebooks, and a detailed
handbook. The included reference implementation reports an out-of-time evaluation
against an XGBoost baseline; see the
[`technical report`](credit-foundation-model/docs/technical_report.md) for the
methodology, results, and limitations.

To explore it locally:

```bash
cd credit-foundation-model
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Read the [Credit Foundation Model README](credit-foundation-model/README.md) for
dataset recipes, GPU setup, the end-to-end training commands, and optional extras.

### Synthetic Dutch RMBS panels

The Synthetic Data Designer creates a coherent monthly Dutch residential-mortgage
panel with a 71-column schema aligned to ESMA Annex 2 and the Green Lion reference
format. Data Designer samples the origination book, then vectorised ageing models
amortisation, delinquency transitions, prepayment, and property-value changes. The
default workflow uses no LLM calls.

Run a small local example:

```bash
cd synthetic-data-designer
python -m venv .venv
source .venv/bin/activate
pip install data-designer numpy pandas pyarrow duckdb
python run.py --num-records 5000 --out-dir ./out_smoke
python tests/run_sql_tests.py --cutoff-dir ./out_smoke/cutoffs
```

See the [Synthetic Data Designer README](synthetic-data-designer/README.md) before
attempting a production-scale run; it documents expected runtime, memory, disk use,
calibration controls, and known limitations.

## Choose a starting point

- **I need clean, standardised loan data:** begin with the
  [lakehouse and ETL overview](etl-pipelines/readme.md).
- **I want to train or evaluate a credit sequence model:** follow the
  [Credit Foundation Model quickstart](credit-foundation-model/README.md#quickstart)
  or start with its [handbook](credit-foundation-model/docs/handbook/00_README.md).
- **I need a synthetic mortgage panel:** use the
  [Synthetic Data Designer quickstart](synthetic-data-designer/README.md#quick-start).
- **I want to connect an AI client:** install the
  [MCP server](mcp-server/README.md#quick-start).
- **I want to build on the data API:** review the
  [backend documentation](api/api-backend-main/readme.md) and
  [OpenAPI specification](api/api-backend-main/openapi.json).
- **I want to see example user interfaces:** browse the
  [application library](app-library/).

## Repository map

```text
deeploans/
├── etl-pipelines/              data ingestion, validation, and lakehouse pipelines
├── credit-foundation-model/    credit sequence-model framework and references
├── synthetic-data-designer/    synthetic Dutch RMBS panel generator
├── api/                        FastAPI backend and API documentation
├── app-library/                reference analyst applications
├── mcp-server/                 MCP server for AI/client integrations
├── CONTRIBUTING.md             contribution guidelines
└── LICENSE                     Apache License 2.0
```

Each component is independently documented and may have its own environment and
dependencies. Follow the component README rather than installing everything into a
single Python environment.

## Contributing

Contributions from developers, analysts, researchers, and documentation writers are
welcome. Useful ways to help include:

- adding or improving validation rules and ETL coverage;
- testing workflows on real-world data and reporting reproducible issues;
- extending dataset adapters, recipes, examples, or documentation;
- proposing applications and integrations; and
- improving usability for first-time contributors.

Before opening a pull request, read [`CONTRIBUTING.md`](CONTRIBUTING.md) and the
[`CLA`](cla.md). For design proposals and feature ideas, use the organisation's
[GitHub Discussions](https://github.com/orgs/Algoritmica-ai/discussions).

## License

deeploans is licensed under the [Apache License 2.0](LICENSE). Components imported
from related projects also include their own license files; consult the component
directory when redistributing it independently.

## Contact

- [luca.borella@algoritmica.ai](mailto:luca.borella@algoritmica.ai)
- [dylan.thiam@algoritmica.ai](mailto:dylan.p.thiam@algoritmica.ai)
