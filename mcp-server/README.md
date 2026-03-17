# Deeploans MCP Server

This folder contains a standalone MCP (Model Context Protocol) server for Deeploans.

## What it provides

The server currently exposes these tools:

- `list_platform_components`: high-level map of Deeploans components
- `get_asset_classes`: currently supported ETL asset classes
- `fetch_api_docs`: fetches OpenAPI metadata from a running backend API (with local fallback)
- `list_tables`: lists available API tables for a given credit type
- `describe_table`: returns column/type/filter metadata for a specific table
- `build_filter_examples`: generates filter query examples from schema metadata
- `sample_rows`: fetches preview rows from API endpoints for analyst workflows

## Quick start

```bash
cd mcp-server
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run the server over stdio:

```bash
deeploans-mcp
```

## Example MCP client config

```json
{
  "mcpServers": {
    "deeploans": {
      "command": "deeploans-mcp"
    }
  }
}
```

## Notes

- API-calling tools expect the backend to be running (default: `http://localhost:8000`).
- `fetch_api_docs`/`list_tables` can still work offline by reading local OpenAPI metadata.
- `describe_table` and `build_filter_examples` use local table schema metadata.
- This service is intentionally separate from the FastAPI backend so it can evolve independently.
