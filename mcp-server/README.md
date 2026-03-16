# Deeploans MCP Server

This folder contains a standalone MCP (Model Context Protocol) server for Deeploans.

## What it provides

The server currently exposes these tools:

- `list_platform_components`: high-level map of Deeploans components
- `get_asset_classes`: currently supported ETL asset classes
- `fetch_api_docs`: fetches OpenAPI metadata from a running backend API

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

- `fetch_api_docs` expects the API to be running (default: `http://localhost:8000`).
- This service is intentionally separate from the FastAPI backend so it can evolve independently.
