# Deeploans API overview

This folder contains API-related assets for the Deeploans project.

## Main backend location

The primary backend implementation is in:

- `api/api-backend-main/backend/`

Key files:

- `app/server.py`: FastAPI application entrypoint
- `app/routers.py`: API route registration
- `openapi.json`: API schema snapshot

## Quick start (local)

From the repository root:

```bash
cd api/api-backend-main
```

Then follow the instructions in:

- `api/api-backend-main/readme.md`

## What this API provides

At a high level, the backend exposes endpoints to:

- access table data by credit type
- discover schema and available filters
- support downstream apps and analyst workflows

## Related folders

- `api/api-backend-main/backend/tests/`: API and filter parser tests
- `api/api-backend-main/openapi.json`: OpenAPI metadata used by tooling
- `mcp-server/`: MCP server that can consume API metadata
