"""Deeploans MCP server.

This server exposes a small set of tools to help AI clients discover Deeploans
components and inspect API health/documentation endpoints.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("deeploans")


@mcp.tool()
def list_platform_components() -> dict[str, Any]:
    """Return a high-level view of Deeploans components and repository layout."""
    return {
        "project": "deeploans",
        "components": [
            {
                "name": "ETL-Pipelines",
                "purpose": "Data processing and lakehouse pipelines",
                "path": "ETL-Pipelines/",
            },
            {
                "name": "api-backend",
                "purpose": "FastAPI backend for querying datasets",
                "path": "api/api-backend-main/backend/",
            },
            {
                "name": "data-quality-app",
                "purpose": "Frontend utility app for data quality checks",
                "path": "deeploans-app-library/data-quality-app/",
            },
        ],
    }


@mcp.tool()
def get_asset_classes() -> list[str]:
    """Return the asset classes currently covered by Deeploans ETLs."""
    return [
        "SME loans",
        "Residential mortgages",
        "Consumer lending",
        "Auto loans",
    ]


@mcp.tool()
def fetch_api_docs(base_url: str = "http://localhost:8000") -> dict[str, Any]:
    """Fetch the backend OpenAPI JSON from a running Deeploans API instance.

    Args:
        base_url: Base URL for the backend API service.

    Returns:
        Dictionary with status and OpenAPI metadata.
    """
    openapi_url = f"{base_url.rstrip('/')}/openapi.json"
    request = urllib.request.Request(openapi_url, method="GET")

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return {
                "ok": True,
                "url": openapi_url,
                "title": payload.get("info", {}).get("title"),
                "version": payload.get("info", {}).get("version"),
                "paths_count": len(payload.get("paths", {})),
            }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "url": openapi_url,
            "error": str(exc),
        }


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
