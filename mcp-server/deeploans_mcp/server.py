"""Deeploans MCP server.

This server exposes tools that help AI clients discover Deeploans components,
inspect API metadata, and perform analyst-friendly dataset introspection.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("deeploans")

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_OPENAPI_PATH = REPO_ROOT / "api" / "api-backend-main" / "openapi.json"
LOCAL_SCHEMA_PATH = (
    REPO_ROOT
    / "api"
    / "api-backend-main"
    / "backend"
    / "app"
    / "files"
    / "tables_with_filters.json"
)


class MetadataLoadError(RuntimeError):
    """Raised when OpenAPI or table schema metadata cannot be loaded."""


def _load_local_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise MetadataLoadError(f"Metadata file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MetadataLoadError(f"Invalid JSON in {path}: {exc}") from exc


def _fetch_openapi_json(base_url: str, timeout_seconds: int = 5) -> dict[str, Any]:
    openapi_url = f"{base_url.rstrip('/')}/openapi.json"
    request = urllib.request.Request(openapi_url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _load_openapi_metadata(base_url: str | None = None) -> dict[str, Any]:
    """Load OpenAPI metadata from API when available, local file otherwise."""
    if base_url:
        try:
            return _fetch_openapi_json(base_url=base_url)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            pass

    return _load_local_json(LOCAL_OPENAPI_PATH)


def _load_tables_schema() -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    return _load_local_json(LOCAL_SCHEMA_PATH)


def _extract_credit_type_and_table(path: str) -> tuple[str, str] | None:
    parts = path.strip("/").split("/")
    if len(parts) >= 4 and parts[0] == "api" and parts[1] == "v1":
        return parts[2], parts[3]
    return None


def _normalize_identifier(value: str) -> str:
    return value.strip().lower()


@mcp.tool()
def list_platform_components() -> dict[str, Any]:
    """Return a high-level view of Deeploans components and repository layout."""
    return {
        "project": "deeploans",
        "components": [
            {
                "name": "etl-pipelines",
                "purpose": "Data processing and lakehouse pipelines",
                "path": "etl-pipelines/",
            },
            {
                "name": "api-backend",
                "purpose": "FastAPI backend for querying datasets",
                "path": "api/api-backend-main/backend/",
            },
            {
                "name": "data-quality-app",
                "purpose": "Frontend utility app for data quality checks",
                "path": "app-library/data-quality/",
            },
            {
                "name": "hastructure-studio",
                "purpose": "Payload builder and connector for Hastructure scenarios",
                "path": "app-library/hastructure-studio/",
            },
            {
                "name": "mcp-server",
                "purpose": "Model Context Protocol server for AI integrations",
                "path": "mcp-server/",
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
    """Fetch OpenAPI metadata from API, with local-file fallback for offline use."""
    openapi_url = f"{base_url.rstrip('/')}/openapi.json"
    try:
        payload = _fetch_openapi_json(base_url=base_url)
        source = "api"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        try:
            payload = _load_local_json(LOCAL_OPENAPI_PATH)
            source = "local_file"
        except MetadataLoadError as exc:
            return {"ok": False, "url": openapi_url, "error": str(exc)}

    return {
        "ok": True,
        "url": openapi_url,
        "source": source,
        "title": payload.get("info", {}).get("title"),
        "version": payload.get("info", {}).get("version"),
        "paths_count": len(payload.get("paths", {})),
    }


@mcp.tool()
def list_tables(credit_type: str, base_url: str | None = None) -> dict[str, Any]:
    """List API tables available for a credit type.

    Args:
        credit_type: Dataset credit type (e.g., "sme").
        base_url: Optional API base URL. If unavailable, local OpenAPI is used.
    """
    normalized_credit_type = _normalize_identifier(credit_type)
    if not normalized_credit_type:
        return {"ok": False, "credit_type": credit_type, "error": "credit_type is required"}

    try:
        payload = _load_openapi_metadata(base_url=base_url)
    except MetadataLoadError as exc:
        return {"ok": False, "credit_type": credit_type, "error": str(exc)}

    tables = sorted(
        {
            parsed[1]
            for path in payload.get("paths", {})
            if (parsed := _extract_credit_type_and_table(path)) and parsed[0] == normalized_credit_type
        }
    )

    if not tables:
        return {
            "ok": False,
            "credit_type": normalized_credit_type,
            "tables": [],
            "count": 0,
            "error": "No tables found for credit_type",
        }

    return {
        "ok": True,
        "credit_type": normalized_credit_type,
        "tables": tables,
        "count": len(tables),
    }


@mcp.tool()
def describe_table(credit_type: str, table_name: str) -> dict[str, Any]:
    """Describe available columns and filterability for a table from local schema metadata."""
    normalized_credit_type = _normalize_identifier(credit_type)
    normalized_table_name = _normalize_identifier(table_name)
    if not normalized_credit_type or not normalized_table_name:
        return {
            "ok": False,
            "credit_type": normalized_credit_type,
            "table_name": normalized_table_name,
            "error": "credit_type and table_name are required",
        }

    try:
        schema = _load_tables_schema()
    except MetadataLoadError as exc:
        return {
            "ok": False,
            "credit_type": normalized_credit_type,
            "table_name": normalized_table_name,
            "error": str(exc),
        }

    table_meta = schema.get(normalized_credit_type, {}).get(normalized_table_name)
    if not table_meta:
        return {
            "ok": False,
            "credit_type": normalized_credit_type,
            "table_name": normalized_table_name,
            "error": "Table metadata not found",
            "available_credit_types": sorted(schema.keys()),
            "available_tables": sorted(schema.get(normalized_credit_type, {}).keys()),
        }

    columns = []
    for column, config in table_meta.items():
        columns.append(
            {
                "name": column,
                "type": config.get("type"),
                "is_filter": bool(config.get("is_filter", False)),
            }
        )

    filterable = [c["name"] for c in columns if c["is_filter"]]

    return {
        "ok": True,
        "credit_type": normalized_credit_type,
        "table_name": normalized_table_name,
        "column_count": len(columns),
        "filterable_count": len(filterable),
        "filterable_columns": filterable,
        "columns": columns,
    }


@mcp.tool()
def build_filter_examples(credit_type: str, table_name: str, limit: int = 8) -> dict[str, Any]:
    """Generate example filter clauses based on schema types for a table."""
    table_description = describe_table(credit_type=credit_type, table_name=table_name)
    if not table_description.get("ok"):
        return table_description

    type_examples = {
        "STRING": "{col}:ABC",
        "DATE": "{col}>=2024-01-01",
        "INTEGER": "{col}>0",
        "FLOAT": "{col}>=0.0",
        "BOOLEAN": "{col}:TRUE",
        "BOOL": "{col}:TRUE",
    }

    examples: list[str] = []
    for col in table_description["columns"]:
        if not col["is_filter"]:
            continue
        template = type_examples.get(col["type"], "{col}:VALUE")
        examples.append(template.format(col=col["name"]))
        if len(examples) >= max(1, limit):
            break

    combined_examples = []
    if len(examples) >= 2:
        combined_examples.append(f"{examples[0]} AND {examples[1]}")
    if len(examples) >= 3:
        combined_examples.append(f"-{examples[0]} OR {examples[2]}")

    return {
        "ok": True,
        "credit_type": table_description["credit_type"],
        "table_name": table_description["table_name"],
        "single_clause_examples": examples,
        "combined_examples": combined_examples,
        "note": "Use : for equals, ~ for LIKE, and prefix '-' for negation.",
    }


@mcp.tool()
def sample_rows(
    credit_type: str,
    table_name: str,
    limit: int = 5,
    base_url: str = "http://localhost:8000",
    api_key: str | None = None,
) -> dict[str, Any]:
    """Fetch sample rows from Deeploans API for analyst previewing.

    Returns an informative error if the API is unreachable or unauthorized.
    """
    normalized_credit_type = _normalize_identifier(credit_type)
    normalized_table_name = _normalize_identifier(table_name)
    if not normalized_credit_type or not normalized_table_name:
        return {
            "ok": False,
            "credit_type": credit_type,
            "table_name": table_name,
            "error": "credit_type and table_name are required",
        }

    try:
        row_limit = max(1, min(int(limit), 100))
    except ValueError:
        return {"ok": False, "error": "limit must be an integer"}

    url = f"{base_url.rstrip('/')}/api/v1/{normalized_credit_type}/{normalized_table_name}?limit={row_limit}&offset=0"
    headers = {"Accept": "application/json"}
    if api_key:
        headers["x-algoritmica-api-key"] = api_key

    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
            rows = payload if isinstance(payload, list) else payload.get("data", [payload])
            rows = rows if isinstance(rows, list) else []
            return {
                "ok": True,
                "url": url,
                "row_count": len(rows),
                "sample": rows,
            }
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="ignore") if exc.fp else ""
        return {
            "ok": False,
            "url": url,
            "error": f"HTTP {exc.code}",
            "details": details,
        }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "url": url,
            "error": str(exc),
            "hint": "Start the API backend and pass a valid x-algoritmica-api-key.",
        }


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
