"""
Integration tests for SME API endpoints using FastAPI's TestClient.

External services (BigQuery, MongoDB) are mocked — these tests exercise
routing, middleware auth, query-parameter parsing, and error responses.
"""
import pytest
from unittest.mock import patch, AsyncMock

from app.config import TABLES


# ── Route existence / discovery ──────────────────────────────────────────────


class TestRouteDiscovery:
    """Verify that all expected SME table endpoints are registered."""

    SME_TABLES = TABLES["sme"]

    def test_all_sme_tables_have_routes(self, test_client, auth_header):
        """Every table listed in config.TABLES['sme'] should have a GET route."""
        for table in self.SME_TABLES:
            resp = test_client.get(f"/api/v1/sme/{table}", headers=auth_header)
            # 400 is acceptable — it means the route matched but BigQuery
            # wasn't mocked for this particular call.  404 would mean no route.
            assert resp.status_code != 404, f"Missing route for /api/v1/sme/{table}"

    def test_trailing_slash_redirects(self, test_client, auth_header):
        """HandleTrailingSlashRouter registers both /path and /path/ variants."""
        resp = test_client.get(
            "/api/v1/sme/obligors/", headers=auth_header, follow_redirects=False,
        )
        assert resp.status_code != 404


# ── Authentication / middleware ──────────────────────────────────────────────


class TestAuthentication:
    """Verify middleware rejects unauthenticated / unauthorised requests."""

    def test_missing_api_key_returns_403(self, test_client):
        resp = test_client.get("/api/v1/sme/obligors")
        assert resp.status_code == 403

    def test_dev_key_passes_auth(self, test_client, auth_header):
        """The DEV API key should bypass the quota check entirely."""
        resp = test_client.get("/api/v1/sme/obligors", headers=auth_header)
        # Not 403 — we got past middleware (will be 400 because BQ is not mocked)
        assert resp.status_code != 403

    def test_swagger_docs_bypass_auth(self, test_client):
        """The /docs endpoint should not require an API key."""
        resp = test_client.get("/docs")
        assert resp.status_code == 200


# ── Root endpoint ────────────────────────────────────────────────────────────


class TestRootEndpoint:
    def test_root_returns_hello_world(self, test_client, auth_header):
        resp = test_client.get("/", headers=auth_header)
        assert resp.status_code == 200
        assert resp.json() == {"Hello": "World"}


# ── SME endpoints with mocked BigQuery ──────────────────────────────────────


class TestSmeEndpointsWithMockedBQ:
    """End-to-end tests with BigQuery responses stubbed out."""

    def test_obligors_returns_mocked_data(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        resp = test_client.get("/api/v1/sme/obligors", headers=auth_header)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["dl_code"] == "TEST001"

    def test_query_called_with_correct_credit_type(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        test_client.get("/api/v1/sme/loans", headers=auth_header)
        call_kwargs = mock_bigquery_dicts.call_args.kwargs
        assert call_kwargs["credit_type"] == "sme"
        assert call_kwargs["table_name"] == "loans"

    def test_limit_param_forwarded(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        test_client.get(
            "/api/v1/sme/obligors", headers=auth_header, params={"limit": 5},
        )
        assert mock_bigquery_dicts.call_args.kwargs["limit"] == 5

    def test_offset_param_forwarded(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        test_client.get(
            "/api/v1/sme/obligors", headers=auth_header, params={"offset": 10},
        )
        assert mock_bigquery_dicts.call_args.kwargs["offset"] == 10

    def test_filter_param_parsed_and_forwarded(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        resp = test_client.get(
            "/api/v1/sme/obligors",
            headers=auth_header,
            params={"filter": "AS15:pl"},
        )
        assert resp.status_code == 200
        filters = mock_bigquery_dicts.call_args.kwargs["filters"]
        assert isinstance(filters, list)
        assert filters[0]["column"] == "AS15"

    def test_columns_param_parsed_and_forwarded(
        self, test_client, auth_header, mock_bigquery_dicts,
    ):
        resp = test_client.get(
            "/api/v1/sme/obligors",
            headers=auth_header,
            params={"columns": "dl_code,AS15"},
        )
        assert resp.status_code == 200
        cols = mock_bigquery_dicts.call_args.kwargs["columns"]
        assert "dl_code" in cols
        assert "AS15" in cols

    def test_invalid_filter_returns_400(self, test_client, auth_header):
        """A bad filter string should surface as HTTP 400, not 500."""
        resp = test_client.get(
            "/api/v1/sme/obligors",
            headers=auth_header,
            params={"filter": "BADCOLUMN:val"},
        )
        assert resp.status_code == 400
