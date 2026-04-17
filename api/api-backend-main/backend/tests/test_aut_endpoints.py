"""
Integration tests for AUT API endpoints using FastAPI's TestClient.

These tests mirror SME route behavior checks and verify that AUT routes are
registered and support the shared query parameter contract.
"""

from app.config import TABLES, TABLES_SCHEMA


class TestAutRouteDiscovery:
    AUT_TABLES = TABLES["aut"]

    def test_all_aut_tables_have_routes(self, test_client, auth_header):
        for table in self.AUT_TABLES:
            resp = test_client.get(f"/api/v1/aut/{table}", headers=auth_header)
            assert resp.status_code != 404, f"Missing route for /api/v1/aut/{table}"


class TestAutQueryBehavior:
    TABLE = "deals"

    def _first_column(self):
        return next(iter(TABLES_SCHEMA["aut"][self.TABLE].keys()))

    def test_limit_and_offset_forwarded(self, test_client, auth_header, mock_bigquery_dicts):
        test_client.get(
            f"/api/v1/aut/{self.TABLE}",
            headers=auth_header,
            params={"limit": 7, "offset": 3},
        )
        kwargs = mock_bigquery_dicts.call_args.kwargs
        assert kwargs["limit"] == 7
        assert kwargs["offset"] == 3

    def test_columns_param_is_forwarded(self, test_client, auth_header, mock_bigquery_dicts):
        col = self._first_column()
        resp = test_client.get(
            f"/api/v1/aut/{self.TABLE}",
            headers=auth_header,
            params={"columns": col},
        )
        assert resp.status_code == 200
        assert mock_bigquery_dicts.call_args.kwargs["columns"] == col

    def test_detailed_false_uses_list_mode(self, test_client, auth_header, mock_bigquery_lists):
        resp = test_client.get(
            f"/api/v1/aut/{self.TABLE}",
            headers=auth_header,
            params={"detailed": "false"},
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
        assert isinstance(resp.json()[0], list)
