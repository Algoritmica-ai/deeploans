import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, "mcp-server")
from deeploans_mcp import server


class LocalDeeploansAPI:
    def __init__(self):
        self.requests = []
        self._httpd = None
        self._thread = None

    def __enter__(self):
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                parent.requests.append(
                    {
                        "path": parsed.path,
                        "query": parse_qs(parsed.query),
                        "headers": dict(self.headers),
                    }
                )

                if parsed.path == "/openapi.json":
                    self._send_json(
                        {
                            "openapi": "3.1.0",
                            "info": {"title": "Local Deeploans Test API", "version": "test"},
                            "paths": {"/api/v1/sme/loans": {"get": {}}},
                        }
                    )
                    return

                if parsed.path == "/api/v1/sme/loans":
                    self._send_json(
                        {
                            "data": [
                                {"loan_id": "loan-1", "principal_balance": 1000},
                                {"loan_id": "loan-2", "principal_balance": 2500},
                            ]
                        }
                    )
                    return

                if parsed.path == "/api/v1/sme/erroring_loans":
                    self._send_json({"error": "backend unavailable"}, status=500)
                    return

                self._send_json({"error": "not found"}, status=404)

            def log_message(self, format, *args):
                pass

            def _send_json(self, payload, status=200):
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)

    @property
    def base_url(self):
        host, port = self._httpd.server_address
        return f"http://{host}:{port}"


class TestMCPToAPISmoke(unittest.TestCase):
    def test_fetch_api_docs_uses_configured_local_base_url(self):
        with LocalDeeploansAPI() as api:
            result = server.fetch_api_docs(base_url=api.base_url)

        self.assertTrue(result["ok"])
        self.assertEqual(result["source"], "api")
        self.assertEqual(result["title"], "Local Deeploans Test API")
        self.assertEqual(result["url"], f"{api.base_url}/openapi.json")
        self.assertEqual(api.requests[0]["path"], "/openapi.json")

    def test_sample_rows_calls_configured_local_api_path_and_query(self):
        with LocalDeeploansAPI() as api:
            result = server.sample_rows("sme", "loans", limit=2, base_url=api.base_url)

        self.assertTrue(result["ok"])
        self.assertEqual(result["url"], f"{api.base_url}/api/v1/sme/loans?limit=2&offset=0")
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["sample"][0]["loan_id"], "loan-1")
        self.assertEqual(api.requests[-1]["path"], "/api/v1/sme/loans")
        self.assertEqual(api.requests[-1]["query"], {"limit": ["2"], "offset": ["0"]})

    def test_sample_rows_preserves_http_error_url_and_details(self):
        with LocalDeeploansAPI() as api:
            result = server.sample_rows("sme", "erroring_loans", limit=2, base_url=api.base_url)

        self.assertFalse(result["ok"])
        self.assertEqual(result["url"], f"{api.base_url}/api/v1/sme/erroring_loans?limit=2&offset=0")
        self.assertEqual(result["error"], "HTTP 500")
        self.assertIn("backend unavailable", result["details"])
        self.assertEqual(api.requests[-1]["path"], "/api/v1/sme/erroring_loans")


if __name__ == "__main__":
    unittest.main()
