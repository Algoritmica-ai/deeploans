"""
Shared test configuration and fixtures.

Sets up environment variables and working directory so that app.config
can be imported without a real .env file or external services.
"""
import os
import sys

# ---------------------------------------------------------------------------
# Bootstrap: must run before any `app.*` import so that app.config can read
# its required environment variables and find its JSON schema file.
# ---------------------------------------------------------------------------

_backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Ensure the backend package root is importable
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

# app/config.py opens 'app/files/tables_with_filters.json' with a relative
# path, so the working directory must be the backend root.
os.chdir(_backend_dir)

# Provide dummy values for every env var that app.config reads at import time.
# These are never used to connect to real services; they just prevent
# starlette.config.Config from raising on missing keys.
_test_env = {
    "MONGO_INITDB_ROOT_USERNAME": "test_user",
    "MONGO_INITDB_ROOT_PASSWORD": "test_password",
    "MONGO_SERVER": "localhost",
    "MONGO_PORT": "27017",
    "DEV_API_KEY": "test-dev-key",
    "DEFAULT_LIMIT_OF_SQL_REQUESTED_ROWS": "100",
    "MAX_LIMIT_OF_SQL_REQUESTED_ROWS": "1000",
    "DEFAULT_CALLS_QUOTA": "100",
}
for key, value in _test_env.items():
    os.environ.setdefault(key, value)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.fixture()
def dev_api_key():
    """Return the DEV API key used to bypass authentication middleware."""
    return os.environ["DEV_API_KEY"]


@pytest.fixture()
def auth_header(dev_api_key):
    """Return headers dict that passes the authentication middleware."""
    return {"x-algoritmica-api-key": dev_api_key}


@pytest.fixture()
def test_client(auth_header):
    """
    Provide a FastAPI TestClient with external services (BigQuery, MongoDB)
    mocked out.  Uses the DEV_API_KEY so requests bypass quota checks.
    """
    with patch("app.server_startup_tasks._startup_tasks", new_callable=AsyncMock):
        with patch("app.server_startup_tasks._shutdown_tasks", new_callable=AsyncMock):
            from starlette.testclient import TestClient
            from app.server import app

            with TestClient(app, raise_server_exceptions=False) as client:
                yield client


@pytest.fixture()
def mock_bigquery_dicts():
    """Mock big_query_useful.query_to_list_of_dicts to return sample rows."""
    sample_rows = [
        {"dl_code": "TEST001", "AS3": "VALUE1"},
        {"dl_code": "TEST002", "AS3": "VALUE2"},
    ]
    with patch(
        "app.big_query_useful.query_to_list_of_dicts",
        new_callable=AsyncMock,
        return_value=sample_rows,
    ) as mock:
        yield mock


@pytest.fixture()
def mock_bigquery_lists():
    """Mock big_query_useful.query_to_list_of_lists to return sample rows."""
    sample_rows = [
        ("TEST001", "VALUE1"),
        ("TEST002", "VALUE2"),
    ]
    with patch(
        "app.big_query_useful.query_to_list_of_lists",
        new_callable=AsyncMock,
        return_value=sample_rows,
    ) as mock:
        yield mock
