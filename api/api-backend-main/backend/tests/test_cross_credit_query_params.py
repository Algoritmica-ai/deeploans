"""Regression tests for query-parameter handling across credit-type routers."""
from importlib import import_module

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient
from unittest.mock import AsyncMock, patch

from app.config import TABLES


CREDIT_TYPES = ["aut", "cmr", "cre", "les", "rmb", "sme"]


@pytest.mark.parametrize("credit_type", CREDIT_TYPES)
def test_common_query_params_forwarded_for_all_credit_types(credit_type):
    module = import_module(f"app.routes.{credit_type}")
    table_name = TABLES[credit_type][0]

    app = FastAPI()
    app.include_router(module.router, prefix=f"/api/v1/{credit_type}")

    with patch(
        "app.big_query_useful.query_to_list_of_dicts",
        new_callable=AsyncMock,
        return_value=[{"ok": True}],
    ) as mock_query:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(
                f"/api/v1/{credit_type}/{table_name}",
                params={"limit": 7, "offset": 3},
            )

    assert response.status_code == 200
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["credit_type"] == credit_type
    assert call_kwargs["table_name"] == table_name
    assert call_kwargs["limit"] == 7
    assert call_kwargs["offset"] == 3


def test_sme_router_forwards_filter_and_columns_params():
    app = FastAPI()
    sme_module = import_module("app.routes.sme")
    app.include_router(sme_module.router, prefix="/api/v1/sme")

    with patch(
        "app.big_query_useful.query_to_list_of_dicts",
        new_callable=AsyncMock,
        return_value=[{"dl_code": "TEST001"}],
    ) as mock_query:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(
                "/api/v1/sme/obligors",
                params={"filter": "AS15:pl", "columns": "dl_code,AS15"},
            )

    assert response.status_code == 200
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["filters"][0]["column"] == "AS15"
    assert call_kwargs["columns"] == ["dl_code", "AS15"]
