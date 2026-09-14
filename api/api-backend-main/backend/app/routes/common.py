import functools
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from app import big_query_useful as useful
from app.config import SHOULD_RETURN_DICT, TABLES
from app.filters.parser import parse_columns_query, parse_filter_query


def build_credit_router(credit_type: str) -> APIRouter:
    router = APIRouter()
    tables = TABLES[credit_type]

    async def get_result(
        table_name: str,
        filter: str = None,
        limit: int = None,
        offset: int = None,
        columns: str = None,
        detailed: Optional[bool] = None,
        x_algoritmica_api_key: Optional[str] = Header(None),  # needed for swagger
    ):
        try:
            filters = parse_filter_query(credit_type, table_name, filter) if filter else None
            cols = parse_columns_query(credit_type, table_name, columns) if columns else None

            params = dict(
                credit_type=credit_type,
                table_name=table_name,
                filters=filters,
                limit=limit,
                offset=offset,
                columns=cols,
            )

            should_return_dict = SHOULD_RETURN_DICT if detailed is None else detailed
            result = await useful.query_to_list_of_dicts(**params) if should_return_dict else await useful.query_to_list_of_lists(**params)
            return result
        except Exception as ex:
            raise HTTPException(status_code=400, detail=str(ex))

    for table in tables:
        obj = functools.partial(get_result, table_name=table)
        router.add_api_route(f"/{table}", obj, tags=[credit_type], methods=["GET"], name=table)

    return router
