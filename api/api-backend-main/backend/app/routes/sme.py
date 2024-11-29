import functools
from typing import Optional

from fastapi import APIRouter, HTTPException, Header

from app import big_query_useful as useful
from app.config import TABLES
from app.filters.parser import parse_filter_query, parse_columns_query

router = APIRouter()
credit_type = 'sme'

tables = TABLES[credit_type]


async def get_result(
        table_name: str,
        filter: str = None,
        limit: int = None,
        offset: int = None,
        columns: str = None,
        detailed: bool = True,
        x_algoritmica_api_key: Optional[str] = Header(None) # needed for swagger
    ):
    try:
        filters = parse_filter_query(credit_type, table_name, filter) if filter else None
        cols = parse_columns_query(credit_type, table_name, columns) if columns else None

        params = dict(credit_type=credit_type,
                      table_name=table_name,
                      filters=filters,
                      limit=limit,
                      offset=offset,
                      columns=cols)

        result = await useful.query_to_list_of_dicts(
            **params) if detailed else await useful.query_to_list_of_lists(**params)

        return result
    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex))


for table in tables:
    # Method above requires table_name, but because add_api_route requires Callable I need to use partial
    obj = functools.partial(get_result, table_name=table)

    router.add_api_route(f"/{table}", obj, tags=[credit_type], methods=["GET"], name=table)
