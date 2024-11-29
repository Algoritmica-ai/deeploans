"""
SQL - there are 5 parameters we need to know:
SELECT - * for all columns
FROM - name of the table: 'algoritmica_sme_silver.loans
    Credit type (aut, cmr, sme) depends from endpoint
    Table name (laons, bond_info etc) also depends from endpoint
WHERE - filters on query
LIMIT - max number of rows in result
OFFSET - for pagination

"""
import sys
from app.config import MAX_LIMIT_OF_SQL_REQUESTED_ROWS, DEFAULT_LIMIT_OF_SQL_REQUESTED_ROWS
from app.memory import memory

from fastapi import HTTPException
from google.cloud import bigquery


# Create query
def create_sql_statement(credit_type: str,
                         table_name: str,
                         filters: list | None,
                         limit: int | None,
                         offset: int | None,
                         columns: str | None) -> tuple:
    dataset_name = f"algoritmica_{credit_type}_silver.{table_name}"

    # Check if user set limit higher than allowed
    req_limit = min(limit, MAX_LIMIT_OF_SQL_REQUESTED_ROWS) if limit else DEFAULT_LIMIT_OF_SQL_REQUESTED_ROWS

    # Determine columns
    select_statement = columns if columns else '*'

    if filters:
        where_query_part = "WHERE " + "".join([
            f"{'NOT ' if filter['is_negated'] else ''}{'UPPER(' if filter['operator'] == 'LIKE' else ''}{filter['column']}{')' if filter['operator'] == 'LIKE' else ''} {filter['operator']} ?{' ' + filter['logical_operator'] + ' ' if filter['logical_operator'] else ''}"
            for filter in filters
        ]
        )
        query = f"SELECT {select_statement} FROM {dataset_name} {where_query_part} LIMIT {req_limit} OFFSET {offset if offset else 0}"

    else:
        filters = []  # Otherwise QueryJobConfig fails
        query = f"SELECT {select_statement} FROM {dataset_name} LIMIT {req_limit} OFFSET {offset if offset else 0}"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, filter["value_type"], filter["value"]) for filter in filters
        ]
    )
    return query, job_config


async def query_to_list_of_dicts(credit_type: str,
                                 table_name: str,
                                 filters: list = None,
                                 limit: int = None,
                                 offset: int = None,
                                 columns: str = None) -> list:
    q, job_config = create_sql_statement(credit_type=credit_type,
                                         table_name=table_name,
                                         filters=filters,
                                         limit=limit,
                                         offset=offset,
                                         columns=columns)

    query_job = memory.big_query_client.query(q, job_config=job_config)
    rows = query_job.result()

    columns = ["{0}".format(schema.name) for schema in rows.schema]

    result = []

    for row in rows:
        row_values = row.values()
        d = {}
        for index, col in enumerate(columns):
            d[col] = row_values[index]
        result.append(d)

    return result


async def query_to_list_of_lists(credit_type: str,
                                 table_name: str,
                                 filters: list = None,
                                 limit: int = None,
                                 offset: int = None,
                                 columns: str = None) -> list:
    q, job_config = create_sql_statement(credit_type=credit_type,
                                         table_name=table_name,
                                         filters=filters,
                                         limit=limit,
                                         offset=offset,
                                         columns=columns)

    query_job = memory.big_query_client.query(q, job_config=job_config)
    rows = query_job.result()

    result = []

    for row in rows:
        result.append(row.values())

    return result


async def custom_query_to_list_of_dicts(q: str) -> list:
    # Unused - Only use for debug purpose - not in production
    try:
        query_job = memory.big_query_client.query(q)
        rows = query_job.result()

        columns = ["{0}".format(schema.name) for schema in rows.schema]

        result = []

        for row in rows:
            row_values = row.values()
            d = {}
            for index, col in enumerate(columns):
                d[col] = row_values[index]
            result.append(d)

        return result
    except Exception as ex:
        raise HTTPException(status_code=400, detail='Invalid query syntax')
