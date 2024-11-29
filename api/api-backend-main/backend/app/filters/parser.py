import re

from .validator import validate_column_name, validate_value, validate_operator
from app.config import TABLES_SCHEMA


class QueryParsingException(Exception):
    pass


class QueryValidationException(Exception):
    pass


OPERATORS = [":", "<", ">", "<=", ">=", "~"]


def parse_filter_query(credit_type: str,
                       table_name: str,
                       q: str) -> str:
    filters = []
    q = q.upper()
    filters_list = q.split(" ")

    if len(filters_list) % 2 == 0:
        raise QueryParsingException("Query is incomplete")

    for filter in filters_list:
        if filter == "OR" or filter == "AND":
            if filters:
                filters[-1]["logical_operator"] = f"{filter}"
                continue
            else:
                raise QueryParsingException("Query is incomplete")

        match = re.search(r"(?<=\w)(\<\=|\>\=|\:|\<|\>|\~)(?=(\%|\w|\d))", filter)
        if match:
            operator = match.group()
        else:
            raise QueryParsingException("Wrong operator used")

        column, operator, value = filter.partition(operator)

        if column.startswith("-"):
            column = column[1:]
            is_negated = True
        else:
            is_negated = False

        try:
            column, value_type = validate_column_name(column, credit_type, table_name)

            validate_operator(operator, value_type)

            value = validate_value(value, value_type)
        except Exception as e:
            raise QueryValidationException(e)
        
        operator = "=" if operator == ":" else operator
        operator = "LIKE" if operator == "~" else operator

        filters_record = {
            "is_negated": is_negated,
            "column": column,
            "operator": operator,
            "value": value,
            "value_type": value_type,
            "logical_operator": "",
        }
        filters.append(filters_record)

    return filters


def parse_columns_query(credit_type: str,
                        table_name: str,
                        columns: str) -> str:
    # Find all wanted columns
    columns_splitted = columns.strip(' ,').split(',')

    cols = [c.strip() for c in columns_splitted] # Remove possible empty spaces

    # Make sure all columns are part of the database table
    # TODO - Make comparison case insensitive
    for c in cols:
        if c not in TABLES_SCHEMA[credit_type][table_name].keys():
            raise QueryValidationException(f'Column {c} is not part of {credit_type} - {table_name}')

    return ', '.join(cols)
