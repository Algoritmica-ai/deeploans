from datetime import datetime
from ..config import TABLES_SCHEMA, ALLOWED_OPERATORS


def sql_to_python_type(value, type):
    result = None
    if type == "DATE":
        result = datetime.strptime(value, "%Y-%m-%d").date()
    elif type == "BOOL":
        result = True if value == "TRUE" else False
    elif type == "FLOAT":
        result = float(value)
    elif type == "INTEGER":
        result = int(value)
    elif type == "STRING":
        result = str(value)
    return result


def validate_column_name(column: str, credit_type: str, table_name: str) -> str:
    table: dict[str, dict] = TABLES_SCHEMA[credit_type][table_name]

    # User spelled column wrong
    for k in table.keys():
        if column == k.upper():  # Column match
            if table[k]['is_filter']:  # Match AND is filter
                # thats some inconsistency between GoogleSQL and plain sql names
                value_type = table[k]["type"] if table[k]["type"] != "BOOLEAN" else "BOOL"
                return k, value_type
            else:  # Match BUT no filter
                raise Exception(f"Column {k} is not filterable")  # I use k instead of column to keep Big Query case

    raise Exception("Wrong column name")


def validate_value(value, value_type):
    value = sql_to_python_type(value, value_type)

    if value is not None:
        return value
    else:
        raise Exception("Wrong value")


def validate_operator(operator, value_type):
    if value_type in ALLOWED_OPERATORS.keys() and operator in ALLOWED_OPERATORS[value_type]:
        return True
    raise Exception("Wrong operator for given column type")
