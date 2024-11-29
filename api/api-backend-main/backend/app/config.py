from starlette.config import Config
from starlette.datastructures import Secret
import json

"""
FastAPI
"""

PROJECT_NAME = "Algoritmica"
VERSION = "0.1"
API_VERSION = 'v1'


class Filenames:
    tables_with_filter = 'app/files/tables_with_filters.json'


"""
Database
"""
config = Config(".env")

MONGO_USER = config("MONGO_INITDB_ROOT_USERNAME", cast=str)
MONGO_PASSWORD = config("MONGO_INITDB_ROOT_PASSWORD", cast=Secret)
MONGO_SERVER = config("MONGO_SERVER", cast=str)
MONGO_PORT = config("MONGO_PORT", cast=int)

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_SERVER}:{MONGO_PORT}"

MONGO_DB = 'api'

"""
Queries
"""

API_KEY_HEADER = 'x-algoritmica-api-key'
INVALID_API_KEY_MESSAGE = 'Invalid API key'

DEV_API_KEY = config("DEV_API_KEY", cast=str)

DEFAULT_LIMIT_OF_SQL_REQUESTED_ROWS = config("DEFAULT_LIMIT_OF_SQL_REQUESTED_ROWS", cast=int)
MAX_LIMIT_OF_SQL_REQUESTED_ROWS = config("MAX_LIMIT_OF_SQL_REQUESTED_ROWS", cast=int)
DEFAULT_CALLS_QUOTA = config("DEFAULT_CALLS_QUOTA", cast=int)

SHOULD_RETURN_DICT = True

"""
BIG QUERY
"""
CREDENTIALS_JSON = 'bigquery.json'

TABLES: dict[str, list[str]] = {'aut': ["bond_info", "bond_tranches", "deals", "leases", "nuts3_codes"],
                                'cmr': ["bond_info", "bond_tranches", "bond_transactions", "deals", "loans",
                                        "nuts3_codes", "performances"],
                                'cre': ["bond_info", "bond_tranches", "bond_transactions", "deals", "loans",
                                        "nuts3_codes"],
                                'les': ["bond_info", "bond_tranches", "bond_transactions", "collaterals", "deals",
                                        "features", "financials", "interests", "leases", "nace_codes", "nuts3_codes",
                                        "performances"],
                                'rmb': ["bond_collaterals", "bond_info", "bond_tranches", "bond_transactions",
                                        "borrowers", "collaterals", "deals", "loans", "nuts3_codes", "performances"],
                                'sme': ["amortisation_profiles", "bond_collaterals", "bond_info", "bond_tranches",
                                        "deals", "financials", "interests", "loan_collaterals", "loans", "nace_codes",
                                        "nuts3_codes", "obligors", "performances"]}

TABLES_SCHEMA = {}
with open(Filenames.tables_with_filter) as f:
    TABLES_SCHEMA = json.load(f)
ALLOWED_OPERATORS = {
    "DATE": [":", ">", "<", "<=", ">="],
    "BOOL": [":"],
    "FLOAT": [":", ">", "<", "<=", ">="],
    "INTEGER": [":", ">", "<", "<=", ">="],
    "STRING": [":", "~"],
}
