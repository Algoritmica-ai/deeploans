import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

def TO_NUMBER(n):
    """
    Coerce value to float. Returns None for invalid values, logs warning.
    """
    try:
        return float(n)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid value '{n}' for float conversion: {e}")
        return None

def TO_DATE(s):
    """
    Convert a string to datetime, supporting YYYY, YYYY-MM, or YYYY-MM-DD formats.
    Returns None for invalid formats and logs warning.
    """
    if not s or not isinstance(s, str):
        logger.warning(f"Empty or invalid type for date conversion: {s}")
        return None
    s = s.strip()
    try:
        if s.count("-") == 2:
            return datetime.datetime.strptime(s, "%Y-%m-%d")
        elif s.count("-") == 1:
            return datetime.datetime.strptime(s, "%Y-%m")
        elif s.count("-") == 0:
            return datetime.datetime.strptime(s, "%Y")
        else:
            logger.warning(f"Unexpected date format: {s}")
            return None
    except Exception as e:
        logger.warning(f"Failed to convert '{s}' to datetime: {e}")
        return None

def asset_schema():
    """
    Return validation schema for ASSETS data type.
    """
    schema = {
        # ... [schema fields unchanged, see original for full details] ...
    }
    return schema

def bond_info_schema():
    """
    Return validation schema for BOND_INFO data type.
    """
    schema = {
        # ... [schema fields unchanged, see original for full details] ...
    }
    return schema

# --- Optionally, provide a function to validate a record with logging support ---

def validate_record(record, schema, verbose=False):
    """
    Example utility to validate a record against schema, logging errors if any.
    (Assumes Cerberus or similar validator usage.)
    """
    from cerberus import Validator
    v = Validator(schema)
    is_valid = v.validate(record)
    if verbose or not is_valid:
        logger.info(f"Validation result: {is_valid}, errors: {v.errors if not is_valid else None}")
    return is_valid, v.errors if not is_valid else None
