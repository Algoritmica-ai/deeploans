"""
Unit tests for app.filters.validator — type coercion and validation helpers.

Covers:
  - sql_to_python_type: every supported SQL type + unknown type
  - validate_column_name: valid filterable, valid non-filterable, missing
  - validate_value: valid coercions + invalid values
  - validate_operator: allowed vs disallowed operator/type combos
"""
import pytest
from datetime import date

from app.filters.validator import (
    sql_to_python_type,
    validate_column_name,
    validate_value,
    validate_operator,
)


# ── sql_to_python_type ──────────────────────────────────────────────────────


class TestSqlToPythonType:
    """Verify coercion from stringified SQL values to Python objects."""

    def test_date_conversion(self):
        result = sql_to_python_type("2023-06-15", "DATE")
        assert result == date(2023, 6, 15)

    def test_bool_true(self):
        assert sql_to_python_type("TRUE", "BOOL") is True

    def test_bool_false(self):
        assert sql_to_python_type("FALSE", "BOOL") is False

    def test_float_conversion(self):
        result = sql_to_python_type("3.14", "FLOAT")
        assert result == pytest.approx(3.14)

    def test_integer_conversion(self):
        result = sql_to_python_type("42", "INTEGER")
        assert result == 42
        assert isinstance(result, int)

    def test_string_passthrough(self):
        assert sql_to_python_type("HELLO", "STRING") == "HELLO"

    def test_unknown_type_returns_none(self):
        """An unrecognised SQL type should return None."""
        assert sql_to_python_type("anything", "UNKNOWN_TYPE") is None

    def test_float_negative(self):
        result = sql_to_python_type("-99.5", "FLOAT")
        assert result == pytest.approx(-99.5)

    def test_integer_zero(self):
        assert sql_to_python_type("0", "INTEGER") == 0


# ── validate_column_name ────────────────────────────────────────────────────


class TestValidateColumnName:
    """Verify column lookup against the schema loaded from JSON."""

    CREDIT = "sme"
    TABLE = "obligors"

    def test_valid_filterable_column(self):
        col, vtype = validate_column_name("AS15", self.CREDIT, self.TABLE)
        assert col == "AS15"
        assert vtype == "STRING"

    def test_column_match_is_case_insensitive_on_input(self):
        """Parser uppercases input before calling validator, so this works."""
        col, vtype = validate_column_name("DL_CODE", self.CREDIT, self.TABLE)
        assert vtype == "STRING"

    def test_non_filterable_column_raises(self):
        """AS17 exists but is_filter is False."""
        with pytest.raises(Exception, match="not filterable"):
            validate_column_name("AS17", self.CREDIT, self.TABLE)

    def test_nonexistent_column_raises(self):
        with pytest.raises(Exception, match="Wrong column name"):
            validate_column_name("ZZZZZ", self.CREDIT, self.TABLE)

    def test_boolean_column_returns_BOOL(self):
        """BOOLEAN in schema is normalised to BOOL for GoogleSQL compat."""
        col, vtype = validate_column_name("BS12", self.CREDIT, "bond_collaterals")
        assert vtype == "BOOL"

    def test_date_column(self):
        col, vtype = validate_column_name("AS1", self.CREDIT, self.TABLE)
        assert vtype == "DATE"


# ── validate_value ──────────────────────────────────────────────────────────


class TestValidateValue:
    """Verify that validate_value coerces or rejects values correctly."""

    def test_valid_string(self):
        assert validate_value("HELLO", "STRING") == "HELLO"

    def test_valid_integer(self):
        assert validate_value("7", "INTEGER") == 7

    def test_valid_float(self):
        assert validate_value("1.5", "FLOAT") == pytest.approx(1.5)

    def test_invalid_float_raises(self):
        with pytest.raises(Exception):
            validate_value("not_a_number", "FLOAT")

    def test_invalid_integer_raises(self):
        with pytest.raises(Exception):
            validate_value("abc", "INTEGER")

    def test_invalid_date_raises(self):
        with pytest.raises(Exception):
            validate_value("not-a-date", "DATE")

    def test_unknown_type_raises(self):
        """Unknown types make sql_to_python_type return None → 'Wrong value'."""
        with pytest.raises(Exception, match="Wrong value"):
            validate_value("anything", "IMAGINARY_TYPE")


# ── validate_operator ───────────────────────────────────────────────────────


class TestValidateOperator:
    """Ensure operator/type combos are checked against ALLOWED_OPERATORS."""

    # -- allowed combinations --
    @pytest.mark.parametrize("op", [":", ">", "<", "<=", ">="])
    def test_numeric_operators_allowed_for_integer(self, op):
        assert validate_operator(op, "INTEGER") is True

    @pytest.mark.parametrize("op", [":", ">", "<", "<=", ">="])
    def test_numeric_operators_allowed_for_float(self, op):
        assert validate_operator(op, "FLOAT") is True

    @pytest.mark.parametrize("op", [":", ">", "<", "<=", ">="])
    def test_numeric_operators_allowed_for_date(self, op):
        assert validate_operator(op, "DATE") is True

    def test_equality_allowed_for_bool(self):
        assert validate_operator(":", "BOOL") is True

    @pytest.mark.parametrize("op", [":", "~"])
    def test_string_operators(self, op):
        assert validate_operator(op, "STRING") is True

    # -- disallowed combinations --
    def test_greater_than_disallowed_for_string(self):
        with pytest.raises(Exception, match="Wrong operator"):
            validate_operator(">", "STRING")

    def test_like_disallowed_for_integer(self):
        with pytest.raises(Exception, match="Wrong operator"):
            validate_operator("~", "INTEGER")

    def test_greater_than_disallowed_for_bool(self):
        with pytest.raises(Exception, match="Wrong operator"):
            validate_operator(">", "BOOL")
