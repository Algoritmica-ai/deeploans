"""
Unit tests for app.filters.parser — parse_filter_query & parse_columns_query.

Covers:
  - single / multi-filter queries with all supported operators
  - negation prefix (-)
  - logical operators (AND, OR)
  - operator-to-SQL translation (: → =, ~ → LIKE)
  - input uppercasing behaviour
  - malformed queries (incomplete, bad operator, wrong column)
  - parse_columns_query happy path, invalid column, whitespace handling
"""
import pytest

from app.filters.parser import (
    parse_filter_query,
    parse_columns_query,
    QueryParsingException,
    QueryValidationException,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

CREDIT = "sme"
TABLE = "obligors"          # STRING-heavy table used by the existing test
AMORT_TABLE = "amortisation_profiles"  # has DATE and FLOAT filterable columns


# ── parse_filter_query — happy paths ─────────────────────────────────────────


class TestParseFilterQueryHappyPaths:
    """Valid filter queries that should parse without error."""

    def test_single_equality_filter(self):
        result = parse_filter_query(CREDIT, TABLE, "AS15:pl")
        assert len(result) == 1
        assert result[0]["column"] == "AS15"
        assert result[0]["operator"] == "="
        assert result[0]["value"] == "PL"
        assert result[0]["is_negated"] is False
        assert result[0]["logical_operator"] == ""

    def test_negated_filter(self):
        result = parse_filter_query(CREDIT, TABLE, "-AS15:pl")
        assert result[0]["is_negated"] is True
        assert result[0]["column"] == "AS15"

    def test_two_filters_with_and(self):
        result = parse_filter_query(CREDIT, TABLE, "-as15:pl and AS26:3")
        assert len(result) == 2
        assert result[0]["logical_operator"] == "AND"
        assert result[1]["logical_operator"] == ""

    def test_two_filters_with_or(self):
        result = parse_filter_query(CREDIT, TABLE, "AS15:pl or AS26:3")
        assert len(result) == 2
        assert result[0]["logical_operator"] == "OR"

    def test_three_filters_chained(self):
        result = parse_filter_query(CREDIT, TABLE, "AS15:pl and AS26:3 or AS4:x")
        assert len(result) == 3
        assert result[0]["logical_operator"] == "AND"
        assert result[1]["logical_operator"] == "OR"
        assert result[2]["logical_operator"] == ""

    def test_like_operator_translates_to_LIKE(self):
        result = parse_filter_query(CREDIT, TABLE, "AS15~%test%")
        assert result[0]["operator"] == "LIKE"
        assert result[0]["value"] == "%TEST%"

    def test_greater_than_operator(self):
        """FLOAT column in amortisation_profiles supports > operator."""
        result = parse_filter_query(CREDIT, AMORT_TABLE, "DOUBLE_VALUE>1.5")
        assert result[0]["operator"] == ">"
        assert result[0]["value"] == 1.5
        assert result[0]["value_type"] == "FLOAT"

    def test_less_than_operator(self):
        result = parse_filter_query(CREDIT, AMORT_TABLE, "DOUBLE_VALUE<100")
        assert result[0]["operator"] == "<"

    def test_greater_equal_operator(self):
        result = parse_filter_query(CREDIT, AMORT_TABLE, "DOUBLE_VALUE>=0")
        assert result[0]["operator"] == ">="

    def test_less_equal_operator(self):
        result = parse_filter_query(CREDIT, AMORT_TABLE, "DOUBLE_VALUE<=999")
        assert result[0]["operator"] == "<="

    def test_date_filter(self):
        result = parse_filter_query(CREDIT, AMORT_TABLE, "DATE_VALUE:2023-01-15")
        assert result[0]["value_type"] == "DATE"

    def test_input_is_uppercased(self):
        """Parser uppercases the entire query string."""
        result = parse_filter_query(CREDIT, TABLE, "as15:hello")
        assert result[0]["column"] == "AS15"
        assert result[0]["value"] == "HELLO"

    def test_return_type_is_list(self):
        """Regression: parse_filter_query must return list[dict], not str."""
        result = parse_filter_query(CREDIT, TABLE, "AS15:pl")
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)


# ── parse_filter_query — error cases ────────────────────────────────────────


class TestParseFilterQueryErrors:
    """Malformed queries that should raise exceptions."""

    def test_incomplete_query_even_tokens(self):
        """An even number of space-separated tokens is always incomplete."""
        with pytest.raises(QueryParsingException, match="incomplete"):
            parse_filter_query(CREDIT, TABLE, "AS15:pl and")

    def test_starts_with_logical_operator(self):
        with pytest.raises(QueryParsingException, match="incomplete"):
            parse_filter_query(CREDIT, TABLE, "and AS15:pl")

    def test_missing_operator(self):
        with pytest.raises(QueryParsingException, match="operator"):
            parse_filter_query(CREDIT, TABLE, "AS15pl")

    def test_invalid_column_name(self):
        with pytest.raises(QueryValidationException):
            parse_filter_query(CREDIT, TABLE, "NONEXISTENT:val")

    def test_non_filterable_column(self):
        """AS17 exists in obligors but has is_filter=false."""
        with pytest.raises(QueryValidationException):
            parse_filter_query(CREDIT, TABLE, "AS17:val")

    def test_wrong_operator_for_type(self):
        """STRING columns only allow : and ~, not >."""
        with pytest.raises(QueryValidationException):
            parse_filter_query(CREDIT, TABLE, "AS15>val")


# ── parse_columns_query ─────────────────────────────────────────────────────


class TestParseColumnsQuery:
    """Tests for the column-selection parser."""

    def test_single_column(self):
        result = parse_columns_query(CREDIT, TABLE, "dl_code")
        assert result == "dl_code"

    def test_multiple_columns(self):
        result = parse_columns_query(CREDIT, TABLE, "dl_code,AS15,AS26")
        assert result == "dl_code, AS15, AS26"

    def test_strips_whitespace(self):
        result = parse_columns_query(CREDIT, TABLE, "  dl_code , AS15 ")
        assert result == "dl_code, AS15"

    def test_invalid_column_raises(self):
        with pytest.raises(QueryValidationException):
            parse_columns_query(CREDIT, TABLE, "NOT_A_COLUMN")

    def test_one_invalid_among_valid_raises(self):
        with pytest.raises(QueryValidationException):
            parse_columns_query(CREDIT, TABLE, "dl_code,FAKE")
