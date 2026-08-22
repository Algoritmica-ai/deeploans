"""Expression evaluator: it must compute correctly and refuse everything else.

The refusal half matters as much as the arithmetic half — specs are uploaded
files, and this module is the only thing standing between a spec and `eval`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.generate.deriver import (
    ExpressionError,
    evaluate,
    evaluate_mask,
    evaluate_on,
    expression_names,
)


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "balance": [100.0, 200.0, 300.0],
            "rate": [1.0, 2.0, 3.0],
            "flag": ["Y", "N", "Y"],
            "state": ["Performing", "Late", "Performing"],
        }
    )


# ---------------------------------------------------------------------------
# arithmetic
# ---------------------------------------------------------------------------


def test_arithmetic(df):
    got = evaluate_on("balance / (rate / 100)", df)
    np.testing.assert_allclose(got, [10000.0, 10000.0, 10000.0])


def test_operator_coverage(df):
    assert list(evaluate_on("balance + rate", df)) == [101.0, 202.0, 303.0]
    assert list(evaluate_on("balance - rate", df)) == [99.0, 198.0, 297.0]
    assert list(evaluate_on("balance * 2", df)) == [200.0, 400.0, 600.0]
    assert list(evaluate_on("balance // 30", df)) == [3.0, 6.0, 10.0]
    assert list(evaluate_on("balance % 7", df)) == [2.0, 4.0, 6.0]
    np.testing.assert_allclose(evaluate_on("power(2, rate)", df), [2.0, 4.0, 8.0])
    assert list(evaluate_on("-rate", df)) == [-1.0, -2.0, -3.0]


def test_functions(df):
    np.testing.assert_allclose(evaluate_on("max(rate, 2)", df), [2.0, 2.0, 3.0])
    np.testing.assert_allclose(evaluate_on("min(rate, 2)", df), [1.0, 2.0, 2.0])
    np.testing.assert_allclose(evaluate_on("clip(balance, 150, 250)", df), [150.0, 200.0, 250.0])
    np.testing.assert_allclose(evaluate_on("ceil(rate / 2)", df), [1.0, 1.0, 2.0])
    assert list(evaluate_on("int(balance / 3)", df)) == [33, 66, 100]


def test_params_are_visible_alongside_columns(df):
    got = evaluate_on("balance * factor", df, {"factor": 2})
    assert list(got) == [200.0, 400.0, 600.0]


# ---------------------------------------------------------------------------
# vectorised boolean logic — the part Python's own operators get wrong
# ---------------------------------------------------------------------------


def test_and_or_not_are_elementwise(df):
    got = evaluate_on("(balance > 150) and (rate < 3)", df)
    assert list(np.asarray(got)) == [False, True, False]

    got = evaluate_on("(balance > 250) or (rate < 2)", df)
    assert list(np.asarray(got)) == [True, False, True]

    got = evaluate_on("not (flag == 'Y')", df)
    assert list(np.asarray(got)) == [False, True, False]


def test_membership(df):
    got = evaluate_on("state in ['Late', 'Defaulted']", df)
    assert list(np.asarray(got)) == [False, True, False]

    got = evaluate_on("state not in ['Late']", df)
    assert list(np.asarray(got)) == [True, False, True]


def test_ternary_is_vectorised(df):
    got = evaluate_on("balance if flag == 'Y' else 0.0", df)
    np.testing.assert_allclose(np.asarray(got, dtype=float), [100.0, 0.0, 300.0])


def test_chained_comparison(df):
    got = evaluate_on("100 < balance < 300", df)
    assert list(np.asarray(got)) == [False, True, False]


def test_scalar_ternary_stays_scalar():
    assert evaluate("1 if True else 2", {}) == 1


def test_evaluate_mask_broadcasts_a_constant(df):
    assert list(evaluate_mask("True", df)) == [True, True, True]


# ---------------------------------------------------------------------------
# refusals — the security surface
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr",
    [
        "__import__('os').system('echo hi')",  # the classic eval escape
        "balance.__class__",  # attribute access
        "().__class__.__bases__",  # type traversal
        "open('/etc/passwd')",  # non-whitelisted call
        "[x for x in balance]",  # comprehension
        "lambda: 1",  # lambda
        "balance[0]",  # subscripting
        "eval('1+1')",
        "exec('x=1')",
        "globals()",
        "balance := 5",  # walrus assignment
    ],
)
def test_unsafe_expressions_are_refused(expr, df):
    with pytest.raises(ExpressionError):
        evaluate_on(expr, df)


def test_unknown_name_is_refused(df):
    with pytest.raises(ExpressionError, match="unknown name"):
        evaluate_on("balance * nonexistent", df)


def test_syntax_error_is_reported_with_the_expression(df):
    with pytest.raises(ExpressionError, match="cannot parse"):
        evaluate_on("balance +", df)


def test_keyword_arguments_are_refused(df):
    with pytest.raises(ExpressionError, match="keyword"):
        evaluate_on("round(balance, decimals=2)", df)


# ---------------------------------------------------------------------------
# name extraction (drives the spec loader's reference checking)
# ---------------------------------------------------------------------------


def test_expression_names_excludes_functions_and_constants():
    assert expression_names("max(balance, floor_value) + 1") == {"balance", "floor_value"}
    assert expression_names("True if flag == 'Y' else False") == {"flag"}
    assert expression_names("pi * radius") == {"radius"}


def test_expression_names_sees_both_ternary_branches():
    assert expression_names("a if c else b") == {"a", "b", "c"}
