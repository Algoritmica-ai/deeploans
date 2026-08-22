"""A restricted, vectorised expression evaluator.

Design specs contain formulas — ``original_balance / (oltomv_original / 100)``.
Those formulas arrive from files a user uploads, so they must never be handed to
:func:`eval`. This module walks the parsed syntax tree and evaluates only an
explicit whitelist of node types and functions. Anything else raises
:class:`ExpressionError`. There is no attribute access, no indexing, no calls to
anything not in :data:`FUNCTIONS`, and therefore no route to the interpreter
internals that make ``eval`` dangerous.

Everything evaluates *vectorised* over pandas Series, which is why Python's own
``and`` / ``or`` / ``if-else`` cannot be used directly — they force a single
truth value out of an array. They are rewritten here:

===========================  =========================
expression                   evaluated as
===========================  =========================
``a and b``                  ``a & b``
``a or b``                   ``a | b``
``not a``                    ``~a``
``x if cond else y``         ``numpy.where(cond, x, y)``
``a in [1, 2]``              ``numpy.isin(a, [1, 2])``
===========================  =========================
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd


class ExpressionError(ValueError):
    """An expression that is malformed, or uses something not allowed."""


# ---------------------------------------------------------------------------
# whitelist
# ---------------------------------------------------------------------------

_BIN_OPS: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_CMP_OPS: dict[type[ast.cmpop], Callable[[Any, Any], Any]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

_UNARY_OPS: dict[type[ast.unaryop], Callable[[Any], Any]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
    ast.Not: operator.invert,
}


def _clip(x: Any, lo: Any = None, hi: Any = None) -> Any:
    return np.clip(x, lo, hi)


def _coalesce(*args: Any) -> Any:
    """First non-null value, elementwise."""
    if not args:
        raise ExpressionError("coalesce() needs at least one argument")
    out = args[0]
    for nxt in args[1:]:
        out = np.where(pd.isna(out), nxt, out)
    return out


def _to_int(x: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.astype("int64")
    return np.asarray(x).astype("int64") if np.ndim(x) else int(x)


def _to_float(x: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.astype("float64")
    return np.asarray(x).astype("float64") if np.ndim(x) else float(x)


def _to_str(x: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.astype(str)
    return np.asarray(x).astype(str) if np.ndim(x) else str(x)


FUNCTIONS: dict[str, Callable[..., Any]] = {
    "abs": np.abs,
    "min": np.minimum,
    "max": np.maximum,
    "round": np.round,
    "floor": np.floor,
    "ceil": np.ceil,
    "clip": _clip,
    "log": np.log,
    "log10": np.log10,
    "exp": np.exp,
    "sqrt": np.sqrt,
    "power": np.power,
    "where": np.where,
    "isin": lambda x, vals: np.isin(x, list(vals)),
    "coalesce": _coalesce,
    "isnull": pd.isna,
    "notnull": pd.notna,
    "int": _to_int,
    "float": _to_float,
    "str": _to_str,
}

CONSTANTS: dict[str, Any] = {
    "True": True,
    "False": False,
    "None": None,
    "pi": np.pi,
    "e": np.e,
}


# ---------------------------------------------------------------------------
# name extraction (used by the spec loader to validate references)
# ---------------------------------------------------------------------------


def expression_names(expr: str) -> set[str]:
    """Every column/parameter name an expression reads.

    Function names and literal constants are excluded, so the result is exactly
    the set of things that must exist in the data frame.
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"cannot parse expression {expr!r}: {exc}") from exc

    called: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called.add(node.func.id)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    return {n for n in names - called if n not in CONSTANTS and n not in FUNCTIONS}


# ---------------------------------------------------------------------------
# evaluation
# ---------------------------------------------------------------------------


class _Evaluator(ast.NodeVisitor):
    def __init__(self, env: dict[str, Any], expr: str) -> None:
        self.env = env
        self.expr = expr

    def _fail(self, node: ast.AST, why: str) -> ExpressionError:
        return ExpressionError(f"{why} in expression {self.expr!r} (at {type(node).__name__})")

    # -- disallowed by omission: any node without a visit_* method lands here
    def generic_visit(self, node: ast.AST) -> Any:
        raise self._fail(
            node,
            f"{type(node).__name__} is not allowed — expressions may only use arithmetic, "
            "comparisons, boolean logic, conditionals, and whitelisted functions "
            f"({', '.join(sorted(FUNCTIONS))})",
        )

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in self.env:
            return self.env[node.id]
        if node.id in CONSTANTS:
            return CONSTANTS[node.id]
        raise self._fail(node, f"unknown name {node.id!r}")

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        fn = _BIN_OPS.get(type(node.op))
        if fn is None:
            raise self._fail(node, f"operator {type(node.op).__name__} is not allowed")
        return fn(self.visit(node.left), self.visit(node.right))

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        fn = _UNARY_OPS.get(type(node.op))
        if fn is None:
            raise self._fail(node, f"unary operator {type(node.op).__name__} is not allowed")
        value = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            # `not` on an array must become elementwise negation.
            return ~_as_bool(value)
        return fn(value)

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        # `and`/`or` short-circuit on a single truth value; vectorise instead.
        values = [_as_bool(self.visit(v)) for v in node.values]
        combine = operator.and_ if isinstance(node.op, ast.And) else operator.or_
        out = values[0]
        for v in values[1:]:
            out = combine(out, v)
        return out

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        result: Any = None
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            right = self.visit(comparator)
            if isinstance(op, ast.In | ast.NotIn):
                if not isinstance(right, list | tuple | set):
                    raise self._fail(node, "`in` requires a literal list on the right-hand side")
                cmp = _isin(left, list(right))
                if isinstance(op, ast.NotIn):
                    cmp = ~_as_bool(cmp)
            else:
                fn = _CMP_OPS.get(type(op))
                if fn is None:
                    raise self._fail(node, f"comparison {type(op).__name__} is not allowed")
                cmp = fn(left, right)
            result = cmp if result is None else (_as_bool(result) & _as_bool(cmp))
            left = right
        return result

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        cond = _as_bool(self.visit(node.test))
        body = self.visit(node.body)
        orelse = self.visit(node.orelse)
        if np.ndim(cond) == 0:
            return body if cond else orelse
        return np.where(cond, body, orelse)

    def visit_Call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name):
            raise self._fail(node, "only plain function calls are allowed (no attribute access)")
        fn = FUNCTIONS.get(node.func.id)
        if fn is None:
            raise self._fail(
                node,
                f"function {node.func.id!r} is not allowed "
                f"(available: {', '.join(sorted(FUNCTIONS))})",
            )
        if node.keywords:
            raise self._fail(node, "keyword arguments are not supported")
        return fn(*[self.visit(a) for a in node.args])

    def visit_List(self, node: ast.List) -> list[Any]:
        return [self.visit(e) for e in node.elts]

    def visit_Tuple(self, node: ast.Tuple) -> tuple[Any, ...]:
        return tuple(self.visit(e) for e in node.elts)

    def visit_Set(self, node: ast.Set) -> list[Any]:
        return [self.visit(e) for e in node.elts]


def _as_bool(value: Any) -> Any:
    """Coerce to something ``&``/``|``/``~`` work on elementwise."""
    if isinstance(value, pd.Series):
        return value.astype(bool)
    if isinstance(value, np.ndarray):
        return value.astype(bool)
    return bool(value)


def _isin(left: Any, values: list[Any]) -> Any:
    if isinstance(left, pd.Series):
        return left.isin(values)
    return np.isin(left, values)


def evaluate(expr: str, env: dict[str, Any]) -> Any:
    """Evaluate ``expr`` against ``env`` (column name -> Series/scalar)."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"cannot parse expression {expr!r}: {exc}") from exc
    return _Evaluator(env, expr).visit(tree)


def evaluate_on(expr: str, df: pd.DataFrame, extra: dict[str, Any] | None = None) -> Any:
    """Evaluate against a data frame's columns, plus optional extra names."""
    env: dict[str, Any] = {c: df[c] for c in df.columns}
    if extra:
        env.update(extra)
    return evaluate(expr, env)


def evaluate_mask(expr: str, df: pd.DataFrame, extra: dict[str, Any] | None = None) -> np.ndarray:
    """Evaluate to a boolean mask aligned with ``df``'s rows."""
    value = evaluate_on(expr, df, extra)
    if np.ndim(value) == 0:
        return np.full(len(df), bool(value), dtype=bool)
    return np.asarray(_as_bool(value), dtype=bool)
