"""Invariant checks, generated from the spec.

Upstream shipped 53 SQL assertions that named its 71 columns literally, so they
only ever validated one deal. Here the same guarantees are *derived* from the
spec: if a column is declared ``static``, a check appears asserting it never
changes; if a state is declared ``terminal``, a check appears asserting nothing
survives it. Point the engine at a different asset class and the checks follow.

DuckDB does the work, against the consolidated panel, because these are all
group-by-and-compare questions over millions of rows.

Each check returns the *violating rows*. Zero rows means it passed — which also
means a check can genuinely fail, and the negative-control fixtures in
``tests/test_invariants.py`` prove that it does.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from sdd.spec.schema import DesignSpec

MAX_SAMPLE_ROWS = 5


@dataclass
class CheckResult:
    name: str
    description: str
    passed: bool
    violations: int = 0
    sample: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "passed": self.passed,
            "violations": self.violations,
            "sample": self.sample,
            "error": self.error,
        }


@dataclass
class ValidationReport:
    spec_name: str
    checks: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed]

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec": self.spec_name,
            "passed": self.passed,
            "total": len(self.checks),
            "failed": len(self.failures),
            "checks": [c.to_dict() for c in self.checks],
        }

    def summary(self) -> str:
        head = f"{len(self.checks) - len(self.failures)}/{len(self.checks)} checks passed"
        if self.passed:
            return f"{head} — panel is consistent with the spec"
        lines = [head, ""]
        for c in self.failures:
            detail = c.error or f"{c.violations:,} violating row(s)"
            lines.append(f"  FAIL  {c.name}: {detail}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _ident(name: str) -> str:
    """Quote an identifier so a column called `90+ DPD` cannot break the query."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _literal(value: Any) -> str:
    """Quote a value. Strings are escaped; numbers and booleans pass through."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def validate_panel(
    spec: DesignSpec,
    panel: str | Path | pd.DataFrame,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
) -> ValidationReport:
    """Run every enabled check against a panel."""
    own_connection = con is None
    con = con or duckdb.connect()
    try:
        if isinstance(panel, pd.DataFrame):
            con.register("panel_src", panel)
            con.execute("CREATE OR REPLACE TEMP VIEW panel AS SELECT * FROM panel_src")
        else:
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW panel AS SELECT * FROM read_parquet({_literal(str(panel))})"
            )

        available = {row[0] for row in con.execute("DESCRIBE panel").fetchall()}
        checks: list[CheckResult] = []
        toggles = spec.validation.checks

        if toggles.ids_unique_per_period:
            checks.append(_check_ids_unique(spec, con))
        if toggles.closed_pool:
            # An open pool cannot satisfy the closed-pool check — new entities
            # arriving is the point of it — so the same toggle buys the two
            # checks that *are* meaningful there instead.
            if spec.originations is None:
                checks.append(_check_closed_pool(spec, con))
            else:
                checks.append(_check_spans_contiguous(spec, con))
                checks.append(_check_origination_window(spec, con))
        if toggles.group_columns_stable and spec.groups:
            checks += _check_group_columns(spec, con)
        if spec.validation.plausibility:
            checks += _check_plausibility(spec, con, available)
        if toggles.static_columns_stable:
            checks += _check_static_stability(spec, con, available)
        if toggles.terminal_states_absorb and spec.lifecycle:
            checks.append(_check_terminal_absorbs(spec, con))
        if toggles.state_fields_applied and spec.lifecycle:
            checks += _check_state_fields(spec, con, available)
        if toggles.counters_step_correctly:
            checks += _check_counters(spec, con, available)
        if toggles.domains_respected:
            checks += _check_domains(spec, con, available)
        if toggles.non_negative_balances:
            checks += _check_non_negative(spec, con, available)

        checks += _check_custom(spec, con)
        return ValidationReport(spec.meta.name, checks)
    finally:
        if own_connection:
            con.close()


def _run(con: duckdb.DuckDBPyConnection, name: str, description: str, sql: str) -> CheckResult:
    """Execute a violation query: zero rows returned means the check passed."""
    try:
        rows = con.execute(sql).fetchdf()
    except Exception as exc:
        return CheckResult(name, description, passed=False, error=f"query failed: {exc}")
    return CheckResult(
        name=name,
        description=description,
        passed=len(rows) == 0,
        violations=len(rows),
        sample=rows.head(MAX_SAMPLE_ROWS).to_dict("records"),
    )


# ---------------------------------------------------------------------------
# individual checks
# ---------------------------------------------------------------------------


def _check_plausibility(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    """Does the book look like the asset class it claims to be?

    Every invariant can pass on a portfolio of four-thousand-euro loans to
    companies in one country, all rated the same, none of them ever repaying —
    internally consistent and obviously not a CLO. §21 asks for the other
    question, and asks for it as *broad plausibility* rather than as a distance
    from a reference tape, because the bundled pack has no reference tape and
    inventing one would mean shipping vendor-derived parameters.

    Reported through the same `CheckResult` the invariants use, so the bands
    reach the validation report, the HTML artefact and the results screen for
    free. A plausibility check nobody sees is worth nothing.
    """
    results: list[CheckResult] = []
    for band in spec.validation.plausibility:
        name = f"plausibility::{band.name}"
        if band.column not in available:
            results.append(
                CheckResult(
                    name=name,
                    description=band.note,
                    passed=False,
                    error=f"{band.column!r} is not in the panel",
                )
            )
            continue

        scope = "panel"
        if band.at_first_cutoff and spec.entity.time_column in available:
            column = _ident(spec.entity.time_column)
            scope = f"(select * from panel where {column} = (select min({column}) from panel))"

        column = _ident(band.column)
        if band.statistic == "share":
            expression = f"count(*) filter (where {band.where}) * 1.0 / nullif(count(*), 0)"
        elif band.statistic == "distinct":
            expression = f"count(distinct {column})"
        elif band.statistic == "mean":
            expression = f"avg({column})"
        elif band.statistic == "p10":
            expression = f"quantile_cont({column}, 0.10)"
        elif band.statistic == "p90":
            expression = f"quantile_cont({column}, 0.90)"
        else:
            expression = f"median({column})"

        low, high = band.between
        try:
            observed = con.execute(f"select {expression} from {scope}").fetchone()[0]
        except Exception as exc:
            results.append(
                CheckResult(name=name, description=band.note, passed=False, error=str(exc))
            )
            continue

        if observed is None:
            results.append(
                CheckResult(
                    name=name,
                    description=band.note,
                    passed=False,
                    error="nothing to measure",
                )
            )
            continue

        observed = float(observed)
        passed = low <= observed <= high
        results.append(
            CheckResult(
                name=name,
                description=band.note,
                passed=passed,
                # Not a row count. A band is one number against a range, so the
                # sample carries the number itself — a bare "1 violating row"
                # would say nothing about how far outside it landed.
                violations=0 if passed else 1,
                sample=[
                    {
                        "statistic": band.statistic,
                        "column": band.column,
                        "observed": round(observed, 6),
                        "expected_between": [low, high],
                    }
                ],
            )
        )
    return results


def _check_ids_unique(spec: DesignSpec, con) -> CheckResult:
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    return _run(
        con,
        "ids_unique_per_period",
        "Each entity appears at most once per cut-off.",
        f"SELECT {eid}, {etime}, COUNT(*) AS n FROM panel GROUP BY 1, 2 HAVING COUNT(*) > 1",
    )


def _check_closed_pool(spec: DesignSpec, con) -> CheckResult:
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    return _run(
        con,
        "closed_pool",
        "No entity appears that was not in the first cut-off.",
        f"SELECT DISTINCT {eid} FROM panel WHERE {eid} NOT IN ("
        f"  SELECT {eid} FROM panel WHERE {etime} = (SELECT MIN({etime}) FROM panel))",
    )


def _cutoff_index_cte(spec: DesignSpec) -> str:
    """Number the cut-offs 0, 1, 2… so periods can be compared arithmetically.

    The time column holds dates, not indices, and the calendar's step can be
    monthly or quarterly. Ranking the distinct values that actually occur avoids
    having to reconstruct the calendar in SQL.
    """
    etime = _ident(spec.entity.time_column)
    return (
        f"cutoffs AS (SELECT DISTINCT {etime} AS cutoff FROM panel), "
        "indexed AS (SELECT cutoff, ROW_NUMBER() OVER (ORDER BY cutoff) - 1 AS idx FROM cutoffs)"
    )


def _check_spans_contiguous(spec: DesignSpec, con) -> CheckResult:
    """An entity is reported at every cut-off between its first and its last.

    The open-pool counterpart to ``closed_pool``. New entities may appear, but an
    entity that vanishes for a period and comes back is a corrupted panel, and so
    is one written twice — both show up as a row count that does not match the
    span it covers.
    """
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    return _run(
        con,
        "entity_spans_contiguous",
        "Every entity is reported at each cut-off from its first to its last, with no gaps.",
        f"WITH {_cutoff_index_cte(spec)}, "
        f"spans AS (SELECT p.{eid} AS entity_id, MIN(i.idx) AS first_seen, MAX(i.idx) AS last_seen,"
        f"          COUNT(*) AS rows_written"
        f"          FROM panel p JOIN indexed i ON p.{etime} = i.cutoff GROUP BY 1) "
        "SELECT entity_id, first_seen, last_seen, rows_written FROM spans "
        "WHERE rows_written <> last_seen - first_seen + 1",
    )


def _check_origination_window(spec: DesignSpec, con) -> CheckResult:
    """Entities join the pool only when the spec says they may.

    Either at the opening cut-off, or inside the origination window. An entity
    appearing outside it means the ageing loop created one the configuration did
    not ask for.
    """
    plan = spec.originations
    assert plan is not None
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    last = plan.end_period if plan.end_period is not None else spec.entity.calendar.periods
    return _run(
        con,
        "origination_window",
        f"Entities join at the opening cut-off, or between periods {plan.start_period} and {last}.",
        f"WITH {_cutoff_index_cte(spec)}, "
        f"joined AS (SELECT p.{eid} AS entity_id, MIN(i.idx) AS joined_at"
        f"           FROM panel p JOIN indexed i ON p.{etime} = i.cutoff GROUP BY 1) "
        "SELECT entity_id, joined_at FROM joined "
        f"WHERE joined_at <> 0 AND (joined_at < {int(plan.start_period)} OR joined_at > {int(last)})",
    )


def _check_static_stability(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    """A static column must hold the same value for an entity in every period.

    One pass computes the per-entity distinct count for every static column at
    once; only the offending columns are then queried again for a sample, so a
    clean panel costs a single scan however wide the schema is.
    """
    eid = _ident(spec.entity.id_column)
    statics = [c for c in spec.static_columns if c in available and c != spec.entity.id_column]
    if not statics:
        return []

    inner = ", ".join(f"COUNT(DISTINCT {_ident(c)}) AS d{i}" for i, c in enumerate(statics))
    outer = ", ".join(f"MAX(d{i}) AS d{i}" for i in range(len(statics)))
    try:
        row = con.execute(
            f"SELECT {outer} FROM (SELECT {eid}, {inner} FROM panel GROUP BY 1)"
        ).fetchone()
    except Exception as exc:
        return [
            CheckResult(
                "static_columns_stable", "Static columns never change.", False, error=str(exc)
            )
        ]

    results: list[CheckResult] = []
    for i, col in enumerate(statics):
        # A column that is NULL everywhere yields 0 distinct values, not 1.
        if row[i] is not None and row[i] > 1:
            results.append(
                _run(
                    con,
                    f"static_stable::{col}",
                    f"{col} is declared static, so it must never change for an entity.",
                    f"SELECT {eid}, COUNT(DISTINCT {_ident(col)}) AS distinct_values FROM panel "
                    f"GROUP BY 1 HAVING COUNT(DISTINCT {_ident(col)}) > 1",
                )
            )
    if not results:
        results.append(
            CheckResult(
                "static_columns_stable",
                f"All {len(statics)} static column(s) hold steady across every cut-off.",
                passed=True,
            )
        )
    return results


def _check_group_columns(spec: DesignSpec, con) -> list[CheckResult]:
    """A group's attributes must agree across every member, in every period.

    This is the check that catches a broken join. Three facilities lent to the
    same company have to agree about that company's industry — if they disagree,
    the attributes were generated per entity rather than per group, and every
    figure computed by obligor is wrong while looking entirely plausible.

    Deliberately two assertions, not one: constant *within a group* is what makes
    the group real, and constant *over time* is what makes it a parent record
    rather than a monthly observation.
    """
    results: list[CheckResult] = []
    for group in spec.groups:
        if not group.columns:
            continue
        key = _ident(group.key)
        for column in group.columns:
            col = _ident(column.name)
            results.append(
                _run(
                    con,
                    f"group_stable::{group.name}.{column.name}",
                    f"{column.name} belongs to the {group.name}, so every member shares it.",
                    f"SELECT {key}, COUNT(DISTINCT {col}) AS distinct_values FROM panel "
                    f"GROUP BY 1 HAVING COUNT(DISTINCT {col}) > 1",
                )
            )
    if not results and spec.groups:
        results.append(
            CheckResult(
                "group_columns_stable",
                "No group attributes declared.",
                passed=True,
            )
        )
    return results


def _check_terminal_absorbs(spec: DesignSpec, con) -> CheckResult:
    lc = spec.lifecycle
    assert lc is not None
    if not lc.terminal:
        return CheckResult("terminal_states_absorb", "No terminal states declared.", passed=True)
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    state = _ident(lc.state_column)
    values = ", ".join(_literal(s) for s in lc.terminal)
    return _run(
        con,
        "terminal_states_absorb",
        f"An entity reaching {lc.terminal} is written once and never appears again.",
        f"WITH gone AS ("
        f"  SELECT {eid} AS entity_id, MIN({etime}) AS ended FROM panel"
        f"  WHERE {state} IN ({values}) GROUP BY 1)"
        f" SELECT p.{eid}, p.{etime}, p.{state} FROM panel p"
        f" JOIN gone g ON p.{eid} = g.entity_id WHERE p.{etime} > g.ended",
    )


def _check_state_fields(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    """Every value the spec pins to a state must actually hold in the output."""
    lc = spec.lifecycle
    assert lc is not None
    state = _ident(lc.state_column)
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    results: list[CheckResult] = []

    for label, fields in lc.state_fields.items():
        clauses = [
            f"{_ident(col)} IS DISTINCT FROM {_literal(value)}"
            for col, value in fields.items()
            if col in available
        ]
        if not clauses:
            continue
        results.append(
            _run(
                con,
                f"state_fields::{label}",
                f"Rows in state {label!r} carry the values the spec pins to it.",
                f"SELECT {eid}, {etime}, {state} FROM panel "
                f"WHERE {state} = {_literal(label)} AND ({' OR '.join(clauses)})",
            )
        )
    return results


def _check_counters(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    """A fixed-step counter must move by exactly that step between periods.

    Clipping is the one legitimate exception: a counter pinned at its floor stops
    moving, so a row sitting on a bound is excused.
    """
    eid, etime = _ident(spec.entity.id_column), _ident(spec.entity.time_column)
    results: list[CheckResult] = []

    for ctr in spec.dynamics.counters:
        if ctr.step is None or ctr.column not in available:
            continue
        col = _ident(ctr.column)
        excuses = []
        if ctr.clip_min is not None:
            excuses.append(f"curr_value = {_literal(ctr.clip_min)}")
        if ctr.clip_max is not None:
            excuses.append(f"curr_value = {_literal(ctr.clip_max)}")
        excuse_sql = f" AND NOT ({' OR '.join(excuses)})" if excuses else ""

        results.append(
            _run(
                con,
                f"counter_step::{ctr.column}",
                f"{ctr.column} moves by exactly {ctr.step} each period.",
                f"WITH stepped AS ("
                f"  SELECT {eid} AS entity_id, {etime} AS cutoff, {col} AS curr_value,"
                f"         LAG({col}) OVER (PARTITION BY {eid} ORDER BY {etime}) AS prev_value"
                f"  FROM panel)"
                f" SELECT entity_id, cutoff, prev_value, curr_value FROM stepped"
                f" WHERE prev_value IS NOT NULL"
                f" AND curr_value - prev_value <> {_literal(ctr.step)}{excuse_sql}",
            )
        )
    return results


def _check_domains(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    results: list[CheckResult] = []
    for column in spec.columns:
        if not column.domain or column.name not in available:
            continue
        col = _ident(column.name)
        values = ", ".join(_literal(v) for v in column.domain)
        results.append(
            _run(
                con,
                f"domain::{column.name}",
                f"{column.name} only takes the values {column.domain}.",
                f"SELECT DISTINCT {col} AS value FROM panel "
                f"WHERE {col} IS NOT NULL AND {col} NOT IN ({values})",
            )
        )
    return results


def _check_non_negative(spec: DesignSpec, con, available: set[str]) -> list[CheckResult]:
    columns = [c for c in spec.validation.non_negative_columns if c in available]
    if not columns:
        return []
    clauses = " OR ".join(f"{_ident(c)} < 0" for c in columns)
    picked = ", ".join(_ident(c) for c in columns)
    return [
        _run(
            con,
            "non_negative_columns",
            f"These never go below zero: {', '.join(columns)}.",
            f"SELECT {_ident(spec.entity.id_column)}, {picked} FROM panel WHERE {clauses}",
        )
    ]


def _check_custom(spec: DesignSpec, con) -> list[CheckResult]:
    return [
        _run(con, f"custom::{c.name}", c.description or c.name, c.sql)
        for c in spec.validation.custom
    ]
