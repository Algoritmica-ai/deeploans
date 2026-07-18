# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Canonical credit panel schema: one row per (loan_id, observation_date). Required
columns: loan_id, observation_date, is_origination, event_type; plus asset-class
fields. Static fields repeat across observations; dynamic fields vary.

Also provides data-driven helpers used to build tokenizer configs:
``classify_fields`` (role + value type per column) and ``find_redundant`` (exact-duplicate
and functional-dependency detection), so the tokenizer config is reproducible from the data
rather than hand-curated.
"""

from __future__ import annotations

from dataclasses import dataclass, field


REQUIRED_COLUMNS = ['loan_id', 'observation_date', 'is_origination', 'event_type']


@dataclass
class CreditPanelSchema:
    static_fields: list[str] = field(default_factory=list)
    dynamic_fields: list[str] = field(default_factory=list)
    categorical_fields: list[str] = field(default_factory=list)
    numeric_fields: list[str] = field(default_factory=list)

    def validate(self, df) -> None:
        """Assert the dataframe satisfies the canonical schema."""
        raise NotImplementedError


def classify_fields(
    df,
    id_col: str = "loan_id",
    time_col: str = "reporting_date",
    num_unique_cat: int = 20,
    sample_loans: int = 20000,
    seed: int = 42,
) -> dict:
    """Classify every column by role and value type for tokenizer config.

    role  : ``id`` | ``static`` (constant within a loan) | ``dynamic`` (varies per cutoff).
    type  : ``constant`` (1 value → drop) | ``temporal`` | ``flag`` | ``bucket`` |
            ``numeric`` | ``categorical`` | ``text``.

    Static/dynamic is decided on a random sample of loans for speed (structural property).
    Run on ``train.parquet`` so any downstream cardinality stats stay train-only.

    Returns ``{col: {"role", "type", "n_unique", "null_frac"}}``.
    """
    import pandas as pd

    loans = pd.Series(df[id_col].unique())
    if sample_loans and len(loans) > sample_loans:
        keep = set(loans.sample(sample_loans, random_state=seed))
        grouped = df[df[id_col].isin(keep)].groupby(id_col)
    else:
        grouped = df.groupby(id_col)

    out: dict[str, dict] = {}
    for c in df.columns:
        col = df[c]
        name = c.lower()
        n_unique = int(col.nunique(dropna=True))
        if c == id_col:
            role, ftype = "id", "text"
        else:
            role = "static" if grouped[c].nunique(dropna=False).max() <= 1 else "dynamic"
            if n_unique <= 1:
                ftype = "constant"
            elif c == time_col or name.endswith("_date") or "maturity_date" in name:
                ftype = "temporal"
            elif name.endswith("_flag"):
                ftype = "flag"
            elif name.endswith("_bucket"):
                ftype = "bucket"
            elif pd.api.types.is_numeric_dtype(col):
                ftype = "numeric" if n_unique > num_unique_cat else "categorical"
            else:
                avg_len = col.dropna().astype(str).str.len().mean()
                ftype = "text" if (n_unique > 1000 or avg_len > 40) else "categorical"
        out[c] = {
            "role": role,
            "type": ftype,
            "n_unique": n_unique,
            "null_frac": round(float(col.isna().mean()), 4),
        }
    return out


def find_redundant(
    df,
    info: dict,
    id_col: str = "loan_id",
    max_card: int = 64,
    sample_rows: int = 200000,
    seed: int = 42,
) -> dict:
    """Detect redundant columns from the data.

    Returns ``{"safe": {col: reason}, "review": {col: reason}}``.

    * ``safe`` — unambiguous, fine to auto-drop: exact-duplicate columns and pre-computed
      ``*_bucket`` discretizations of a NUMERIC raw field (the raw is kept and re-bucketed by
      the tokenizer). A ``*_bucket`` with no numeric base (e.g. ``arrears_bucket``) is a real
      state and is NOT dropped.
    * ``review`` — functional dependencies among low-cardinality columns (``X = f(Y)``,
      keeping the more granular ``Y``). NOT auto-dropped — surfaced for human judgment, since
      some (e.g. ``default_crr_flag``) may be wanted as explicit signals. Determiners are
      limited to low-cardinality ``Y`` so continuous columns can't trivially determine others.
    """
    feats = [c for c in df.columns
             if c != id_col and info[c]["type"] not in ("constant", "temporal")]
    s = df.sample(min(len(df), sample_rows), random_state=seed) if len(df) > sample_rows else df
    nun = {c: int(s[c].nunique(dropna=False)) for c in feats}

    safe: dict[str, str] = {}
    # exact duplicates (same dtype + cardinality, identical values on the sample)
    by_sig: dict[tuple, list[str]] = {}
    for c in feats:
        by_sig.setdefault((str(s[c].dtype), nun[c]), []).append(c)
    for group in by_sig.values():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if b in safe:
                    continue
                if s[a].reset_index(drop=True).equals(s[b].reset_index(drop=True)):
                    safe[b] = f"== {a}"
    # pre-computed buckets: only redundant if they discretize a NUMERIC raw column.
    for c in feats:
        if c.endswith("_bucket") and c not in safe:
            base = c[: -len("_bucket")]
            raw = next((r for r in df.columns
                        if (r == base or r.endswith(base)) and info.get(r, {}).get("type") == "numeric"),
                       None)
            if raw is not None:
                safe[c] = f"bucketed {raw}"

    review: dict[str, str] = {}
    cand = [c for c in feats if c not in safe and nun[c] <= max_card]
    for x in sorted(cand, key=lambda c: nun[c]):
        for y in feats:
            if y == x or y in safe:
                continue
            if not (nun[x] < nun[y] <= max_card):   # both low-card; keep the finer Y
                continue
            if s.groupby(y, observed=True)[x].nunique(dropna=False).max() == 1:
                review[x] = f"= f({y})"
                break
    return {"safe": safe, "review": review}
