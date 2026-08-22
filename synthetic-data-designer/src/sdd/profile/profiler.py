"""Read a sample tape and work out how it was put together.

This is the "it analyses it" half of the tool. Given a CSV or parquet file it
determines, for every column: what type it holds, whether it stays fixed for an
entity or moves over time, what values it takes, and which distribution best
describes it.

The two structural questions come first, because everything else depends on
them:

**Which column identifies an entity?**
    Looked for by name, then by behaviour — the column that is unique within
    each cut-off and largely repeats across them.

**Which column is the cut-off date?**
    The date-like column with few distinct values, each shared by many rows.

Get those two right and "static vs dynamic" falls out by counting distinct
values per entity. Get them wrong and every downstream inference is wrong, so
both are reported with a reason and can be overridden.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sdd.profile.distributions import Fit, fit_categorical, fit_numeric, looks_discrete
from sdd.spec.schema import ColumnRole, DType

# Column names that give the game away, checked before any behavioural guess.
ID_NAME_HINTS = (
    "loan_id",
    "id",
    "identifier",
    "unique_id",
    "exposure_id",
    "account_id",
    "contract_id",
)
TIME_NAME_HINTS = (
    "reporting_date",
    "as_of",
    "as_of_date",
    "cutoff",
    "cut_off",
    "cut_off_date",
    "data_cut_off_date",
    "period",
    "snapshot_date",
    "observation_date",
)

# A string column with at most this share of distinct values is a category
# rather than free text.
MAX_CATEGORICAL_SHARE = 0.20
MAX_CATEGORICAL_VALUES = 200


@dataclass
class ColumnProfile:
    name: str
    dtype: DType
    role: ColumnRole
    nulls: float
    distinct: int
    examples: list[Any] = field(default_factory=list)
    minimum: float | None = None
    maximum: float | None = None
    mean: float | None = None
    domain: list[Any] | None = None
    fit: Fit | None = None
    note: str | None = None
    resample: dict[str, Any] | None = field(default=None, repr=False)

    @property
    def confidence(self) -> float:
        return self.fit.confidence if self.fit else 0.5

    def to_dict(self) -> dict[str, Any]:
        out = {k: v for k, v in asdict(self).items() if k != "fit"}
        out["fit"] = self.fit.to_dict() if self.fit else None
        out["confidence"] = round(self.confidence, 3)
        return out


@dataclass
class DatasetProfile:
    rows: int
    columns: list[ColumnProfile]
    id_column: str | None
    time_column: str | None
    periods: int
    entities: int
    is_panel: bool
    opening_entities: int = 0
    detection_notes: dict[str, str] = field(default_factory=dict)
    dynamics: dict[str, Any] = field(default_factory=dict)
    derived: list[Any] = field(default_factory=list)
    correlation: dict[str, Any] | None = None

    def column(self, name: str) -> ColumnProfile | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def needs_review(self) -> list[ColumnProfile]:
        return [c for c in self.columns if c.confidence < 0.5]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "entities": self.entities,
            "opening_entities": self.opening_entities,
            "periods": self.periods,
            "is_panel": self.is_panel,
            "id_column": self.id_column,
            "time_column": self.time_column,
            "detection_notes": self.detection_notes,
            "dynamics": self.dynamics,
            "correlation": self.correlation,
            "derived": [
                {"target": d.target, "source": d.source, "confidence": d.confidence}
                for d in self.derived
            ],
            "needs_review": [c.name for c in self.needs_review],
            "columns": [c.to_dict() for c in self.columns],
        }

    def summary(self) -> str:
        shape = "panel" if self.is_panel else "single snapshot"
        lines = [
            f"{self.rows:,} rows x {len(self.columns)} columns ({shape})",
            f"  entity      {self.id_column or '(not detected)'}  -> {self.entities:,} entities",
            f"  time        {self.time_column or '(not detected)'}  -> {self.periods} period(s)",
        ]
        by_role: dict[str, int] = {}
        for c in self.columns:
            by_role[c.role] = by_role.get(c.role, 0) + 1
        lines.append("  roles       " + ", ".join(f"{v} {k}" for k, v in sorted(by_role.items())))
        if self.dynamics:
            learned = ", ".join(sorted(self.dynamics))
            lines.append(f"  learned     {learned}")
        if self.derived:
            pairs = ", ".join(f"{d.target}<-{d.source}" for d in self.derived[:4])
            more = f" (+{len(self.derived) - 4} more)" if len(self.derived) > 4 else ""
            lines.append(f"  derived     {pairs}{more}")
        if self.needs_review:
            names = ", ".join(c.name for c in self.needs_review[:6])
            more = f" (+{len(self.needs_review) - 6} more)" if len(self.needs_review) > 6 else ""
            lines.append(f"  review      {names}{more}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------


READABLE_SUFFIXES = (
    ".csv",
    ".tsv",
    ".txt",
    ".parquet",
    ".pq",
    ".xlsx",
    ".xlsm",
    ".xls",
    ".json",
    ".jsonl",
    ".ndjson",
)


def read_sample(path: str | Path, *, max_rows: int | None = None) -> pd.DataFrame:
    """Load a sample tape from any of the four formats the UI accepts.

    CSV and parquet are the working formats; Excel and JSON are accepted because
    that is how a schema or a small extract usually arrives from a business team.
    ``max_rows`` is honoured by the readers that support it and applied after the
    fact by the ones that do not.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in (".parquet", ".pq"):
        df = pd.read_parquet(path)
    elif suffix in (".xlsx", ".xlsm", ".xls"):
        df = pd.read_excel(path, nrows=max_rows)
    elif suffix in (".jsonl", ".ndjson"):
        df = pd.read_json(path, lines=True, nrows=max_rows)
    elif suffix == ".json":
        df = _read_json(path)
    elif suffix in (".tsv",):
        df = pd.read_csv(path, sep="\t", nrows=max_rows, low_memory=False)
    else:
        df = pd.read_csv(path, nrows=max_rows, low_memory=False)

    return df.head(max_rows) if max_rows is not None and len(df) > max_rows else df


def _read_json(path: Path) -> pd.DataFrame:
    """Read a JSON tape, whichever of the three usual shapes it arrives in.

    A list of row objects, an object wrapping one under a common key
    (``data``/``rows``/``records``), or a column-oriented mapping. Anything else
    is a schema document, not data, and is refused here rather than silently
    profiled as one row of nested junk.
    """
    import json as _json

    raw = _json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        for key in ("data", "rows", "records", "items"):
            if isinstance(raw.get(key), list):
                raw = raw[key]
                break
    if isinstance(raw, list):
        if not raw:
            raise ValueError(f"{path.name} holds an empty list; there is nothing to profile")
        return pd.json_normalize(raw)
    if isinstance(raw, dict) and all(isinstance(v, list) for v in raw.values()):
        return pd.DataFrame(raw)
    raise ValueError(
        f"{path.name} is not tabular JSON — expected a list of row objects, or an object "
        "wrapping one under 'data'/'rows'/'records'"
    )


# ---------------------------------------------------------------------------
# structure detection
# ---------------------------------------------------------------------------


def detect_time_column(df: pd.DataFrame) -> tuple[str | None, str]:
    """Find the cut-off date column.

    A cut-off column has few distinct values, each shared by many rows — the
    opposite shape to an origination or maturity date, which is roughly unique
    per loan. That distinction is what stops the profiler from treating
    ``maturity_date`` as the panel's time axis.
    """
    for name in TIME_NAME_HINTS:
        if name in df.columns:
            return name, f"matched the known cut-off name {name!r}"

    best: tuple[str, int] | None = None
    for col in df.columns:
        series = df[col].dropna()
        if series.empty or not _looks_like_date(series):
            continue
        distinct = series.nunique()
        # Few distinct values, each covering many rows.
        if distinct < 2 or distinct > max(len(df) // 10, 2):
            continue
        if best is None or distinct < best[1]:
            best = (col, distinct)

    if best:
        return best[0], f"date-like with only {best[1]} distinct values, each shared by many rows"
    return None, "no column looked like a repeated cut-off date"


def detect_id_column(df: pd.DataFrame, time_column: str | None) -> tuple[str | None, str]:
    """Find the entity identifier."""
    for name in ID_NAME_HINTS:
        if name in df.columns:
            return name, f"matched the known identifier name {name!r}"

    candidates: list[tuple[str, float]] = []
    for col in df.columns:
        if col == time_column:
            continue
        series = df[col]
        if series.isna().any():
            continue
        if time_column and time_column in df.columns:
            # Unique inside every cut-off is the defining property.
            per_period = df.groupby(time_column)[col].agg(["nunique", "size"])
            if not (per_period["nunique"] == per_period["size"]).all():
                continue
            # Prefer the column that recurs most across periods.
            recurrence = len(df) / max(series.nunique(), 1)
            candidates.append((col, recurrence))
        elif series.is_unique:
            candidates.append((col, 1.0))

    if candidates:
        best = max(candidates, key=lambda c: c[1])
        return best[0], "unique within every cut-off and recurring across them"
    return None, "no column was unique within each cut-off"


def _looks_like_date(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    sample = series.astype(str).head(200)
    try:
        parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
    except (ValueError, TypeError):
        return False
    return bool(parsed.notna().mean() > 0.9)


# ---------------------------------------------------------------------------
# per-column profiling
# ---------------------------------------------------------------------------


def infer_dtype(series: pd.Series) -> DType:
    clean = series.dropna()
    if clean.empty:
        return "str"
    if pd.api.types.is_bool_dtype(clean):
        return "bool"
    if pd.api.types.is_numeric_dtype(clean):
        return "int" if looks_discrete(clean) and clean.nunique() > 2 else "float"
    if _looks_like_date(clean):
        return "date"
    return "category" if _is_categorical(clean) else "str"


def _is_categorical(series: pd.Series) -> bool:
    distinct = series.nunique()
    if distinct > MAX_CATEGORICAL_VALUES:
        return False
    return distinct / max(len(series), 1) <= MAX_CATEGORICAL_SHARE or distinct <= 30


def infer_role(
    df: pd.DataFrame, column: str, id_column: str | None, is_panel: bool
) -> tuple[ColumnRole, str | None]:
    """Static, dynamic, or constant — decided by counting, not by guessing.

    Without a panel there is no way to tell static from dynamic (nothing has
    been observed twice), so everything non-constant is called static and the
    ambiguity is recorded rather than papered over.
    """
    series = df[column]
    if series.nunique(dropna=False) <= 1:
        return "constant", None
    if not is_panel or not id_column:
        return (
            "static",
            "single snapshot: cannot tell static from dynamic without repeat observations",
        )
    changing = df.groupby(id_column)[column].nunique(dropna=False)
    return ("static", None) if (changing <= 1).all() else ("dynamic", None)


def profile_column(
    df: pd.DataFrame,
    column: str,
    id_column: str | None,
    is_panel: bool,
    first_period: pd.DataFrame | None = None,
) -> ColumnProfile:
    series = df[column]
    dtype = infer_dtype(series)
    role, note = infer_role(df, column, id_column, is_panel)

    # Generators are fitted to the *first* cut-off, never to the pooled panel,
    # because what they produce is the opening book. Two distinct errors follow
    # from pooling, and both compound through the ageing pass:
    #
    #   dynamic columns — seasoning_months pooled over 24 cut-offs is smeared 24
    #     months wider than it is on day one, so every loan starts in the wrong
    #     place;
    #   static columns — a loan that redeems in month 2 contributes 2 rows while
    #     a survivor contributes 24, so the pooled mix is weighted by survival
    #     rather than by origination. Any characteristic correlated with
    #     prepayment or default is silently skewed.
    source = df
    if first_period is not None and not first_period.empty:
        source = first_period
        note = note or "generator fitted to the first cut-off, which is what the opening book is"
    clean = source[column].dropna()

    profile = ColumnProfile(
        name=column,
        dtype=dtype,
        role=role,
        nulls=round(float(series.isna().mean()), 6),
        distinct=int(series.nunique(dropna=True)),
        examples=[_plain(v) for v in clean.head(3).tolist()],
        note=note,
    )

    # A column empty at the first cut-off but filled in later.
    #
    # Event dates are the common case: a loan has no default date on day one and
    # acquires one the month it defaults. Generators are fitted to the first
    # cut-off, so the sample is empty, and every branch below assumes it is not —
    # `fit_categorical` refused to build from nothing and the whole profile run
    # died on the column.
    #
    # Empty *is* the answer for the opening book: the column starts blank. What
    # is lost is that something fills it later, and no amount of looking at
    # period 0 reveals what. So the generator says blank, the domain is taken
    # from the whole panel so the validator accepts the values that do arrive,
    # and the note tells whoever reads the spec that a rule is missing.
    if clean.empty and series.notna().any():
        from sdd.spec.schema import ConstantGen

        later = series.dropna()
        profile.fit = Fit(
            ConstantGen(value=None),
            "constant",
            0.0,
            0.4,
            note=(
                f"empty at the first cut-off and filled in later ({series.notna().sum():,} "
                "values across the panel). Generated blank, which is correct for the opening "
                "book; add a derivation to stamp it when the event happens."
            ),
        )
        if dtype in ("category", "bool", "str", "date"):
            profile.domain = [_plain(v) for v in sorted(later.unique(), key=str)][
                :MAX_CATEGORICAL_VALUES
            ]
        return profile

    if dtype in ("int", "float") and not clean.empty:
        numeric = pd.to_numeric(clean, errors="coerce").dropna()
        if not numeric.empty:
            profile.minimum = round(float(numeric.min()), 6)
            profile.maximum = round(float(numeric.max()), 6)
            profile.mean = round(float(numeric.mean()), 6)

    if role == "constant":
        from sdd.spec.schema import ConstantGen

        value = _plain(clean.iloc[0]) if not clean.empty else None
        profile.fit = Fit(ConstantGen(value=value), "constant", 0.0, 1.0)
    elif dtype in ("category", "bool", "str"):
        if dtype == "str" and not _is_categorical(clean):
            # Free text or a per-row identifier: resampling it would leak the
            # sample verbatim, so hand back a synthetic id instead.
            from sdd.spec.schema import UUIDGen

            profile.fit = Fit(
                UUIDGen(short=True),
                "uuid",
                0.0,
                0.3,
                note="high-cardinality text; replaced with a synthetic identifier rather than "
                "resampled, which would copy sample values verbatim",
            )
        else:
            profile.fit = fit_categorical(clean)
            # The domain comes from the *whole* panel even though the generator
            # comes from period 0. They answer different questions: the
            # generator seeds the opening book, while the domain constrains
            # every period. A delinquency column has no Redeemed loans on day
            # one but certainly acquires them, and a domain drawn from period 0
            # would make the validator reject the engine's own correct output.
            everywhere = df[column].dropna()
            profile.domain = [_plain(v) for v in sorted(everywhere.unique(), key=str)][
                :MAX_CATEGORICAL_VALUES
            ]
    elif dtype == "date":
        profile.fit = fit_categorical(clean.astype(str))
        profile.fit.confidence = 0.6
        profile.fit.note = "dates resampled from observed values; consider a calendar rule instead"
    else:
        profile.fit = fit_numeric(clean)
        # Kept alongside the chosen fit so the "sampling" generation method has
        # the observed shape to draw from without re-reading the tape.
        profile.resample = _resample_generator(clean)

    return profile


def _resample_generator(values: pd.Series) -> dict[str, Any] | None:
    """A binned copy of the observed distribution, as a serialisable generator."""
    from sdd.profile.distributions import _empirical

    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty or numeric.nunique() < 2:
        return None
    try:
        return _empirical(numeric).generator.model_dump(mode="json")
    except Exception:
        return None


# Enough columns to carry the joint structure that matters, few enough that the
# matrix stays readable in a spec and cheap to reimpose.
MAX_CORRELATED_COLUMNS = 40

# Below this, a pair is noise rather than structure, and reimposing it just adds
# spurious detail to the output.
MIN_ABS_CORRELATION = 0.05


def measure_correlation(df: pd.DataFrame, columns: list[str]) -> dict[str, Any] | None:
    """Spearman rank correlation between numeric columns.

    Rank correlation rather than Pearson because that is what can be reimposed
    on generated data by reordering: reordering preserves each column's own
    distribution exactly, and only ranks carry across.
    """
    usable = [
        c
        for c in columns
        if c in df.columns and pd.api.types.is_numeric_dtype(df[c]) and df[c].nunique() > 2
    ][:MAX_CORRELATED_COLUMNS]
    if len(usable) < 2:
        return None

    frame = df[usable].apply(pd.to_numeric, errors="coerce")
    correlations = frame.corr(method="spearman", min_periods=20)
    if correlations.isna().all().all():
        return None

    # Taken into numpy with an explicit copy before anything is written into it.
    # Under pandas' copy-on-write — the default from pandas 3 — the array behind
    # a frame is handed out read-only, and `fill_diagonal` on it raises.
    values = correlations.fillna(0.0).to_numpy(dtype=float, copy=True)
    np.fill_diagonal(values, 1.0)
    matrix = pd.DataFrame(values, index=correlations.index, columns=correlations.columns)

    # Drop columns that correlate with nothing: carrying them costs spec size and
    # buys no structure.
    off_diagonal = matrix.abs().sum(axis=1) - 1.0
    keep = [c for c in usable if off_diagonal[c] >= MIN_ABS_CORRELATION]
    if len(keep) < 2:
        return None
    matrix = matrix.loc[keep, keep]

    return {
        "columns": keep,
        "matrix": [[round(float(v), 4) for v in row] for row in matrix.to_numpy()],
        "method": "spearman",
        "rows": len(frame),
    }


# ---------------------------------------------------------------------------
# the whole dataset
# ---------------------------------------------------------------------------


def profile_dataset(
    data: str | Path | pd.DataFrame,
    *,
    id_column: str | None = None,
    time_column: str | None = None,
    state_column: str | None = None,
    max_rows: int | None = None,
    learn_dynamics: bool = True,
) -> DatasetProfile:
    """Profile a sample tape or panel.

    ``id_column`` / ``time_column`` override detection when the heuristics get
    it wrong, which is why both report the reason they chose what they did.
    """
    df = data if isinstance(data, pd.DataFrame) else read_sample(data, max_rows=max_rows)
    # Applied again for an in-memory frame, which no reader trimmed. Without
    # this the cap silently means nothing when the caller passes a DataFrame,
    # and "profile the first 200k rows" quietly profiles all five million.
    if max_rows is not None and len(df) > max_rows:
        df = df.head(max_rows)
    if df.empty:
        raise ValueError("the sample is empty; nothing to profile")

    notes: dict[str, str] = {}
    if time_column is None:
        time_column, why = detect_time_column(df)
        notes["time_column"] = why
    else:
        notes["time_column"] = "supplied by the caller"

    if id_column is None:
        id_column, why = detect_id_column(df, time_column)
        notes["id_column"] = why
    else:
        notes["id_column"] = "supplied by the caller"

    periods = int(df[time_column].nunique()) if time_column else 1
    entities = int(df[id_column].nunique()) if id_column else len(df)
    is_panel = bool(time_column and id_column and periods > 1)

    first_period = (
        df[df[time_column] == df[time_column].min()] if is_panel and time_column else None
    )
    columns = [profile_column(df, c, id_column, is_panel, first_period) for c in df.columns]

    # How many entities the book *opened* with, which is not how many it ever
    # held. A pool that reinvests acquires assets as it goes, so the panel-wide
    # count runs well ahead of the opening one — 538 facilities seen against 250
    # on day one, in a run measured here. Anything describing the opening book
    # has to divide by the smaller number or it states a portfolio that never
    # existed at any one moment.
    opening_entities = entities
    if first_period is not None and not first_period.empty:
        opening_entities = (
            int(first_period[id_column].nunique()) if id_column else len(first_period)
        )

    profile = DatasetProfile(
        rows=len(df),
        columns=columns,
        id_column=id_column,
        time_column=time_column,
        periods=periods,
        entities=entities,
        is_panel=is_panel,
        opening_entities=opening_entities,
        detection_notes=notes,
    )

    # Measured on the first cut-off for the same reason generators are: what the
    # opening book needs is how columns moved together on day one, not how the
    # survivors looked pooled across two years.
    profile.correlation = measure_correlation(
        first_period if first_period is not None and not first_period.empty else df,
        [c.name for c in columns if c.dtype in ("int", "float") and c.role != "constant"],
    )

    # Columns computed from other columns are re-expressed as derivations, so
    # the generated data stays internally consistent instead of pairing a
    # balance with a band that does not contain it.
    #
    # Ahead of the dynamics pass, which it used to follow. A binned column moves
    # over time like anything else, so the panel learner saw `balance_bucket`
    # stepping down through its bands and offered it as a state machine
    # migrating under its own steam. Marking derivations first means every later
    # pass sees `role == "derived"` and leaves them alone — a band should follow
    # the balance it was cut from, not run beside it.
    from sdd.profile.derived import find_bucket_columns

    buckets = find_bucket_columns(df, profile)
    if buckets:
        profile.derived = buckets
        by_name = {b.target: b for b in buckets}
        for col in profile.columns:
            if col.name in by_name:
                col.role = "derived"
                col.fit = None
                col.note = by_name[col.name].note

    if is_panel and learn_dynamics:
        from sdd.profile.panel import learn_panel_dynamics

        profile.dynamics = learn_panel_dynamics(df, profile, state_column=state_column)

    return profile


def _plain(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value
