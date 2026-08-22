"""Build the period-0 book: one row per entity, at the first cut-off.

This is the "origination snapshot" — every loan as it looks on day one of the
panel. :mod:`sdd.age.panel` then walks it forward.

Order of operations, and why:

1. **Sample** each column in declaration order, so a conditional generator always
   sees its parent.
2. **Apply constants** — deal-level facts that never vary.
3. **Assign ids** from ``entity.id_format`` when given. Upstream did this because
   an 8-hex-character random id collides about 29 times in 500k draws; a
   sequential id cannot collide at all.
4. **Run derivations** in order — arithmetic that pandas does far faster than a
   per-row sampler would.
5. **Coerce dtypes**, drop helpers, order columns.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd

from sdd.calendar import period_dates
from sdd.generate.deriver import evaluate_on
from sdd.generate.samplers import sample
from sdd.spec.schema import Bucket, Derivation, DesignSpec, DType

ProgressFn = Callable[[str, float], None]


class GenerationError(RuntimeError):
    """Generation failed in a way the spec should have prevented."""


def build_book(
    spec: DesignSpec,
    num_records: int,
    *,
    seed: int = 42,
    backend: str = "numpy",
    sample: pd.DataFrame | None = None,
    notes: dict[str, Any] | None = None,
    id_offset: int = 0,
    at: str | None = None,
    progress: ProgressFn | None = None,
    group_state: dict[str, pd.DataFrame] | None = None,
    fresh_cohort: bool = False,
) -> pd.DataFrame:
    """Generate a book of ``num_records`` entities.

    Usually this is the period-0 book. An open pool builds it again at later
    cut-offs for the entities joining then, which is what ``id_offset`` and
    ``at`` are for: the first continues the identifier sequence past the
    entities that already exist, the second stamps the cut-off they join on.

    ``sample`` is the real tape the spec was profiled from, when there is one. It
    is only read by the deep generation methods (``ctgan``, ``hybrid``), which
    learn from it directly; every other method works from the spec alone.

    ``notes``, when given, is filled with what the randomness controls and the
    deep model actually did — a slider that silently did nothing is worse than no
    slider, so the caller is given the means to say.
    """
    if num_records < 1:
        raise GenerationError(f"num_records must be at least 1, got {num_records}")

    def report(stage: str, frac: float) -> None:
        if progress:
            progress(stage, frac)

    rng = np.random.default_rng(seed)

    if backend == "nemo":
        from sdd.generate.backend_nemo import sample_columns_nemo

        df = sample_columns_nemo(spec, num_records, seed=seed)
    else:
        df = _sample_columns(spec, num_records, rng, report, id_offset)

    if spec.generation.needs_sample:
        report("deep model", 0.52)
        df, polish_note = _polish(spec, df, sample)
        if notes is not None:
            notes["polish"] = polish_note

    # The opening state mix, if the spec declares one.
    #
    # `initial_distribution` is documented as "the state mix at period 0", and
    # until now it set nothing: the state column's own generator supplied the
    # opening states and this field was read only by the rate calibration. The
    # two shipped packs happened to carry identical numbers in both places, so
    # nothing looked wrong — while a spec whose two disagreed got the generator's
    # mix in the data and had its implied default and prepayment rates computed
    # against the other one.
    if spec.lifecycle is not None and spec.lifecycle.initial_distribution:
        df = _apply_initial_states(spec, df, rng)

    # Groups before randomness and before derivations: a group attribute is an
    # input to both, and joining it afterwards would leave derived columns
    # computed from values that were not there yet.
    if spec.groups:
        from sdd.generate.groups import attach_groups

        report("groups", 0.53)
        df = attach_groups(spec, df, rng, state=group_state, fresh_cohort=fresh_cohort)

    report("randomness", 0.54)
    from sdd.generate.randomness import apply_randomness

    df, randomness = apply_randomness(spec, df, rng)
    if notes is not None:
        notes["randomness"] = randomness

    report("constants", 0.55)
    df = _apply_constants(spec, df)

    report("identifiers", 0.6)
    df = _apply_id_format(spec, df, id_offset)

    report("time column", 0.62)
    dates = period_dates(spec.entity.calendar)
    df[spec.entity.time_column] = at or dates[0].strftime("%Y-%m-%d")

    # Apply the lifecycle's per-state values before derivations so period 0 and
    # every later period agree on what a state implies. Without this the book
    # would need its own copy of the state -> days-past-due mapping, and the two
    # would drift apart the first time someone edited one of them.
    report("state fields", 0.63)
    df = _apply_initial_state_fields(spec, df)

    report("derivations", 0.65)
    df = apply_derivations(spec, df, stage="book")

    report("finalise", 0.95)
    df = finalise(spec, df)
    report("done", 1.0)
    return df


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


def _sample_columns(
    spec: DesignSpec, n: int, rng: np.random.Generator, report: ProgressFn, id_offset: int = 0
) -> pd.DataFrame:
    df = pd.DataFrame(index=pd.RangeIndex(n))
    sampled = [c for c in spec.columns if c.generator is not None]
    for i, col in enumerate(sampled):
        try:
            df[col.name] = sample(col.generator, n, rng, df, id_offset)
        except Exception as exc:
            raise GenerationError(f"column {col.name!r} failed to sample: {exc}") from exc
        if sampled:
            report(f"sampling {col.name}", 0.5 * (i + 1) / len(sampled))
    return df


def _polish(
    spec: DesignSpec, df: pd.DataFrame, sample: pd.DataFrame | None
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Hand the book to a deep tabular model trained on the real tape.

    Refused rather than silently downgraded when there is no sample: a deep model
    trained on rule-based output can only learn the independence that output
    already has, so running it would cost minutes and change nothing.
    """
    from sdd.polish.ctgan import polish_book

    if sample is None or sample.empty:
        raise GenerationError(
            f"generation.method is {spec.generation.method!r}, which learns from the real tape, "
            "but no sample data was provided. Upload sample data, or choose a method that works "
            "from the schema alone (distribution, statistical, sampling, rule_based)."
        )
    polished, report = polish_book(
        df,
        spec,
        seed_data=sample,
        model=spec.generation.polish_model,
        epochs=spec.generation.polish_epochs,
    )
    return polished, report


def _apply_constants(spec: DesignSpec, df: pd.DataFrame) -> pd.DataFrame:
    for name, value in spec.constants.items():
        df[name] = value
    return df


def _apply_id_format(spec: DesignSpec, df: pd.DataFrame, id_offset: int = 0) -> pd.DataFrame:
    fmt = spec.entity.id_format
    if not fmt:
        return df
    context: dict[str, Any] = {**spec.params, **spec.constants}
    start = id_offset + 1
    try:
        df[spec.entity.id_column] = [
            fmt.format(seq=i, **context) for i in range(start, start + len(df))
        ]
    except KeyError as exc:
        raise GenerationError(
            f"entity.id_format {fmt!r} uses placeholder {exc} which is not in "
            f"`params` or `constants` (available: {sorted(context)})"
        ) from exc
    return df


def _apply_initial_state_fields(spec: DesignSpec, df: pd.DataFrame) -> pd.DataFrame:
    lc = spec.lifecycle
    if lc is None or not lc.state_fields:
        return df
    if lc.state_column not in df.columns:
        raise GenerationError(
            f"lifecycle.state_column {lc.state_column!r} is not produced at period 0; "
            "give it a generator so every entity starts in a known state"
        )
    from sdd.age.panel import apply_state_fields

    return apply_state_fields(spec, df, df[lc.state_column].to_numpy())


def _apply_initial_states(
    spec: DesignSpec, df: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    """Draw the period-0 state from the lifecycle's declared opening mix.

    Overwrites whatever the state column's generator produced. That is the point:
    a spec that says both should have one of them win, and the lifecycle is the
    more specific statement — a generator describes a column, an opening mix
    describes the book.
    """
    lc = spec.lifecycle
    assert lc is not None and lc.initial_distribution

    states = list(lc.initial_distribution)
    weights = np.array([lc.initial_distribution[s] for s in states], dtype=float)
    weights = weights / weights.sum()
    df[lc.state_column] = rng.choice(states, size=len(df), p=weights)
    return df


def apply_derivations(
    spec: DesignSpec, df: pd.DataFrame, *, stage: str, extra: dict[str, Any] | None = None
) -> pd.DataFrame:
    """Run the derivations whose ``stage`` matches, in declaration order."""
    env: dict[str, Any] = {**spec.params, **(extra or {})}
    for d in spec.derivations:
        if d.stage != stage and d.stage != "both":
            continue
        try:
            values = _derive_one(spec, d, df, env)
        except Exception as exc:
            raise GenerationError(f"derivation {d.target!r} failed: {exc}") from exc
        if d.round is not None and not isinstance(values, pd.Categorical):
            values = np.round(np.asarray(values, dtype=float), d.round)
        df[d.target] = values
        if d.dtype:
            df[d.target] = _coerce(df[d.target], d.dtype)
    return df


def _derive_one(spec: DesignSpec, d: Derivation, df: pd.DataFrame, env: dict[str, Any]) -> Any:
    if d.kind == "expr":
        assert d.expr
        return evaluate_on(d.expr, df, env)

    if d.kind == "when":
        assert d.rules
        n = len(df)
        out = np.full(n, d.else_, dtype=object)
        # Later rules must not overwrite earlier ones: first match wins.
        assigned = np.zeros(n, dtype=bool)
        for rule in d.rules:
            cond = evaluate_on(rule.if_, df, env)
            mask = np.asarray(cond, dtype=bool) if np.ndim(cond) else np.full(n, bool(cond))
            take = mask & ~assigned
            out[take] = rule.then
            assigned |= take
        return out

    if d.kind == "bucket":
        assert d.bucket and d.source
        return apply_bucket(spec.buckets[d.bucket], df[d.source])

    if d.kind == "format":
        assert d.template
        parts = {name: np.asarray(evaluate_on(e, df, env)) for name, e in d.args.items()}
        n = len(df)
        broadcast = {name: (np.full(n, v) if v.ndim == 0 else v) for name, v in parts.items()}
        return [d.template.format(**{k: v[i] for k, v in broadcast.items()}) for i in range(n)]

    raise GenerationError(f"unknown derivation kind {d.kind!r}")


def apply_bucket(bucket: Bucket, values: pd.Series) -> pd.Series:
    """Bin a numeric column into labelled bands."""
    return pd.cut(
        values,
        bins=bucket.bins,
        labels=bucket.labels,
        right=bucket.right,
        include_lowest=bucket.include_lowest,
    ).astype(str)


# ---------------------------------------------------------------------------
# dtypes and column order
# ---------------------------------------------------------------------------

_PANDAS_DTYPE: dict[DType, str] = {
    "int": "int64",
    "float": "float64",
    "str": "object",
    "bool": "bool",
    "category": "object",
}


def _coerce(series: pd.Series, dtype: DType) -> pd.Series:
    if dtype == "date":
        return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")
    if dtype == "int":
        # Round first: float -> int truncates, which quietly biases counters low.
        numeric = np.round(pd.to_numeric(series, errors="coerce"))
        # A blanked value has no integer, so the column becomes pandas' nullable
        # integer rather than failing. Only reached when missing values are on.
        return numeric.astype("Int64" if numeric.isna().any() else "int64")
    if dtype == "float":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return series.astype(_PANDAS_DTYPE[dtype])


def coerce_dtypes(spec: DesignSpec, df: pd.DataFrame) -> pd.DataFrame:
    for col in spec.columns:
        if col.dtype and col.name in df.columns:
            df[col.name] = _coerce(df[col.name], col.dtype)
    for ctr in spec.dynamics.counters:
        if ctr.dtype and ctr.column in df.columns:
            df[ctr.column] = _coerce(df[ctr.column], ctr.dtype)
    return df


def finalise(spec: DesignSpec, df: pd.DataFrame) -> pd.DataFrame:
    """Coerce dtypes and put the columns in output order.

    Helper columns are *retained* here — the ageing engine may still need them —
    and dropped only when writing to disk (see :func:`sdd.age.panel.to_output`).
    """
    df = coerce_dtypes(spec, df)
    ordered = spec.output_columns()
    missing = [c for c in ordered if c not in df.columns]
    if missing:
        raise GenerationError(
            f"spec declares output columns that generation never produced: {missing}"
        )
    tail = [c for c in df.columns if c not in ordered]
    return df[ordered + tail]
