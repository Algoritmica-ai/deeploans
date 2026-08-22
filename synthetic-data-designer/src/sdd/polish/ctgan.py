"""Optional deep-generative polish, between generation and ageing.

The rule-based path fits every column on its own. That is fast, explainable, and
editable — but it cannot reproduce how columns move *together*, because nothing
in a set of marginals knows that a loan-to-value is a balance divided by a
valuation. Measured on the RMBS round trip, marginals come back within a couple
of percentage points while the largest pairwise correlation is off by ~1.0.

A deep tabular generator (CTGAN or TVAE, via SDV) learns the joint distribution
directly. Given a *real* seed dataset it can recover structure the marginal path
misses. It is optional, and off by default, for three honest reasons:

1. It needs a real seed. Trained on rule-based output it can only learn the
   independence that output already has.
2. It is a black box. The calibration knobs that make a spec auditable —
   "reduce this transition cell to halve defaults" — do not survive it.
3. It is slow and needs a GPU to be comfortable at scale.

So the sequence is: rule-based sample first, deep polish second, and the
fidelity report before and after so the value is measured rather than assumed.
If polishing does not move the numbers, it has earned nothing and should be
switched off.

Enable with::

    pip install 'sdd[deep]'
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from sdd.spec.schema import DesignSpec

# Columns never worth handing to a deep model: identifiers are unique by
# construction and dates are handled by the calendar.
SKIP_ROLES = ("constant",)


class DeepUnavailable(RuntimeError):
    """The polish step was requested but SDV is not installed."""


def _require_sdv() -> Any:
    try:
        from sdv.metadata import SingleTableMetadata  # noqa: F401
        from sdv.single_table import CTGANSynthesizer, TVAESynthesizer  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on the extra
        raise DeepUnavailable(
            "the deep polish step needs SDV, which is not installed.\n"
            "  pip install 'sdd[deep]'\n"
            "Generation works without it; the polish step only refines an existing sample."
        ) from exc
    import sdv

    return sdv


def polish_book(
    book: pd.DataFrame,
    spec: DesignSpec,
    *,
    seed_data: pd.DataFrame,
    model: str = "ctgan",
    epochs: int = 300,
    columns: list[str] | None = None,
    verbose: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Resample a book's joint structure from a real seed dataset.

    ``seed_data`` is the *real* tape. The model learns from it and produces the
    same number of rows as ``book``, which then replaces the polished columns.
    Columns outside ``columns`` are carried through untouched, so identifiers,
    dates and deal constants stay exactly as the spec produced them.

    Returns the polished book and a report describing what was done — including
    the before/after fidelity, which is the only way to tell whether the step
    was worth running.
    """
    _require_sdv()
    from sdv.metadata import SingleTableMetadata
    from sdv.single_table import CTGANSynthesizer, TVAESynthesizer

    from sdd.validate import compare

    targets = columns or _polishable_columns(spec, book, seed_data)
    if not targets:
        return book, {"polished": [], "reason": "no columns were eligible"}

    training = seed_data[targets].dropna()
    if len(training) < 100:
        return book, {
            "polished": [],
            "reason": f"seed has only {len(training)} usable rows; too few to train on",
        }

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(training)

    synthesizer_cls = {"ctgan": CTGANSynthesizer, "tvae": TVAESynthesizer}.get(model)
    if synthesizer_cls is None:
        raise ValueError(f"unknown model {model!r}; expected 'ctgan' or 'tvae'")

    synthesizer = synthesizer_cls(metadata, epochs=epochs, verbose=verbose)
    synthesizer.fit(training)
    sampled = synthesizer.sample(num_rows=len(book)).reset_index(drop=True)

    before = compare(seed_data[targets], book[targets])
    polished = book.copy().reset_index(drop=True)
    for column in targets:
        polished[column] = sampled[column]
    after = compare(seed_data[targets], polished[targets])

    return polished, {
        "model": model,
        "epochs": epochs,
        "polished": targets,
        "training_rows": len(training),
        "fidelity_before": {
            "columns_failed": len(before.failures),
            "correlation_delta": before.correlation_delta,
        },
        "fidelity_after": {
            "columns_failed": len(after.failures),
            "correlation_delta": after.correlation_delta,
        },
        # The whole point of the step. If this is not clearly positive, the
        # polish is costing time and interpretability for nothing.
        "improved": (
            after.correlation_delta is not None
            and before.correlation_delta is not None
            and after.correlation_delta < before.correlation_delta
        ),
    }


def _polishable_columns(spec: DesignSpec, book: pd.DataFrame, seed_data: pd.DataFrame) -> list[str]:
    """Columns worth learning jointly: present in both, sampled, and not an id."""
    excluded = {spec.entity.id_column, spec.entity.time_column} | set(spec.constants)
    return [
        column.name
        for column in spec.columns
        if column.name not in excluded
        and column.role not in SKIP_ROLES
        and column.name in book.columns
        and column.name in seed_data.columns
    ]
