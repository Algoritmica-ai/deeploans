"""Optional sampling backend: NVIDIA NeMo Data Designer.

Upstream deeploans routed all sampling through NeMo Data Designer. This project
makes numpy the default — the package should install and run without a heavy
dependency — but keeps the NeMo path available for anyone already invested in it,
or who wants the richer column types it offers (LLM-backed text, structured
outputs, embeddings) alongside the samplers.

Enable with::

    pip install -e '.[nemo]'
    sdd run <spec> --backend nemo

The contract is identical either way: same spec in, same columns out. Only the
sampling changes, so a spec is portable between backends and the invariant and
fidelity checks apply unchanged.

Two things this backend cannot promise:

**Bit-identical output.**
    NeMo and numpy do not share a random number stream, so the same seed gives
    statistically equivalent but not identical data.

**Every generator kind.**
    Only the kinds with a NeMo equivalent are translated; anything else falls
    back to numpy for that column and says so. Silently substituting a different
    distribution would be worse than a mixed run.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sdd.spec.schema import (
    BernoulliGen,
    CategoricalGen,
    ConditionalCategoricalGen,
    DesignSpec,
    GaussianGen,
    ScipyGen,
    UUIDGen,
)


class NemoUnavailable(RuntimeError):
    """The NeMo backend was requested but is not installed."""


def _require_nemo() -> tuple[Any, Any]:
    try:
        import data_designer.config as dd
        from data_designer.interface import DataDesigner
    except ImportError as exc:  # pragma: no cover - depends on the extra
        raise NemoUnavailable(
            "the 'nemo' backend needs NVIDIA NeMo Data Designer, which is not installed.\n"
            "  pip install -e '.[nemo]'\n"
            "The default numpy backend needs no extra dependencies and produces the "
            "same columns."
        ) from exc
    return dd, DataDesigner


def sample_columns_nemo(spec: DesignSpec, n: int, *, seed: int = 42) -> pd.DataFrame:
    """Sample the spec's generator columns through NeMo Data Designer.

    Columns NeMo cannot express are drawn with numpy instead and listed in
    ``df.attrs['numpy_fallback']`` so the caller can report a mixed run rather
    than pretend it was pure.
    """
    dd, DataDesigner = _require_nemo()

    builder = dd.DataDesignerConfigBuilder()
    translated: list[str] = []
    fallback: list[str] = []

    for column in spec.columns:
        if column.generator is None:
            continue
        config = _translate(dd, column.name, column.generator)
        if config is None:
            fallback.append(column.name)
            continue
        builder.add_column(config)
        translated.append(column.name)

    designer = DataDesigner()
    result = designer.create(config_builder=builder, num_records=n, dataset_name=spec.meta.name)
    df = (result.load_dataset() if hasattr(result, "load_dataset") else result.dataset).copy()
    df = df.reset_index(drop=True)

    # Fill anything NeMo could not express, in declaration order so conditional
    # generators still see their parents.
    if fallback:
        from sdd.generate.samplers import sample

        rng = np.random.default_rng(seed)
        for column in spec.columns:
            if column.name in fallback and column.generator is not None:
                df[column.name] = sample(column.generator, n, rng, df)

    df.attrs["backend"] = "nemo"
    df.attrs["nemo_columns"] = translated
    df.attrs["numpy_fallback"] = fallback
    return df


def _translate(dd: Any, name: str, generator: Any) -> Any | None:
    """Map one spec generator onto a NeMo column config, or None if it has no
    equivalent."""
    match generator:
        case CategoricalGen():
            return dd.SamplerColumnConfig(
                name=name,
                sampler_type=dd.SamplerType.CATEGORY,
                params=dd.CategorySamplerParams(
                    values=list(generator.values), weights=generator.weights
                ),
            )
        case ConditionalCategoricalGen():
            return dd.SamplerColumnConfig(
                name=name,
                sampler_type=dd.SamplerType.SUBCATEGORY,
                params=dd.SubcategorySamplerParams(
                    category=generator.parent, values=generator.mapping
                ),
            )
        case ScipyGen():
            return dd.SamplerColumnConfig(
                name=name,
                sampler_type=dd.SamplerType.SCIPY,
                params=dd.ScipySamplerParams(
                    dist_name=generator.dist,
                    dist_params=dict(generator.params),
                    decimal_places=generator.decimals,
                ),
            )
        case GaussianGen():
            return dd.SamplerColumnConfig(
                name=name,
                sampler_type=dd.SamplerType.GAUSSIAN,
                params=dd.GaussianSamplerParams(
                    mean=generator.mean,
                    stddev=generator.stddev,
                    decimal_places=generator.decimals,
                ),
            )
        case BernoulliGen():
            # NeMo's Bernoulli emits 0/1; custom labels are applied by the
            # derivation layer, so only the plain form maps cleanly.
            if generator.true_value == 1 and generator.false_value == 0:
                return dd.SamplerColumnConfig(
                    name=name,
                    sampler_type=dd.SamplerType.BERNOULLI,
                    params=dd.BernoulliSamplerParams(p=generator.p),
                )
            return None
        case UUIDGen():
            return dd.SamplerColumnConfig(
                name=name,
                sampler_type=dd.SamplerType.UUID,
                params=dd.UUIDSamplerParams(
                    prefix=generator.prefix,
                    short_form=generator.short,
                    uppercase=generator.uppercase,
                ),
            )
        case _:
            # empirical, uniform, sequence, constant — numpy handles these.
            return None
