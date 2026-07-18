# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Dataset contract — parse and validate ``configs/<asset>/dataset.yaml`` (v1.1 G1.1).

``dataset.yaml`` is the single onboarding artifact for an asset: the column contract
(``id_col`` / ``time_col`` / ``origination_col``), the declarative ``labels:`` (task targets),
and the machine-enforced ``leakage:`` / ``exclude:`` lists. Every pipeline consumer reads these
through :func:`load_dataset_config`; nothing re-declares them.

Contract rules enforced at load time (fail fast, with actionable messages):

* required keys present and well-typed;
* every label is a known ``type`` with a positive ``horizon_months``;
* a label's ``event_col`` and ``gate_col`` **must themselves be listed under** ``leakage:`` —
  the label is the answer, so it can never be a feature;
* ``leakage:`` and ``exclude:`` do not overlap (a column has exactly one reason to be dropped).

``resolve_leakage_exclude`` is the migration shim: consumers that historically read the lists
from ``baseline.yaml`` fall through to the dataset contract when the old keys are absent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

VALID_LABEL_TYPES = {"forward_event"}   # v1.1: did <event_col> fire within horizon after cutoff?


def _fail(path: str | Path, msg: str) -> None:
    raise ValueError(f"{path}: {msg}")


@dataclass(frozen=True)
class LabelSpec:
    """One declarative task target (see docstring for the contract)."""

    name: str
    type: str
    event_col: str
    horizon_months: int
    event_value: Any = True                 # panel value that counts as "the event fired"
    gate_col: str | None = None             # observe only rows where gate_col in gate_values
    gate_values: tuple = (True,)


@dataclass(frozen=True)
class DatasetConfig:
    """Parsed + validated dataset.yaml."""

    name: str
    adapter: str                            # registry key ("generic" = already-conforming panel)
    id_col: str
    time_col: str
    origination_col: str
    origination_derived: bool               # True: split derives it (e.g. Dutch, DL-007)
    labels: dict[str, LabelSpec] = field(default_factory=dict)
    leakage: frozenset = frozenset()        # outcome-encoding columns — never features
    exclude: frozenset = frozenset()        # structural non-features (ids, dates, geo, text)
    schema: str | None = None               # tokenizer field-schema path
    path: str = ""                          # where this config was loaded from (lineage)

    @property
    def banned(self) -> frozenset:
        """All columns that must never reach a feature schema."""
        return self.leakage | self.exclude


def _parse_label(path, name: str, raw: dict) -> LabelSpec:
    if not isinstance(raw, dict):
        _fail(path, f"labels.{name}: expected a mapping, got {type(raw).__name__}")
    ltype = raw.get("type")
    if ltype not in VALID_LABEL_TYPES:
        _fail(path, f"labels.{name}.type: '{ltype}' not one of {sorted(VALID_LABEL_TYPES)}")
    event_col = raw.get("event_col")
    if not isinstance(event_col, str) or not event_col:
        _fail(path, f"labels.{name}.event_col: required (the boolean/flag panel column)")
    horizon = raw.get("horizon_months")
    if not isinstance(horizon, int) or horizon <= 0:
        _fail(path, f"labels.{name}.horizon_months: required positive int, got {horizon!r}")
    gate_values = raw.get("gate_values", [True])
    if not isinstance(gate_values, list) or not gate_values:
        _fail(path, f"labels.{name}.gate_values: expected a non-empty list")
    return LabelSpec(name=name, type=ltype, event_col=event_col, horizon_months=horizon,
                     event_value=raw.get("event_value", True),
                     gate_col=raw.get("gate_col"), gate_values=tuple(gate_values))


def load_dataset_config(path: str | Path) -> DatasetConfig:
    """Load + validate a ``dataset.yaml``; raise ``ValueError`` with the offending key on error."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"dataset config not found: {p}")
    raw = yaml.safe_load(p.read_text()) or {}

    ds = raw.get("dataset")
    if not isinstance(ds, dict):
        _fail(p, "top-level 'dataset:' mapping is required")
    for key in ("name", "adapter", "id_col", "time_col", "origination_col"):
        if not isinstance(ds.get(key), str) or not ds.get(key):
            _fail(p, f"dataset.{key}: required non-empty string")

    for key in ("leakage", "exclude"):
        val = raw.get(key, [])
        if not isinstance(val, list) or not all(isinstance(c, str) for c in val):
            _fail(p, f"{key}: expected a list of column names")
    leakage, exclude = frozenset(raw.get("leakage", [])), frozenset(raw.get("exclude", []))
    overlap = leakage & exclude
    if overlap:
        _fail(p, f"columns in BOTH leakage and exclude (pick one reason): {sorted(overlap)}")

    labels = {name: _parse_label(p, name, spec) for name, spec in (raw.get("labels") or {}).items()}
    for spec in labels.values():
        # the label's own ingredients are, by definition, leakage — enforce, don't trust
        if spec.event_col not in leakage:
            _fail(p, f"labels.{spec.name}.event_col '{spec.event_col}' must also be listed under "
                     f"leakage: (the label is the answer — it can never be a feature)")
        if spec.gate_col and spec.gate_col not in leakage:
            _fail(p, f"labels.{spec.name}.gate_col '{spec.gate_col}' must also be listed under "
                     f"leakage: (contemporaneous state — it encodes the outcome)")

    return DatasetConfig(
        name=ds["name"], adapter=ds["adapter"], id_col=ds["id_col"], time_col=ds["time_col"],
        origination_col=ds["origination_col"],
        origination_derived=bool(ds.get("origination_derived", False)),
        labels=labels, leakage=leakage, exclude=exclude,
        schema=raw.get("schema"), path=str(p))


def resolve_leakage_exclude(cfg: dict, config_path: str | Path = "") -> tuple[list, list]:
    """Migration shim: return ``(exclude, leakage)`` for consumers of the old baseline.yaml keys.

    Old-style configs that still carry the lists keep working (with a deprecation note); configs
    that instead carry ``dataset: <path>`` read the contract. One release of grace, then the old
    keys go.
    """
    if "leakage" in cfg or "exclude" in cfg:
        if "dataset" in cfg:
            print("note: config carries BOTH inline leakage/exclude lists and a dataset: pointer;"
                  " using the inline lists (deprecated — move to dataset.yaml)", flush=True)
        return list(cfg.get("exclude", [])), list(cfg.get("leakage", []))
    ds_path = cfg.get("dataset")
    if not ds_path:
        raise ValueError(f"{config_path or 'config'}: needs either exclude:/leakage: lists "
                         f"(deprecated) or a dataset: pointer to dataset.yaml")
    ds = load_dataset_config(ds_path)
    return sorted(ds.exclude), sorted(ds.leakage)
