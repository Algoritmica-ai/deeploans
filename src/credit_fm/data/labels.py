# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Declarative task labels — build targets from the dataset contract (v1.1 G2.1).

A prediction task is **configuration, not code**: the ``labels:`` block of ``dataset.yaml``
declares *what* to predict (``forward_event``: did ``event_col`` fire within ``horizon_months``
after the observation cutoff, among entities gated-in by ``gate_col``/``gate_values``), and this
module turns a :class:`~credit_fm.data.dataset_config.LabelSpec` into concrete label sets.

Consumers (``finetune.py``, ``evaluate_downstream.py``) reference a task by name::

    task:
      label: default_12m          # resolved from dataset.yaml's labels: block
      train_cutoffs: [...]
      test_cutoffs: [...]

The legacy inline keys (``label_col`` / ``gate_col`` / ``horizon_months`` / ``label_value`` /
``gate_values``) are still honored for one release — :func:`resolve_label_spec` synthesizes a
spec from them and prints a deprecation note.

``forward_event_entities`` is the former ``finetune.forward_default_loans`` promoted to the
package and generalized (any event column/value, not just boolean ``default_event``) — the golden
equivalence with the old logic is pinned in ``tests/test_labels.py``.
"""

from __future__ import annotations

import pandas as pd

from .dataset_config import DatasetConfig, LabelSpec, load_dataset_config


def forward_event_entities(panel: pd.DataFrame, spec: LabelSpec, *,
                           id_col: str, time_col: str, cutoff) -> set:
    """Entity ids whose ``spec.event_col`` fires in ``(cutoff, cutoff + horizon_months]``.

    ``event_value is True`` uses the boolean fast-path (``fillna(False).astype(bool)`` — the exact
    legacy ``forward_default_loans`` semantics, safe for pandas nullable booleans); any other
    ``event_value`` matches by equality (e.g. the Dutch ``default_crr_flag == 'Y'``).
    """
    lo = pd.to_datetime(cutoff)
    hi = lo + pd.DateOffset(months=spec.horizon_months)
    dt = pd.to_datetime(panel[time_col], errors="coerce")
    col = panel[spec.event_col]
    if spec.event_value is True:
        fired = col.fillna(False).astype(bool)
    else:
        fired = (col == spec.event_value).fillna(False)
    hit = panel[(dt > lo) & (dt <= hi) & fired]
    return set(hit[id_col])


def resolve_label_spec(cfg, dataset_cfg: DatasetConfig | None = None) -> LabelSpec:
    """Resolve the task's :class:`LabelSpec` from a stage config.

    Two paths:

    * **contract (preferred)** — ``task.label: <name>`` names a spec in the dataset contract
      (loaded from ``cfg['dataset']`` unless ``dataset_cfg`` is passed).
    * **legacy (deprecated)** — inline ``task.label_col`` (+ ``gate_col`` / ``horizon_months`` /
      ``label_value`` / ``gate_values``) synthesizes a one-off spec, with a deprecation note.
    """
    def get(dotted, default=None):
        cur = cfg
        for part in dotted.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    name = get("task.label")
    if name:
        ds = dataset_cfg
        if ds is None:
            ds_path = get("dataset")
            if not ds_path:
                raise ValueError("task.label needs a dataset: pointer in the config "
                                 "(the labels live in dataset.yaml)")
            ds = load_dataset_config(ds_path)
        if name not in ds.labels:
            raise ValueError(f"task.label '{name}' not defined in {ds.path or 'dataset.yaml'} "
                             f"(available: {sorted(ds.labels)})")
        return ds.labels[name]

    label_col = get("task.label_col")
    if not label_col:
        raise ValueError("task needs either label: <name> (from dataset.yaml) or the legacy "
                         "label_col/horizon_months keys")
    horizon = get("task.horizon_months")
    if not isinstance(horizon, int) or horizon <= 0:
        raise ValueError(f"task.horizon_months: required positive int, got {horizon!r}")
    print("note: task.label_col/gate_col/horizon_months are deprecated — declare the task under "
          "dataset.yaml labels: and reference it as task.label", flush=True)
    return LabelSpec(name=f"legacy_{label_col}", type="forward_event", event_col=label_col,
                     horizon_months=horizon, event_value=get("task.label_value", True),
                     gate_col=get("task.gate_col"),
                     gate_values=tuple(get("task.gate_values", [True])))
