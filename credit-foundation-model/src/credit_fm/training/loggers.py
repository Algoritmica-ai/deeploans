# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Pluggable training-metrics loggers (v1.1 G4c) — resolves decision DL-009.

The trainer keeps printing to stdout exactly as before (that stays the default, byte-identical);
these loggers add an OPTIONAL structured-metrics stream behind one config block::

    logging:
      backend: null          # null (default) | jsonl | tensorboard | wandb
      dir: runs/logs         # jsonl / tensorboard output directory (local)
      project: credit-fm     # wandb project name
      run_name: null         # wandb/tensorboard run name (null = backend default)
      mode: offline          # wandb mode — offline by default (DL-009 sovereign-cloud constraint)

DL-009 resolution baked in here: **no logger phones home unless explicitly asked to.**
``jsonl`` is the zero-dependency self-hosted option (one JSON object per line — plot with pandas);
``tensorboard`` uses the local event-file writer; ``wandb`` is strictly opt-in and defaults to
``offline`` mode (runs sync later with ``wandb sync`` if/when a hosted instance is approved).
All third-party imports are lazy — a backend's dependency is only needed if you select it.

Under DDP only rank 0 logs (the trainer guards this). Every backend implements the same tiny
interface: ``log_config(dict)`` once, ``log_metrics(step, dict)`` per step, ``finish(summary)``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


class NullLogger:
    """The default: no structured metrics — stdout printing alone (pre-G4c behavior)."""

    def log_config(self, config: dict) -> None:
        pass

    def log_metrics(self, step: int, metrics: dict[str, Any]) -> None:
        pass

    def finish(self, summary: dict | None = None) -> None:
        pass


class JsonlLogger:
    """Zero-dependency structured log: one JSON object per line in ``<dir>/<run_name>.jsonl``.

    The sovereign-cloud workhorse — no services, no packages, greppable, and
    ``pd.read_json(path, lines=True)`` turns it straight into a plot. Local paths only
    (object stores can't append); point ``dir`` at a mounted disk and copy up afterwards.
    """

    def __init__(self, dir: str = "runs/logs", run_name: str | None = None, **_):
        name = run_name or f"run_{time.strftime('%Y%m%d_%H%M%S')}"
        self.path = Path(dir) / f"{name}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self.path, "a")

    def _write(self, obj: dict) -> None:
        self._f.write(json.dumps(obj, default=str) + "\n")
        self._f.flush()                                   # crash-safe: every line lands on disk

    def log_config(self, config: dict) -> None:
        self._write({"event": "config", "config": config})

    def log_metrics(self, step: int, metrics: dict[str, Any]) -> None:
        self._write({"event": "metrics", "step": step, **metrics})

    def finish(self, summary: dict | None = None) -> None:
        self._write({"event": "finish", **(summary or {})})
        self._f.close()


class TensorBoardLogger:
    """Local TensorBoard event files via ``torch.utils.tensorboard`` (needs the tensorboard pkg)."""

    def __init__(self, dir: str = "runs/logs", run_name: str | None = None, **_):
        try:
            from torch.utils.tensorboard import SummaryWriter
        except ImportError as e:
            raise ImportError("logging.backend=tensorboard needs the 'tensorboard' package "
                              "(pip install tensorboard)") from e
        log_dir = str(Path(dir) / run_name) if run_name else str(dir)
        self._w = SummaryWriter(log_dir=log_dir)

    def log_config(self, config: dict) -> None:
        self._w.add_text("config", f"```json\n{json.dumps(config, indent=2, default=str)}\n```")

    def log_metrics(self, step: int, metrics: dict[str, Any]) -> None:
        for k, v in metrics.items():
            if isinstance(v, (int, float)):
                self._w.add_scalar(k, v, global_step=step)

    def finish(self, summary: dict | None = None) -> None:
        if summary:
            self.log_config({"summary": summary})
        self._w.close()


class WandbLogger:
    """Weights & Biases — strictly opt-in, and **offline by default** (DL-009).

    Offline runs write to ``./wandb/`` locally; sync later with ``wandb sync`` if a hosted or
    self-hosted instance is approved. Nothing leaves the machine unless ``mode: online`` is set
    explicitly. The wandb import is lazy (only needed if this backend is selected).
    """

    def __init__(self, project: str = "credit-fm", run_name: str | None = None,
                 mode: str = "offline", dir: str | None = None, **_):
        try:
            import wandb
        except ImportError as e:
            raise ImportError("logging.backend=wandb needs the 'wandb' package "
                              "(pip install wandb)") from e
        self._run = wandb.init(project=project, name=run_name, mode=mode or "offline",
                               dir=dir, reinit=True)

    def log_config(self, config: dict) -> None:
        self._run.config.update(config, allow_val_change=True)

    def log_metrics(self, step: int, metrics: dict[str, Any]) -> None:
        self._run.log(metrics, step=step)

    def finish(self, summary: dict | None = None) -> None:
        for k, v in (summary or {}).items():
            self._run.summary[k] = v
        self._run.finish()


_BACKENDS = {None: NullLogger, "null": NullLogger, "none": NullLogger,
             "jsonl": JsonlLogger, "tensorboard": TensorBoardLogger, "wandb": WandbLogger}


def build_logger(cfg: dict | None) -> NullLogger | JsonlLogger | TensorBoardLogger | WandbLogger:
    """Build a metrics logger from a ``logging:`` config block (missing/null → NullLogger).

    Recognised keys: ``backend``, ``dir``, ``run_name``, ``project``, ``mode``.
    """
    cfg = dict(cfg or {})
    backend = cfg.pop("backend", None)
    key = str(backend).lower() if backend is not None else None
    if key not in _BACKENDS:
        raise ValueError(f"logging.backend '{backend}' not one of "
                         f"{sorted(k for k in _BACKENDS if isinstance(k, str))} (or null)")
    cls = _BACKENDS[key]
    return cls(**cfg) if cls is not NullLogger else NullLogger()
