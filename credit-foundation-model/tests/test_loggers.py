# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Metrics-logger tests (v1.1 G4c) — the factory, the jsonl backend, and trainer integration.

The default (backend null) must be a strict no-op so pre-G4c behavior is byte-identical; the
jsonl backend must produce parseable, crash-safe lines; and the trainer must emit metrics at the
same cadence it prints (rank-0 only under DDP — inert loggers elsewhere).
"""

from __future__ import annotations

import json

import pytest

from credit_fm.training.loggers import JsonlLogger, NullLogger, WandbLogger, build_logger


def test_null_logger_is_a_strict_noop():
    lg = NullLogger()
    lg.log_config({"a": 1})
    lg.log_metrics(1, {"train/loss": 1.0})
    lg.finish({"best_val": None})                        # nothing raised, nothing produced


def test_build_logger_defaults_to_null():
    assert isinstance(build_logger(None), NullLogger)
    assert isinstance(build_logger({}), NullLogger)
    assert isinstance(build_logger({"backend": None}), NullLogger)
    assert isinstance(build_logger({"backend": "null", "dir": "x"}), NullLogger)


def test_build_logger_unknown_backend_is_actionable():
    with pytest.raises(ValueError, match="jsonl"):
        build_logger({"backend": "mlflow"})


def test_jsonl_logger_writes_parseable_crash_safe_lines(tmp_path):
    lg = build_logger({"backend": "jsonl", "dir": str(tmp_path), "run_name": "toy"})
    assert isinstance(lg, JsonlLogger)
    lg.log_config({"model": {"dim": 16}})
    lg.log_metrics(1, {"train/loss": 6.5, "train/lr": 3e-7})
    lg.log_metrics(50, {"train/loss": 3.6})
    # crash-safety: lines are flushed BEFORE finish — readable even if the run dies here
    mid = [json.loads(x) for x in (tmp_path / "toy.jsonl").read_text().splitlines()]
    assert [e["event"] for e in mid] == ["config", "metrics", "metrics"]
    lg.log_metrics(100, {"val/loss": 0.68})
    lg.finish({"best_val": 0.68, "best_step": 100})

    events = [json.loads(x) for x in (tmp_path / "toy.jsonl").read_text().splitlines()]
    assert events[0]["config"]["model"]["dim"] == 16
    assert events[1] == {"event": "metrics", "step": 1, "train/loss": 6.5, "train/lr": 3e-7}
    assert events[-1] == {"event": "finish", "best_val": 0.68, "best_step": 100}


def test_tensorboard_backend_lazy_import():
    pytest.importorskip("tensorboard", reason="tensorboard not installed (backend is optional)")
    lg = build_logger({"backend": "tensorboard", "dir": "/tmp/tb_test"})
    lg.log_metrics(1, {"train/loss": 1.0})
    lg.finish()


def test_wandb_backend_import_error_is_actionable(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def no_wandb(name, *a, **k):
        if name == "wandb":
            raise ImportError("No module named 'wandb'")
        return real_import(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", no_wandb)
    with pytest.raises(ImportError, match="wandb"):
        WandbLogger()


def test_trainer_emits_metrics_at_print_cadence(tmp_path):
    """train_mlm feeds the logger at log_every/val points and finishes with the summary."""
    import numpy as np
    import pandas as pd

    from credit_fm.data import CreditDataModule
    from credit_fm.data.encode import iter_shards
    from credit_fm.models import CreditFoundationModel
    from credit_fm.tokenizer import KVTTokenizer
    from credit_fm.training import train_mlm
    from credit_fm.utils import storage

    CONFIG = {"id_col": "loan_id", "time_col": "reporting_date", "time_field": "loan_age",
              "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
              "event": {"numeric": ["current_upb"], "categorical": []},
              "n_bins": 8, "max_categories": 64, "max_events": 60, "calendar": "yearquarter"}
    rng = np.random.default_rng(0)
    rows = [{"loan_id": f"L{i}", "reporting_date": f"2020-{m+1:02d}-28", "loan_age": 12 + m,
             "original_ltv": int(rng.integers(40, 97)), "channel": rng.choice(["R", "C"]),
             "current_upb": 200_000 - m * 1_000} for i in range(10) for m in range(4)]
    panel = pd.DataFrame(rows)
    tok = KVTTokenizer(CONFIG).fit(panel)
    train_dir, val_dir = str(tmp_path / "tr"), str(tmp_path / "va")
    for d in (train_dir, val_dir):
        names = []
        for i, sh in enumerate(iter_shards(tok, panel, 8)):
            n = f"shard-{i:05d}.parquet"
            storage.write_parquet(sh, storage.join(d, n))
            names.append(n)
        storage.write_text(json.dumps({"vocab_size": tok.vocab_size, "shards": names}),
                           storage.join(d, "manifest.json"))

    class Recorder:
        def __init__(self):
            self.metrics, self.finished = [], None

        def log_config(self, c):
            pass

        def log_metrics(self, step, m):
            self.metrics.append((step, m))

        def finish(self, s=None):
            self.finished = s

    rec = Recorder()
    dm = CreditDataModule(train_dir, val_dir=val_dir, batch_size=4)
    model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                  profile_layers=1, event_layers=1, history_layers=1)
    train_mlm(model, dm, steps=10, lr=1e-3, warmup=2, device="cpu", log_every=5, val_every=5,
              metrics_logger=rec)

    train_steps = [s for s, m in rec.metrics if "train/loss" in m]
    val_steps = [s for s, m in rec.metrics if "val/loss" in m]
    assert train_steps == [1, 5, 10]                      # same cadence as the prints
    assert val_steps == [5, 10]
    assert rec.finished is not None and "best_val" in rec.finished
