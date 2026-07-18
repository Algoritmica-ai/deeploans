# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""train_mlm tests — the loop drives loss down on real shards through the CreditDataModule.

Writes tiny synthetic train/val shard dirs, builds the datamodule + a tiny model, and runs a short
training loop on CPU, asserting the loss falls and validation is recorded.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from credit_fm.data import CreditDataModule
from credit_fm.data.encode import iter_shards
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.training import build_optimizer, train_mlm
from credit_fm.utils import storage

CONFIG = {
    "id_col": "loan_id",
    "time_col": "reporting_date",
    "time_field": "loan_age",
    "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
    "event": {"numeric": ["current_interest_rate", "current_upb"], "categorical": []},
    "n_bins": 8,
    "max_categories": 64,
    "max_events": 60,
    "calendar": "yearquarter",
}


def _panel(n_loans, n_months=5) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for lid in range(n_loans):
        ltv = int(rng.integers(40, 97))
        chan = rng.choice(["R", "C", "B"])
        rate = float(rng.uniform(3, 8))
        for m in range(n_months):
            rows.append({
                "loan_id": f"L{lid}", "reporting_date": f"2020-{m+1:02d}-28",
                "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                "current_interest_rate": rate, "current_upb": 200_000 - m * 1_000,
            })
    return pd.DataFrame(rows)


def _write_split(tok, panel, out_dir, shard_size=8):
    names = []
    for i, shard in enumerate(iter_shards(tok, panel, shard_size)):
        name = f"shard-{i:05d}.parquet"
        storage.write_parquet(shard, storage.join(out_dir, name))
        names.append(name)
    storage.write_text(json.dumps({"vocab_size": tok.vocab_size, "shards": names}),
                       storage.join(out_dir, "manifest.json"))


def test_build_optimizer_splits_decay_groups():
    tok = KVTTokenizer(CONFIG).fit(_panel(4))
    model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                  profile_layers=1, event_layers=1, history_layers=1)
    opt = build_optimizer(model, lr=1e-3, weight_decay=0.1)
    decay, no_decay = opt.param_groups
    assert decay["weight_decay"] == 0.1 and no_decay["weight_decay"] == 0.0
    assert len(decay["params"]) and len(no_decay["params"])


def test_train_mlm_reduces_loss_and_logs_val(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel(12))
    train_dir, val_dir = str(tmp_path / "train"), str(tmp_path / "val")
    _write_split(tok, _panel(12), train_dir)
    _write_split(tok, _panel(6), val_dir)

    dm = CreditDataModule(train_dir, val_dir=val_dir, batch_size=4)
    model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                  profile_layers=1, event_layers=1, history_layers=1)
    history = train_mlm(model, dm, steps=60, lr=2e-3, warmup=5, device="cpu",
                        log_every=0, val_every=30)
    assert len(history["train"]) == 60
    # windowed means (single-step loss is noisy across shuffled batches) + the val trend
    assert np.mean(history["train"][-10:]) < np.mean(history["train"][:10])
    assert len(history["val"]) == 2 and history["val"][-1][1] < history["val"][0][1]
    # best-val tracking: records the lowest val loss seen
    assert history["best_val"] == min(v for _, v in history["val"])
    assert history["best_step"] in {s for s, _ in history["val"]}


class _CountingLoader:
    """Wrap a DataLoader, counting every micro-batch yielded. Re-iterable (the trainer cycles it)."""

    def __init__(self, dl, counter):
        self._dl, self._counter = dl, counter

    def __iter__(self):
        for b in self._dl:
            self._counter["n"] += 1
            yield b


def _count_micro_batches(dm, counter):
    orig = dm.train_dataloader()                 # a real, re-iterable DataLoader (called once)
    dm.train_dataloader = lambda: _CountingLoader(orig, counter)


def test_grad_accum_consumes_micro_batches_and_reduces_loss(tmp_path):
    """grad_accum=N runs N micro-batches per optimiser step (pulls N× batches) and still trains."""
    tok = KVTTokenizer(CONFIG).fit(_panel(12))
    train_dir = str(tmp_path / "train")
    _write_split(tok, _panel(12), train_dir)

    pulled = {"n": 0}
    dm = CreditDataModule(train_dir, batch_size=4)
    _count_micro_batches(dm, pulled)

    model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                  profile_layers=1, event_layers=1, history_layers=1)
    history = train_mlm(model, dm, steps=30, grad_accum=3, lr=2e-3, warmup=5, device="cpu",
                        log_every=0)
    assert len(history["train"]) == 30                       # 30 optimiser steps
    assert pulled["n"] == 30 * 3                             # each step consumed 3 micro-batches
    assert np.isfinite(history["train"]).all()
    assert np.mean(history["train"][-8:]) < np.mean(history["train"][:8])   # still learns


def test_grad_accum_one_pulls_one_batch_per_step(tmp_path):
    """grad_accum=1 (the default) consumes exactly one micro-batch per optimiser step."""
    tok = KVTTokenizer(CONFIG).fit(_panel(10))
    train_dir = str(tmp_path / "train")
    _write_split(tok, _panel(10), train_dir)

    pulled = {"n": 0}
    dm = CreditDataModule(train_dir, batch_size=4)
    _count_micro_batches(dm, pulled)

    model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                  profile_layers=1, event_layers=1, history_layers=1)
    train_mlm(model, dm, steps=10, lr=1e-3, warmup=2, device="cpu", log_every=0)   # grad_accum default
    assert pulled["n"] == 10                                  # one micro-batch per step


# ------------------------------------------------------- mid-run checkpoint / resume (v1.1 G4a)

def _tiny(tok):
    return CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                 profile_layers=1, event_layers=1, history_layers=1)


def _train_dir(tok, tmp_path):
    train_dir = str(tmp_path / "train")
    _write_split(tok, _panel(10), train_dir)
    return train_dir


def test_step_checkpoints_written_and_rotated(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel(10))
    dm = CreditDataModule(_train_dir(tok, tmp_path), batch_size=4)
    out = str(tmp_path / "runs" / "toy.pt")
    (tmp_path / "runs").mkdir()
    train_mlm(_tiny(tok), dm, steps=10, lr=1e-3, warmup=2, device="cpu", log_every=0,
              checkpoint_every=2, checkpoint_keep=2, checkpoint_out=out)
    files = sorted(f.name for f in (tmp_path / "runs").glob("toy.pt.step*.pt"))
    assert files == ["toy.pt.step000008.pt", "toy.pt.step000010.pt"]   # rotated: keep newest 2


def test_resume_noop_restores_exact_state(tmp_path):
    """resume with steps == saved step trains 0 new steps and restores weights/history/RNG."""
    import torch
    tok = KVTTokenizer(CONFIG).fit(_panel(10))
    train_dir = _train_dir(tok, tmp_path)
    out = str(tmp_path / "toy.pt")

    model_a = _tiny(tok)
    hist_a = train_mlm(model_a, CreditDataModule(train_dir, batch_size=4), steps=10, lr=1e-3,
                       warmup=2, device="cpu", log_every=0,
                       checkpoint_every=5, checkpoint_out=out)
    rng_after_a = torch.get_rng_state()

    model_b = _tiny(tok)                                  # different random init...
    hist_b = train_mlm(model_b, CreditDataModule(train_dir, batch_size=4), steps=10, lr=1e-3,
                       warmup=2, device="cpu", log_every=0,
                       checkpoint_every=5, checkpoint_out=out, resume="auto")
    assert hist_b["resumed_from"] == 10 and hist_b["train"] == hist_a["train"]
    for k, v in model_a.state_dict().items():             # ...but restored to A's exact weights
        assert torch.equal(v, model_b.state_dict()[k]), k
    assert torch.equal(rng_after_a, torch.get_rng_state())   # RNG stream restored too


def test_resume_auto_continues_training(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel(10))
    train_dir = _train_dir(tok, tmp_path)
    out = str(tmp_path / "toy.pt")

    hist_a = train_mlm(_tiny(tok), CreditDataModule(train_dir, batch_size=4), steps=10, lr=1e-3,
                       warmup=2, device="cpu", log_every=0,
                       checkpoint_every=5, checkpoint_out=out)
    hist_b = train_mlm(_tiny(tok), CreditDataModule(train_dir, batch_size=4), steps=14, lr=1e-3,
                       warmup=2, device="cpu", log_every=0,
                       checkpoint_every=5, checkpoint_out=out, resume="auto")
    assert hist_b["resumed_from"] == 10
    assert len(hist_b["train"]) == 14                     # 10 restored + 4 new optimiser steps
    assert hist_b["train"][:10] == hist_a["train"]        # restored history is verbatim
    assert np.isfinite(hist_b["train"]).all()


def test_resume_auto_cold_start_when_nothing_saved(tmp_path):
    tok = KVTTokenizer(CONFIG).fit(_panel(10))
    dm = CreditDataModule(_train_dir(tok, tmp_path), batch_size=4)
    hist = train_mlm(_tiny(tok), dm, steps=5, lr=1e-3, warmup=2, device="cpu", log_every=0,
                     checkpoint_every=2, checkpoint_out=str(tmp_path / "fresh.pt"), resume="auto")
    assert "resumed_from" not in hist and len(hist["train"]) == 5
