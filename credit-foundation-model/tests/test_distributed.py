# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Distributed-training tests (v1.1 G4b).

Two layers: (1) the no-op single-process contract (the DDP helpers must be inert when no process
group exists, so the single-GPU path is unchanged); (2) a real **2-process gloo** smoke test that
spawns two ranks and checks DistributedDataParallel actually synchronises gradients — the property
that makes multi-GPU training correct. The smoke test runs in a subprocess so its multiprocessing
never entangles pytest's own process.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import torch

from credit_fm.training.distributed import DistInfo, all_reduce_mean, barrier, unwrap

REPO = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------- no-op single-process contract

def test_distinfo_single_process_defaults():
    info = DistInfo()
    assert info.world_size == 1 and info.rank == 0
    assert info.is_main and not info.is_distributed


def test_unwrap_returns_plain_model_untouched():
    model = torch.nn.Linear(3, 2)
    assert unwrap(model) is model                    # nothing to strip when not DDP-wrapped


def test_barrier_and_all_reduce_are_noops_without_group():
    barrier()                                        # must not raise when no process group
    assert all_reduce_mean(3.5, "cpu") == 3.5        # returns the value unchanged


# ---------------------------------------------------------------- real 2-process gloo smoke

_SMOKE = textwrap.dedent('''
    import os, torch, torch.distributed as dist
    import torch.multiprocessing as mp
    from torch.nn.parallel import DistributedDataParallel as DDP

    def worker(rank, world_size):
        os.environ.update(MASTER_ADDR="127.0.0.1", MASTER_PORT="29541",
                          RANK=str(rank), WORLD_SIZE=str(world_size), LOCAL_RANK=str(rank))
        from credit_fm.training.distributed import (init_distributed, cleanup_distributed,
                                                    unwrap, all_reduce_mean, barrier)
        info = init_distributed(device="cpu")
        assert info.is_distributed and info.world_size == world_size and info.rank == rank

        torch.manual_seed(0)                       # identical init on both ranks
        model = torch.nn.Linear(4, 2)
        ddp = DDP(model)
        # rank-specific data -> different LOCAL grads; DDP must all-reduce them to be IDENTICAL
        x = torch.randn(8, 4) + rank
        y = torch.randint(0, 2, (8,))
        torch.nn.functional.cross_entropy(ddp(x), y).backward()

        g = unwrap(ddp).weight.grad.clone()
        others = [torch.zeros_like(g) for _ in range(world_size)]
        dist.all_gather(others, g)
        assert torch.allclose(others[0], others[1]), "DDP did NOT synchronise gradients"
        assert torch.isfinite(g).all()

        m = all_reduce_mean(float(rank), "cpu")    # (0 + 1) / 2 == 0.5 on both ranks
        assert abs(m - 0.5) < 1e-6, m
        barrier()
        if rank == 0:
            print("DDP SMOKE OK")
        cleanup_distributed()

    if __name__ == "__main__":
        mp.spawn(worker, args=(2,), nprocs=2, join=True)
''')


def _run_ddp_script(tmp_path, name, body, ok_marker):
    script = tmp_path / name
    script.write_text(body)
    proc = subprocess.run(
        [sys.executable, str(script)], cwd=REPO, capture_output=True, text=True,
        env={"PYTHONPATH": str(REPO / "src"), "PATH": "/usr/bin:/bin", "OMP_NUM_THREADS": "1",
             # DDP init imports torch._dynamo -> onnx; NGC image's onnx breaks under protobuf 4.x
             "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION": "python"})
    blob = proc.stdout + "\n" + proc.stderr
    if proc.returncode != 0 and "Descriptors cannot be created directly" in blob:
        pytest.skip("container onnx/protobuf incompatibility on DDP import (unrelated to our code; "
                    "pretrain.py sets PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python for the real run)")
    assert proc.returncode == 0, blob[-3000:]
    assert ok_marker in proc.stdout


def test_ddp_gloo_two_process_gradients_synchronise(tmp_path):
    _run_ddp_script(tmp_path, "ddp_smoke.py", _SMOKE, "DDP SMOKE OK")


# The real regression for the "classification_head unused in MLM" bug: run the ACTUAL model through
# train_mlm under 2-process DDP. Without find_unused_parameters=True this raises the DDP reducer
# error on step 2 ("parameters that were not used in producing loss") — this test would have caught it.
_TRAIN_SMOKE = textwrap.dedent('''
    import json, os, tempfile
    import numpy as np, pandas as pd
    import torch.multiprocessing as mp

    CONFIG = {"id_col": "loan_id", "time_col": "reporting_date", "time_field": "loan_age",
              "profile": {"numeric": ["original_ltv"], "categorical": ["channel"]},
              "event": {"numeric": ["current_interest_rate", "current_upb"], "categorical": []},
              "n_bins": 8, "max_categories": 64, "max_events": 60, "calendar": "yearquarter"}

    def _panel(n=12, months=5):
        rng = np.random.default_rng(0); rows = []
        for lid in range(n):
            ltv = int(rng.integers(40, 97)); chan = rng.choice(["R", "C", "B"])
            rate = float(rng.uniform(3, 8))
            for m in range(months):
                rows.append({"loan_id": f"L{lid}", "reporting_date": f"2020-{m+1:02d}-28",
                             "loan_age": 12 + m, "original_ltv": ltv, "channel": chan,
                             "current_interest_rate": rate, "current_upb": 200000 - m * 1000})
        return pd.DataFrame(rows)

    def worker(rank, world):
        os.environ.update(MASTER_ADDR="127.0.0.1", MASTER_PORT="29543",
                          RANK=str(rank), WORLD_SIZE=str(world), LOCAL_RANK=str(rank))
        from credit_fm.training.distributed import init_distributed, cleanup_distributed
        from credit_fm.training import train_mlm
        from credit_fm.data import CreditDataModule
        from credit_fm.data.encode import iter_shards
        from credit_fm.models import CreditFoundationModel
        from credit_fm.tokenizer import KVTTokenizer
        from credit_fm.utils import storage
        info = init_distributed(device="cpu")
        tok = KVTTokenizer(CONFIG).fit(_panel())
        d = tempfile.mkdtemp()                          # each rank builds an identical local copy
        names = []
        for i, sh in enumerate(iter_shards(tok, _panel(), 8)):
            n = f"shard-{i:05d}.parquet"
            storage.write_parquet(sh, storage.join(d, n)); names.append(n)
        storage.write_text(json.dumps({"vocab_size": tok.vocab_size, "shards": names}),
                           storage.join(d, "manifest.json"))
        dm = CreditDataModule(d, batch_size=4)
        model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=16, n_heads=2,
                                      profile_layers=1, event_layers=1, history_layers=1)
        # 3 steps: without find_unused_parameters the classification_head trips DDP on step 2
        hist = train_mlm(model, dm, steps=3, grad_accum=2, lr=1e-3, warmup=1, device="cpu",
                         log_every=0, dist_info=info)
        assert len(hist["train"]) == 3 and all(x == x for x in hist["train"])
        if rank == 0:
            print("DDP TRAIN OK")
        cleanup_distributed()

    if __name__ == "__main__":
        mp.spawn(worker, args=(2,), nprocs=2, join=True)
''')


def test_ddp_real_model_trains_with_unused_head(tmp_path):
    _run_ddp_script(tmp_path, "ddp_train_smoke.py", _TRAIN_SMOKE, "DDP TRAIN OK")
