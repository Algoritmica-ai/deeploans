# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Load a fine-tuned Credit FM and score a portfolio panel.

The shared inference path used by ``scripts/score_portfolio.py`` and ``scripts/finetune.py``
(which reuses ``LoRALinear`` / ``apply_lora`` / ``observe_panel`` from here so the LoRA math and
the observation gate have a single source of truth).

Scoring is leakage-safe by construction: ``observe_panel`` truncates every loan's history to the
observation ``cutoff`` (the embedding only sees the past) and, with a gate column, keeps only loans
performing at that cutoff (score *new* defaults, not ones already in progress).
"""

from __future__ import annotations

import fsspec
import numpy as np
import pandas as pd
import torch
import torch.nn as nn

from credit_fm.data.collators import MLMCollator
from credit_fm.data.encode import encode_panel_parallel
from credit_fm.models import CreditFoundationModel
from credit_fm.utils import storage

_KEYS = ("input_ids", "event_index", "field_type", "branch")


class LoRALinear(nn.Module):
    """Frozen base Linear + trainable low-rank update ``B @ A`` (scaled)."""

    def __init__(self, base: nn.Linear, r: int, alpha: int):
        super().__init__()
        self.base = base
        for p in self.base.parameters():
            p.requires_grad = False
        self.lora_A = nn.Parameter(torch.zeros(r, base.in_features))
        self.lora_B = nn.Parameter(torch.zeros(base.out_features, r))
        nn.init.normal_(self.lora_A, std=0.02)
        self.scale = alpha / r

    def forward(self, x):
        return self.base(x) + (x @ self.lora_A.t() @ self.lora_B.t()) * self.scale


def apply_lora(model, r: int, alpha: int) -> None:
    """Wrap every encoder ``nn.Linear`` in a ``LoRALinear`` (in place).

    Collect the target Linears FIRST, then wrap — mutating during traversal would re-wrap the
    base Linear inside each new ``LoRALinear`` forever (infinite recursion).
    """
    targets = []
    for enc in (model.profile_encoder, model.event_encoder, model.history_encoder):
        for mod in enc.modules():
            for cname, child in mod.named_children():
                if isinstance(child, nn.Linear):
                    targets.append((mod, cname, child))
    for mod, cname, child in targets:
        setattr(mod, cname, LoRALinear(child, r, alpha))


def observe_panel(panel: pd.DataFrame, id_col: str, time_col: str, cutoff, gate_col=None,
                  gate_values=(True,)):
    """Truncate to history ``<= cutoff`` and (optionally) keep only gated-in loans at the cutoff.

    ``gate_values`` generalizes the gate beyond booleans (v1.1 G2.1): a loan is kept when its
    LAST row at/before the cutoff has ``gate_col`` in ``gate_values``. The default ``(True,)``
    reproduces the legacy boolean behavior exactly (NA gates → dropped); the Dutch panel gates on
    ``arrears_bucket in ["Performing", "1-29 DPD", ...]``.
    """
    dt = pd.to_datetime(panel[time_col], errors="coerce")
    panel = panel[dt <= pd.to_datetime(cutoff)]
    if gate_col is not None:
        last = panel.sort_values(time_col).groupby(id_col).tail(1)
        if tuple(gate_values) == (True,):                 # legacy boolean fast-path (NA -> False)
            gated = last[gate_col].fillna(False).astype(bool)
        else:
            gated = last[gate_col].isin(list(gate_values))
        keep = set(last.loc[gated, id_col])
        panel = panel[panel[id_col].isin(keep)]
    return panel


def load_finetuned(path: str, key=None):
    """Rebuild a Credit FM from a fine-tuned checkpoint and load its weights.

    The checkpoint (written by ``finetune.py --save``) is ``{"config", "model", "finetune"}``.
    A LoRA checkpoint needs the adapters re-inserted before ``load_state_dict`` — the ``finetune``
    metadata carries the mode + rank/alpha. Returns ``(model.eval(), finetune_meta)``.
    """
    storage.ensure_auth(path, key)
    with fsspec.open(path, "rb") as f:
        ckpt = torch.load(f, map_location="cpu", weights_only=False)
    c = ckpt["config"]
    meta = ckpt.get("finetune", {}) or {}
    model = CreditFoundationModel(
        c["vocab_size"], c["n_field_types"], dim=c["dim"], n_heads=c["n_heads"],
        profile_layers=c["profile_layers"], event_layers=c["event_layers"],
        history_layers=c["history_layers"])
    if meta.get("mode") == "lora" and meta.get("lora"):
        apply_lora(model, meta["lora"]["rank"], meta["lora"]["alpha"])
    model.load_state_dict(ckpt["model"])
    model.eval()
    return model, meta


def _samples_from_shard(shard: pd.DataFrame) -> list[dict]:
    out = []
    for _, r in shard.iterrows():
        s = {k: torch.tensor(r[k], dtype=torch.long) for k in _KEYS}
        s["n_events"] = int(r["n_events"])
        out.append(s)
    return out


@torch.no_grad()
def _predict_pd(model, samples, collate, device, bsz, use_amp) -> np.ndarray:
    """P(default) per sample = softmax(classify)[:, 1]."""
    model.eval()
    out = []
    for i in range(0, len(samples), bsz):
        batch = collate(samples[i:i + bsz])
        batch = {k: (v.to(device) if torch.is_tensor(v) else v) for k, v in batch.items()}
        with torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
            out.append(model.classify(batch).float().softmax(-1)[:, 1].cpu().numpy())
    return np.concatenate(out) if out else np.empty(0)


def score_panel(model, tok, tokenizer_path: str, panel: pd.DataFrame, id_col: str, time_col: str,
                cutoff, gate_col=None, *, limit: int = 0, workers: int = 0, engine: str = "cpu",
                key=None, device: str = "cpu", bsz: int = 256, use_amp: bool = False) -> pd.DataFrame:
    """Score one portfolio panel at ``cutoff``: one row per (gated) loan with a default score.

    Returns ``[id_col, score, n_events, cutoff]`` (empty frame if no loan is observable).
    """
    obs = observe_panel(panel, id_col, time_col, cutoff, gate_col)
    if limit:
        obs = obs[obs[id_col].isin(obs[id_col].drop_duplicates().head(limit))]
    if len(obs) == 0:
        return pd.DataFrame({id_col: [], "score": [], "n_events": [], "cutoff": []})
    shard = encode_panel_parallel(tok, tokenizer_path, obs, workers=workers, key=key, engine=engine)
    probs = _predict_pd(model, _samples_from_shard(shard), MLMCollator(vocab_size=tok.vocab_size,
                        mask=False), device, bsz, use_amp)
    return pd.DataFrame({
        id_col: shard[id_col].to_numpy(),
        "score": probs.astype("float64"),
        "n_events": shard["n_events"].to_numpy(),
        "cutoff": str(pd.to_datetime(cutoff).date()),
    })
