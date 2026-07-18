# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Extract per-loan ``[USR]`` embeddings from a pretrained checkpoint.

Runs a processed monthly panel (tokenizer schema) through the frozen model and writes one row per
loan: ``loan_id`` + the ``dim`` embedding columns (``e0..e{dim-1}``) + carried columns + ``n_events``.

Leakage control lives here: ``observe.cutoff`` truncates each loan's history to reporting dates
``<= cutoff`` (so the embedding only "sees" the past), and ``observe.gate_col: is_performing``
keeps only loans performing at that cutoff (the "predict *new* defaults" gate). The downstream
label (default within the forward window) is computed later, in ``evaluate_downstream.py``.

Config-driven (recipe: ``configs/fannie_mae/extract.yaml``)::

    python scripts/extract_embeddings.py -c configs/fannie_mae/extract.yaml
    python scripts/extract_embeddings.py -c configs/fannie_mae/extract.yaml \
        --limit 1000 --observe.cutoff 2017-06-30 --out gs://.../m3_jun2017.parquet
"""

from __future__ import annotations

import time

import fsspec
import numpy as np
import pandas as pd
import torch

from credit_fm.data.collators import MLMCollator
from credit_fm.data.encode import encode_panel_parallel
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def load_checkpoint(path: str, key: str):
    """Rebuild the model from a checkpoint (state_dict + config) and set it to eval mode."""
    storage.ensure_auth(path, key)
    with fsspec.open(path, "rb") as f:
        ckpt = torch.load(f, map_location="cpu", weights_only=False)
    c = ckpt["config"]
    model = CreditFoundationModel(
        c["vocab_size"], c["n_field_types"], dim=c["dim"], n_heads=c["n_heads"],
        profile_layers=c["profile_layers"], event_layers=c["event_layers"],
        history_layers=c["history_layers"])
    model.load_state_dict(ckpt["model"])
    return model.eval(), c


def observe_panel(panel: pd.DataFrame, id_col: str, time_col: str,
                  cutoff: str | None, gate_col: str | None) -> pd.DataFrame:
    """Truncate history to ``<= cutoff`` and (optionally) keep only loans passing the gate at cutoff."""
    if cutoff is not None:
        dt = pd.to_datetime(panel[time_col], errors="coerce")
        panel = panel[dt <= pd.to_datetime(cutoff)]
    if gate_col is not None:
        last = panel.sort_values(time_col).groupby(id_col).tail(1)     # most recent kept row per loan
        keep = set(last.loc[last[gate_col].fillna(False).astype(bool), id_col])
        panel = panel[panel[id_col].isin(keep)]
    return panel


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/extract.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'checkpoint', 'panel', 'observe', 'batch_size', 'out')}", flush=True)

    device = cfg.runtime.device or ("cuda" if torch.cuda.is_available() else "cpu")
    tok = KVTTokenizer.load(cfg.tokenizer)
    model, mc = load_checkpoint(cfg.checkpoint, cfg.key)
    model.to(device)
    dim = mc["dim"]
    print(f"model: dim={dim}, {model.num_parameters()/1e6:.1f}M params on {device}", flush=True)

    storage.ensure_auth(cfg.panel, cfg.key)
    panel = storage.read_parquet(cfg.panel)
    cutoff, gate_col = cfg.observe.cutoff, cfg.observe.gate_col
    panel = observe_panel(panel, tok.id_col, tok.time_col, cutoff, gate_col)
    if cfg.limit:
        keep = panel[tok.id_col].drop_duplicates().head(cfg.limit)
        panel = panel[panel[tok.id_col].isin(keep)]
    print(f"panel: {panel[tok.id_col].nunique():,} loans "
          f"(cutoff={cutoff}, gate={gate_col})", flush=True)

    if panel.empty:
        raise SystemExit("no loans after cutoff/gate — run the diagnostic: check "
                         "reporting_date range/format and is_performing values")
    carry = list(cfg.get_path("carry") or [])
    carried = panel.groupby(tok.id_col)[carry].first() if carry else None

    shard = encode_panel_parallel(tok, cfg.tokenizer, panel,         # per-loan token arrays
                                  workers=cfg.get_path("workers", 0), key=cfg.key,
                                  engine=cfg.get_path("engine", "cpu"))
    collate = MLMCollator(vocab_size=tok.vocab_size, mask=False)      # no masking — inference
    use_amp = cfg.runtime.bf16 and device.startswith("cuda")

    t0, embs = time.time(), []
    for start in range(0, len(shard), cfg.batch_size):
        rows = shard.iloc[start:start + cfg.batch_size]
        samples = []
        for _, r in rows.iterrows():
            s = {k: torch.tensor(r[k], dtype=torch.long)
                 for k in ("input_ids", "event_index", "field_type", "branch")}
            s["n_events"] = int(r["n_events"])
            samples.append(s)
        batch = collate(samples)
        batch = {k: (v.to(device) if torch.is_tensor(v) else v) for k, v in batch.items()}
        with torch.no_grad(), torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
            embs.append(model.extract_embeddings(batch).float().cpu().numpy())
    emb = np.concatenate(embs, axis=0)

    out = pd.DataFrame({tok.id_col: shard[tok.id_col].to_numpy(),
                        "n_events": shard["n_events"].to_numpy()})
    out = pd.concat([out, pd.DataFrame(emb, columns=[f"e{i}" for i in range(dim)])], axis=1)
    if carried is not None:
        out = out.merge(carried, left_on=tok.id_col, right_index=True, how="left")

    storage.write_parquet(out, cfg.out)
    print(f"wrote {len(out):,} loan embeddings (dim={dim}) -> {cfg.out}  "
          f"({time.time()-t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
