# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Fine-tune the pretrained FM on the forward-default task — the fair test of the FM's value.

The frozen-embedding probe (``evaluate_downstream.py``) is the *handicap* match. This adapts the
pretrained weights to the label, three ways, and reports test ROC/PR against the features bar:

* ``frozen`` — freeze the encoder, train only the classification head on cached ``[USR]`` embeddings
  (a proper task-loss linear/MLP probe; usually stronger than XGBoost-on-embeddings).
* ``lora``   — freeze the encoder, insert low-rank adapters into its linear layers + train the head
  (cheap adaptation; the standard FM-efficiency path).
* ``full``   — fine-tune the whole encoder + head at a low LR (the strongest adaptation).

Observation + label match the baseline: observe at ``task.cutoff`` (performing gate, history <=
cutoff, no leakage), label = default within ``task.horizon_months``.

Two evaluation protocols:

* **loan-holdout** (default) — one ``task.cutoff``; loans split randomly into train/test. Both
  sides live in the same period, so this measures representation quality, not generalization
  across time.
* **calendar-OOT** — set ``task.train_cutoffs`` (past) + ``task.test_cutoffs`` (future). The head
  trains on observations whose label windows end before the test era and is scored on later
  cutoffs: train on the past, score the future — the honest deployment test. Loans observed in
  both periods are hash-assigned wholly to one side (loan-disjoint), matching
  ``build_oot_baseline.py`` so the features bar is apples-to-apples.

Class imbalance is handled explicitly (rare-event training destabilizes otherwise — the M4 run
collapsed at 0.11% base rate with raw inverse-frequency weighting): ``train.neg_per_pos``
downsamples FIT negatives (monitor/test sets untouched, so ranking metrics stay honest;
predicted probabilities become uncalibrated by design), and ``train.pos_weight_cap`` bounds the
loss weight. A 10% monitoring split at the TRUE class balance reports val ROC every epoch, so a
collapsing run is visible after epoch 1.

Config-driven (recipe: ``configs/mortgage_performance/finetune.yaml``)::

    python scripts/finetune.py -c configs/mortgage_performance/finetune.yaml                 # lora (default)
    python scripts/finetune.py -c configs/mortgage_performance/finetune.yaml \
        --mode frozen --report reports/ft_frozen.md
    python scripts/finetune.py -c configs/mortgage_performance/finetune.yaml \
        --mode full --train.lr 1e-5 --report reports/ft_full.md
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import yaml
from sklearn.metrics import average_precision_score, roc_auc_score

from credit_fm.data.collators import MLMCollator
from credit_fm.data.encode import encode_panel_parallel
from credit_fm.data.labels import forward_event_entities, resolve_label_spec
from credit_fm.inference.scoring import apply_lora, observe_panel
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize
from credit_fm.utils.reproducibility import set_seed

_KEYS = ("input_ids", "event_index", "field_type", "branch")


def load_checkpoint(path, key):
    storage.ensure_auth(path, key)
    with fsspec.open(path, "rb") as f:
        ckpt = torch.load(f, map_location="cpu", weights_only=False)
    c = ckpt["config"]
    model = CreditFoundationModel(
        c["vocab_size"], c["n_field_types"], dim=c["dim"], n_heads=c["n_heads"],
        profile_layers=c["profile_layers"], event_layers=c["event_layers"],
        history_layers=c["history_layers"])
    model.load_state_dict(ckpt["model"])
    return model, c


def _safe_roc(y, p):
    return roc_auc_score(y, p) if 0 < y.sum() < len(y) else float("nan")


def build_samples(shard, defaulted, id_col):
    samples, ys = [], []
    for _, r in shard.iterrows():
        s = {k: torch.tensor(r[k], dtype=torch.long) for k in _KEYS}
        s["n_events"] = int(r["n_events"])
        samples.append(s)
        ys.append(int(r[id_col] in defaulted))
    return samples, np.array(ys, dtype=np.int64)


def cutoff_samples(tok, cfg, spec, panel, id_col, time_col, cutoff):
    """Observation samples + forward labels + loan ids for one cutoff (label = the LabelSpec)."""
    defaulted = forward_event_entities(panel, spec, id_col=id_col, time_col=time_col, cutoff=cutoff)
    obs = observe_panel(panel, id_col, time_col, cutoff, spec.gate_col, spec.gate_values)
    if cfg.get_path("limit"):
        keep = obs[id_col].drop_duplicates().head(cfg.limit)
        obs = obs[obs[id_col].isin(keep)]
    shard = encode_panel_parallel(tok, cfg.tokenizer, obs,
                                  workers=cfg.get_path("workers", 0), key=cfg.key,
                                  engine=cfg.get_path("engine", "cpu"))
    samples, y = build_samples(shard, defaulted, id_col)
    return samples, y, shard[id_col].to_numpy()


def _to_device(batch, device):
    return {k: (v.to(device) if torch.is_tensor(v) else v) for k, v in batch.items()}


@torch.no_grad()
def embed_all(model, samples, collate, device, bsz, use_amp):
    model.eval()
    out = []
    for i in range(0, len(samples), bsz):
        b = _to_device(collate(samples[i:i + bsz]), device)
        with torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
            out.append(model.extract_embeddings(b).float())
    return torch.cat(out)


@torch.no_grad()
def predict_full(model, samples, collate, device, bsz, use_amp):
    model.eval()
    out = []
    for i in range(0, len(samples), bsz):
        b = _to_device(collate(samples[i:i + bsz]), device)
        with torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
            out.append(model.classify(b).float().softmax(-1)[:, 1])
    return torch.cat(out).cpu().numpy()


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/mortgage_performance/finetune.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'checkpoint', 'panel', 'task', 'mode', 'lora', 'train', 'split', 'report')}",
          flush=True)
    if cfg.mode not in ("frozen", "lora", "full"):
        raise SystemExit(f"mode must be frozen|lora|full, got '{cfg.mode}'")

    set_seed(cfg.seed)
    device = cfg.get_path("runtime.device") or ("cuda" if torch.cuda.is_available() else "cpu")
    use_amp = cfg.get_path("runtime.bf16", False) and device.startswith("cuda")
    epochs, bsz = cfg.train.epochs, cfg.train.batch_size
    lr = cfg.train.lr or {"frozen": 1e-3, "lora": 5e-4, "full": 2e-5}[cfg.mode]
    cutoff = cfg.get_path("task.cutoff")
    spec = resolve_label_spec(cfg)                       # task.label from dataset.yaml (or legacy keys)
    horizon = spec.horizon_months
    print(f"task: {spec.name} — {spec.event_col}"
          + (f"=={spec.event_value!r}" if spec.event_value is not True else "")
          + f" within {horizon}mo, gate {spec.gate_col or 'none'}", flush=True)

    schema = yaml.safe_load(open(cfg.schema))
    id_col, time_col = schema["id_col"], schema["time_col"]
    tok = KVTTokenizer.load(cfg.tokenizer)
    model, base_config = load_checkpoint(cfg.checkpoint, cfg.key)

    storage.ensure_auth(cfg.panel, cfg.key)
    panel = storage.read_parquet(cfg.panel)
    rng = np.random.default_rng(cfg.seed)

    test_cutoffs = cfg.get_path("task.test_cutoffs")
    if test_cutoffs:
        # ---- calendar-OOT: head trains on past cutoffs, is scored on future ones ----
        train_cutoffs = [str(c) for c in cfg.task.train_cutoffs]
        test_cutoffs = [str(c) for c in test_cutoffs]
        when = f"{train_cutoffs[0]}..{train_cutoffs[-1]} -> {', '.join(test_cutoffs)}"
        pool, y_parts, loan_parts = [], [], []
        for co in train_cutoffs:
            s, yy, lids = cutoff_samples(tok, cfg, spec, panel, id_col, time_col, co)
            pool += s
            y_parts.append(yy)
            loan_parts.append(lids)
            print(f"  train cutoff {co}: {len(s):,} obs, {yy.mean()*100:.2f}% positive", flush=True)
        te_samples, yte_parts, te_loan_parts = [], [], []
        for co in test_cutoffs:
            s, yy, lids = cutoff_samples(tok, cfg, spec, panel, id_col, time_col, co)
            te_samples += s
            yte_parts.append(yy)
            te_loan_parts.append(lids)
            print(f"  test cutoff {co}: {len(s):,} obs, {yy.mean()*100:.2f}% positive", flush=True)
        y_pool = np.concatenate(y_parts)
        y_te = np.concatenate(yte_parts)
        tr_loans = np.concatenate(loan_parts)
        te_loans = np.concatenate(te_loan_parts)
        # loan-disjoint guard: a loan observed in both eras goes wholly to one side (hash).
        # Use hashtable membership (set / pd.isin), NOT np.isin on string arrays — at OOT scale
        # almost every loan spans both eras, so np.isin's O(n*m) string scan runs for hours.
        overlap = set(pd.unique(tr_loans)) & set(pd.unique(te_loans))
        if overlap:
            ov = pd.Series(sorted(overlap))
            to_test = set(ov[pd.util.hash_pandas_object(ov, index=False).to_numpy() % 2 == 0])
            to_train = overlap - to_test
            keep_tr = ~pd.Series(tr_loans).isin(to_test).to_numpy()      # hashtable lookup: O(n)
            keep_te = ~pd.Series(te_loans).isin(to_train).to_numpy()
            pool = [pool[i] for i in np.flatnonzero(keep_tr)]
            y_pool = y_pool[keep_tr]
            te_samples = [te_samples[i] for i in np.flatnonzero(keep_te)]
            y_te = y_te[keep_te]
            print(f"  loan-disjoint: {len(overlap):,} loans span both eras (hash-split)", flush=True)
        print(f"obs train {len(y_pool):,} ({y_pool.mean()*100:.2f}%) | "
              f"test {len(y_te):,} ({y_te.mean()*100:.2f}%) | mode={cfg.mode} lr={lr}", flush=True)
    else:
        # ---- single-cutoff loan-holdout ----
        when = cutoff
        samples, y, _ = cutoff_samples(tok, cfg, spec, panel, id_col, time_col, cutoff)
        print(f"loans {len(y):,} | positive rate {y.mean()*100:.2f}% | mode={cfg.mode} lr={lr}",
              flush=True)
        is_test = rng.random(len(samples)) < cfg.split.test_frac
        te_idx = np.flatnonzero(is_test)
        te_samples = [samples[i] for i in te_idx]
        y_te = y[te_idx]
        pool = [samples[i] for i in np.flatnonzero(~is_test)]
        y_pool = y[~is_test]

    # 10% monitoring split at the TRUE class balance — val ROC is printed every epoch
    perm = rng.permutation(len(pool))
    n_va = max(int(0.1 * len(perm)), 1)
    va_idx, fit_idx = perm[:n_va], perm[n_va:]
    va_samples = [pool[i] for i in va_idx]
    y_va = y_pool[va_idx]
    fit_samples = [pool[i] for i in fit_idx]
    y_fit = y_pool[fit_idx]

    # downsample FIT negatives only (ranking metrics honest; probabilities uncalibrated by design)
    npp = cfg.get_path("train.neg_per_pos", 0)
    if npp:
        pos = np.flatnonzero(y_fit == 1)
        neg = np.flatnonzero(y_fit == 0)
        keep = rng.permutation(np.concatenate(
            [pos, rng.choice(neg, min(len(neg), len(pos) * npp), replace=False)]))
        fit_samples = [fit_samples[i] for i in keep]
        y_fit = y_fit[keep]

    n_pos = max(int(y_fit.sum()), 1)
    pos_w = (len(y_fit) - n_pos) / n_pos
    cap = cfg.get_path("train.pos_weight_cap")
    if cap:
        pos_w = min(pos_w, float(cap))
    weight = torch.tensor([1.0, float(pos_w)], device=device)
    celoss = nn.CrossEntropyLoss(weight=weight)
    collate = MLMCollator(vocab_size=tok.vocab_size, mask=False)
    model.to(device)
    print(f"  fit {len(y_fit):,} ({n_pos:,} pos, neg_per_pos={npp or 'all'}) | "
          f"monitor {len(y_va):,} | test {len(y_te):,} | pos_weight {pos_w:.0f}", flush=True)

    if cfg.mode == "frozen":
        for p in model.parameters():
            p.requires_grad = False
        head = model.classification_head
        for p in head.parameters():
            p.requires_grad = True
        efit = embed_all(model, fit_samples, collate, device, bsz, use_amp)
        eva = embed_all(model, va_samples, collate, device, bsz, use_amp)
        ete = embed_all(model, te_samples, collate, device, bsz, use_amp)
        yfit_t = torch.tensor(y_fit, device=device)
        opt = torch.optim.Adam(head.parameters(), lr=lr)
        best_roc, best_state = float("-inf"), None
        for ep in range(epochs):
            head.train()
            order = torch.randperm(len(efit), device=device)
            tot, nb = 0.0, 0
            for i in range(0, len(efit), bsz):
                idx = order[i:i + bsz]
                loss = celoss(head(efit[idx]), yfit_t[idx])
                opt.zero_grad(set_to_none=True)
                loss.backward()
                opt.step()
                tot += loss.item()
                nb += 1
            head.eval()
            with torch.no_grad():
                pva = head(eva).float().softmax(-1)[:, 1].cpu().numpy()
            v = _safe_roc(y_va, pva)
            if v == v and v > best_roc:                       # nan-safe best-epoch tracking
                best_roc, best_state = v, copy.deepcopy(head.state_dict())
            print(f"  epoch {ep+1}/{epochs}  avg loss {tot/max(nb,1):.4f}  val ROC {v:.4f}",
                  flush=True)
        if best_state is not None:
            head.load_state_dict(best_state)
            print(f"  restored best epoch (val ROC {best_roc:.4f})", flush=True)
        with torch.no_grad():
            probs = head(ete).float().softmax(-1)[:, 1].cpu().numpy()
    else:
        if cfg.mode == "lora":
            apply_lora(model, cfg.lora.rank, cfg.lora.alpha)
            model.to(device)
            for p in model.parameters():
                p.requires_grad = False
            for n, p in model.named_parameters():
                if "lora_" in n or n.startswith("classification_head"):
                    p.requires_grad = True
        params = [p for p in model.parameters() if p.requires_grad]
        n_train = sum(p.numel() for p in params)
        print(f"  trainable params: {n_train/1e6:.2f}M", flush=True)
        opt = torch.optim.AdamW(params, lr=lr, weight_decay=cfg.train.weight_decay)
        yfit_t = torch.tensor(y_fit, device=device)
        best_roc, best_state = float("-inf"), None
        for ep in range(epochs):
            model.train()
            order = np.random.default_rng(ep).permutation(len(fit_samples))
            tot, nb = 0.0, 0
            for i in range(0, len(order), bsz):
                idx = order[i:i + bsz]
                b = _to_device(collate([fit_samples[j] for j in idx]), device)
                yb = yfit_t[torch.tensor(idx, device=device)]
                with torch.autocast("cuda", dtype=torch.bfloat16, enabled=use_amp):
                    loss = celoss(model.classify(b), yb)
                opt.zero_grad(set_to_none=True)
                loss.backward()
                if cfg.train.grad_clip:
                    torch.nn.utils.clip_grad_norm_(params, cfg.train.grad_clip)
                opt.step()
                tot += loss.item()
                nb += 1
            pva = predict_full(model, va_samples, collate, device, bsz, use_amp)
            v = _safe_roc(y_va, pva)
            if v == v and v > best_roc:                       # nan-safe best-epoch tracking
                best_roc, best_state = v, copy.deepcopy(model.state_dict())
            print(f"  epoch {ep+1}/{epochs}  avg loss {tot/max(nb,1):.4f}  val ROC {v:.4f}",
                  flush=True)
        if best_state is not None:
            model.load_state_dict(best_state)
            print(f"  restored best epoch (val ROC {best_roc:.4f})", flush=True)
        probs = predict_full(model, te_samples, collate, device, bsz, use_amp)

    roc = roc_auc_score(y_te, probs)
    pr = average_precision_score(y_te, probs)
    bar = cfg.get_path("features_bar")
    bar_txt = f"   (features bar {bar:.4f})" if bar else ""
    print(f"\n=== Fine-tune ({cfg.mode}) — {spec.name}: {spec.event_col} within {horizon}mo of {when} ===")
    print(f"  test: {len(y_te):,} loans, {y_te.mean()*100:.2f}% positive")
    print(f"  ROC-AUC {roc:.4f} | PR-AUC {pr:.4f}{bar_txt}")

    if cfg.get_path("report"):
        rep = Path(cfg.report)
        rep.parent.mkdir(parents=True, exist_ok=True)
        bar_row = f"| features bar (ROC) | {bar:.4f} |\n" if bar else ""
        rep.write_text(
            f"# FM fine-tune ({cfg.mode}) — {spec.name} ({spec.event_col} within {horizon}mo) of {when}\n\n"
            f"Test {len(y_te):,} loans ({y_te.mean()*100:.2f}% positive), loan-disjoint; "
            f"fit set neg_per_pos={cfg.get_path('train.neg_per_pos', 0) or 'all'}, "
            f"pos_weight {pos_w:.0f}.\n\n"
            f"| metric | value |\n|---|--:|\n| ROC-AUC | {roc:.4f} |\n| PR-AUC | {pr:.4f} |\n"
            + bar_row)
        print(f"wrote {rep}")

    # persist the fine-tuned model so it can be served (scripts/score_portfolio.py). model.state_dict()
    # after best-epoch restore captures the deployable weights in all three modes (frozen head /
    # LoRA adapters / full); the finetune meta records what score_portfolio needs to rebuild it.
    save_path = cfg.get_path("save")
    if save_path:
        val_roc = float(best_roc) if np.isfinite(best_roc) else None
        ft_meta = {
            "mode": cfg.mode,
            "lora": ({"rank": cfg.lora.rank, "alpha": cfg.lora.alpha} if cfg.mode == "lora" else None),
            "task": {"label": spec.name, "label_col": spec.event_col,
                     "event_value": spec.event_value, "gate_col": spec.gate_col,
                     "gate_values": list(spec.gate_values),
                     "horizon_months": horizon, "window": str(when)},
            "metrics": {"val_roc": val_roc, "test_roc": float(roc), "test_ap": float(pr)},
            "base_checkpoint": cfg.checkpoint, "tokenizer": cfg.tokenizer, "schema": cfg.schema,
            "n_test": int(len(y_te)),
        }
        storage.ensure_auth(save_path, cfg.key)
        with fsspec.open(save_path, "wb") as f:
            torch.save({"config": base_config, "model": model.state_dict(), "finetune": ft_meta}, f)
        storage.write_text(json.dumps(ft_meta, indent=2, default=str),
                           str(save_path).rsplit(".", 1)[0] + "_meta.json")
        print(f"saved fine-tuned model -> {save_path}")


if __name__ == "__main__":
    main()
