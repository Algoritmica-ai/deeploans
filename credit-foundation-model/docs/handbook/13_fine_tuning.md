# Part 13 — Fine-Tuning: Cashing In the Pretrained Knowledge

> **You are here:**  raw ─▶ ingest ─▶ validate ─▶ split ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ [FINE-TUNE] ─▶ score ─▶ calibrate ─▶ serve


> File: `scripts/finetune.py` · recipes `configs/mortgage_performance/finetune*.yaml` · LoRA machinery in
> `src/credit_fm/inference/scoring.py` (single source of truth, shared with serving).

## 13.1 What fine-tuning is

**Plain English:** the pretrained model can *read* loans fluently but has never been asked a
question. Fine-tuning teaches it one specific question — "will this loan default within 12
months?" — using labeled examples. Because it already understands the language, it needs far
fewer examples than a model starting from scratch.

**Checkpoint loading, concretely** (`finetune.py::load_checkpoint`): open the pretraining `.pt`,
rebuild `CreditFoundationModel` from the `config` dict stored inside it (that's why shape lives
in the checkpoint), `load_state_dict(ckpt["model"])`. The backbone arrives knowing everything
from Part 12; the tiny `classification_head` (768→2, ~1.5k params) arrives random.

**The task comes from config, not code** (G2): `task.label: default_12m` looks up the label
definition in `dataset.yaml`. The prepayment model was created by changing that one line.

## 13.2 The three modes

```
                    frozen                  lora                    full
              ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
   backbone   │ ❄ frozen      │      │ ❄ frozen      │      │ 🔥 all trained │
              │               │      │ + tiny adapter│      │   (low LR!)    │
              │               │      │   🔥 matrices  │      │               │
              └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
   head       🔥 trained             🔥 trained             🔥 trained
   trainable  ~1.5k params           ~1–2% of params        100% of params
   OOT ROC    0.7309                 0.8068                 0.8257  (26M/4%)
```

**`frozen`** — freeze everything; embed every loan **once** (`embed_all` caches the `[USR]`
vectors); train only the head on the cached vectors. Blazing fast (the expensive encoder runs
once, not once per epoch), and it doubles as the *representation-quality probe*: 0.7309 with
1.5k trainable parameters means the pretrained embedding already carries most of the signal.

**`lora`** — Low-Rank Adaptation. Freeze the backbone, but wrap every encoder `nn.Linear` in a
`LoRALinear`: output = frozen_W(x) + **B·A**(x)·scale, where A is r×in and B is out×r with rank
r=4–8. *Why low-rank is enough:* adapting a pretrained model is a small, structured *correction*
— empirically, corrections live in a low-dimensional subspace, so instead of touching a 768×768
matrix (590k params) you learn 768×8 + 8×768 (12k params, ~2%). B starts at zero, so at step 0
the model is *exactly* the pretrained one — adaptation grows from there. Advantages: ~50× fewer
trainable params, less overfitting on small labeled sets, adapters are swappable per task over
one shared backbone. Result: 0.8068 — most of full's gain at a fraction of the cost.

**`full`** — everything trains, at a deliberately tiny LR (2e-5 vs pretraining's 3e-4; big steps
would bulldoze the pretrained weights — "catastrophic forgetting"). Strongest result (0.8257 /
0.8468 at 100M) because the *encoder itself* can reshape its representation toward
default-relevant distinctions. Costs: full GPU memory, slowest, most able to overfit — which the
monitoring split guards.

**Choosing:** frozen to sanity-check a backbone in minutes; LoRA when labels are few or you need
many tasks on one backbone; full for the headline when you have enough labels (this repo's OOT
protocol yields millions of observations, so full wins).

## 13.3 What the script actually does (execution flow)

```
1. resolve task from dataset.yaml (label spec: event_col, horizon, gate)
2. build observation samples per cutoff:  observe_panel (truncate+gate) → encode → tensors
   ├─ calendar-OOT: train_cutoffs 2016..2021, test_cutoffs 2022/2023
   │  └─ loans in both eras hash-assigned to ONE side (loan-disjoint guard)
   └─ or single-cutoff loan-holdout (representation test, not the honest one)
3. carve a 10% MONITORING split at the true base rate (val ROC every epoch, best-epoch restore)
4. rebalance the FIT set only: neg_per_pos downsampling + capped pos_weight in the loss
5. train per mode (§13.2); epoch loop prints avg loss + val ROC
6. report test ROC/PR vs the features bar; write markdown report
7. --save: persist {config, model.state_dict(), finetune meta} → servable by score_portfolio/serve
```

The saved **finetune meta** records mode, LoRA rank/alpha (so the loader can re-insert adapters
before `load_state_dict`), the task definition, metrics, and lineage — a checkpoint that knows
what it is.

## 13.4 Transfer learning, measured

The ladder *is* the evidence: same data, same protocol, only adaptation intensity changes —
0.73 (frozen) → 0.81 (LoRA) → 0.83 (full). And the pretraining corpus matters independently:
the same 26M model full-fine-tuned gains +0.015 ROC when its *pretraining* corpus grows 4%→10%
(E12), with identical labels. Knowledge acquired without labels, cashed in on the task.

## 13.5 Common mistakes

- **Full fine-tune at pretraining LR** (3e-4): loss spikes, val ROC craters — you erased the
  backbone. The mode-default LRs in the script exist for this reason.
- **Judging by fit loss.** The fit set is rebalanced; its loss is meaningless as a quality
  signal. Watch the monitoring val ROC (true base rate).
- **Reading fine-tuned probabilities as PDs.** Rebalancing made them ~50× too high; ranking is
  honest, levels need Part 15's calibration.
- **Comparing modes across different cutoff sets.** The bar (0.7913) and all FM numbers share
  one protocol; change the windows and nothing is comparable.

### Things to remember

1. Three intensities, one ladder: frozen 0.7309 → LoRA 0.8068 → full 0.8257 (26M/4%, OOT).
2. LoRA = frozen weights + low-rank B·A corrections (B zero-init ⇒ starts exactly at the pretrained model).
3. Full fine-tune at ~15× smaller LR than pretraining — big steps erase the backbone.
4. Judge by the monitoring split's val ROC (true base rate), never by the rebalanced fit loss.
5. The task is one YAML line (`task.label`); the saved checkpoint records how to rebuild itself.

---
*Next: [Part 14 — Metrics](14_metrics.md): how we know 0.8468 means something.*
