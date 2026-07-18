#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
#
# 65M scale-up probe, end-to-end and unattended. Trains a ~66.8M-param model on the SAME data +
# tokenizer as the 25.7M M5 model, then runs the calendar-OOT fine-tune and prints the comparison.
# Only the model SIZE differs, so any OOT lift is attributable to scale.
#
# Run in the background (it writes its own timestamped log and survives disconnect):
#     nohup bash scripts/run_65m_experiment.sh >/dev/null 2>&1 &
#     tail -f runs_65m_*.log
#
# Stages: 0) param-count check  1) shard pre-flight  2) pretrain  3) OOT fine-tune  4) compare.
# Any stage failing aborts the run (set -e) and the reason is in the log.

set -euo pipefail
cd /workspace/credit-foundation-model

LOG="runs_65m_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1                 # mirror all stdout/stderr into the log

ROOT="${CREDIT_FM_BUCKET:-gs://sriram-credit-fm-data}"   # override: export CREDIT_FM_BUCKET=gs://<yours>
RUNS="$ROOT/runs"
CKPT="$RUNS/m5_65m.pt"                       # 65M pretrained checkpoint (produced by stage 2)
FT="$RUNS/m5_65m_ft.pt"                      # 65M fine-tuned model (produced by stage 3)
ENCODED="$ROOT/output/encoded/fannie_mae/run_2000_2022"  # must match pretrain_65m.yaml run_name

echo "================================================================"
echo " 65M scale-up experiment — started $(date)"
echo " log: $LOG"
echo "================================================================"

# --- 0. param-count sanity check — confirm the config is ~65M BEFORE spending GPU hours ----------
echo "[0] param-count check ..."
python - <<'PY'
import yaml
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer
m = yaml.safe_load(open("configs/fannie_mae/pretrain_65m.yaml"))["model"]
tok = KVTTokenizer.load("configs/fannie_mae/tokenizer.json")
model = CreditFoundationModel(tok.vocab_size, len(tok.field_types), dim=m["dim"], n_heads=m["n_heads"],
    profile_layers=m["profile_layers"], event_layers=m["event_layers"], history_layers=m["history_layers"])
n = sum(p.numel() for p in model.parameters()) / 1e6
print(f"    {n:.1f}M params (dim={m['dim']} {m['profile_layers']}/{m['event_layers']}/{m['history_layers']} h{m['n_heads']})")
assert 55 < n < 80, f"param count {n:.1f}M is outside the ~65M band — fix dim/layers before running"
import torch
print(f"    cuda available: {torch.cuda.is_available()}  devices: {torch.cuda.device_count()}")
assert torch.cuda.is_available(), "no GPU visible — training would fall back to CPU (abort)"
PY

# --- 1. shard pre-flight — fail fast if the encoded corpus isn't where we expect ----------------
echo "[1] checking encoded shards exist under $ENCODED ..."
if ! gsutil ls "$ENCODED/train/" >/dev/null 2>&1; then
    echo "    ERROR: no encoded shards at $ENCODED/train/."
    echo "    Set the run_name in configs/fannie_mae/pretrain_65m.yaml (and \$ENCODED above) to the"
    echo "    corpus the 26M model trained on, OR run scripts/encode_dataset.py first."
    exit 1
fi
echo "    ok."

# --- 2. pretrain the 65M model (MLM; same shards, same recipe, bigger model) --------------------
echo "[2] pretrain 65M -> $CKPT   ($(date))"
python scripts/pretrain.py -c configs/fannie_mae/pretrain_65m.yaml

# --- 3. calendar-OOT fine-tune with the 65M checkpoint (full mode) ------------------------------
echo "[3] OOT fine-tune (full) with the 65M checkpoint   ($(date))"
python scripts/finetune.py -c configs/fannie_mae/finetune_oot.yaml --mode full \
    --checkpoint "$CKPT" --save "$FT" --report reports/m5_65m_oot_ft_full.md

# --- 4. done — the comparison the whole experiment exists to make -------------------------------
echo "================================================================"
echo " 65M experiment DONE — $(date)"
echo " 26M baseline (M5): OOT ROC 0.8230 / AP 0.0103"
echo " 65M result:        see reports/m5_65m_oot_ft_full.md (and the '=== Fine-tune (full) ===' block above)"
echo " If 65M > 26M on OOT ROC/AP, scale helps on this data -> greenlight the 100-200M project."
echo "================================================================"
