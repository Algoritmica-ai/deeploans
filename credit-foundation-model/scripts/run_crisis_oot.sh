#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
#
# CRISIS-OOT, end-to-end and unattended — the hardest regime-shift test.
# Builds a CRISIS-BLIND backbone (pretrained on <=2007 ONLY, so it never saw the 2008 crisis) and
# runs the calendar-OOT fine-tune (train Dec-2000..2006, test Dec-2008..2010) vs the XGBoost bar 0.757.
#
# Run in the background (writes its own timestamped log, survives disconnect):
#     nohup bash scripts/run_crisis_oot.sh >/dev/null 2>&1 &
#     tail -f runs_crisis_*.log
#
# Stages: 0 preflight · 1 split(<=2007) · 2 validate · 3 encode · 4 pretrain crisis-blind ·
#         5 fine-tune vs 0.757 · 6 compare. Any failure aborts (set -e); the reason is in the log.

set -euo pipefail
cd /workspace/credit-foundation-model

LOG="runs_crisis_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

ROOT="${CREDIT_FM_BUCKET:-gs://sriram-credit-fm-data}"   # override: export CREDIT_FM_BUCKET=gs://<yours>
PANEL="$ROOT/output/raw/mortgage_performance/panel_2000_2024.parquet"
PROC="$ROOT/output/processed/mortgage_performance/run_2000_2007"
ENC="$ROOT/output/encoded/mortgage_performance/run_2000_2007"
CKPT="$ROOT/runs/m_crisis_blind.pt"
RUN_NAME="run_2000_2007"
PRETRAIN_BATCH=48          # <=2007 loans are long (~587 tok/loan) -> smaller batch than the 128 default

# the H100 needs expandable segments for the long-sequence O(L^2) attention (per pretrain.yaml note)
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

echo "================================================================"
echo " CRISIS-OOT experiment — started $(date)   log: $LOG"
echo " crisis-blind pretrain (<=2007) -> fine-tune train 2000-06 / test 2008-10  vs XGB 0.757"
echo "================================================================"

# --- 0. preflight: GPU + panel present -----------------------------------------------------------
echo "[0] preflight ..."
python -c "import torch; assert torch.cuda.is_available(), 'no GPU visible'; print('    cuda ok:', torch.cuda.device_count(), 'devices')"
gsutil ls "$PANEL" >/dev/null 2>&1 || { echo "    ERROR: panel not found: $PANEL"; exit 1; }
echo "    panel ok."

# --- 1. split the crisis-BLIND corpus: cap all reporting at 2007-12-31 (skip if already done) ----
if gsutil ls "$PROC/train.parquet" >/dev/null 2>&1; then
    echo "[1] split exists at $PROC — skipping"
else
    echo "[1] split <=2007 corpus -> $PROC   ($(date))"
    python scripts/prepare_data.py -c configs/mortgage_performance/prepare.yaml \
        --input "$PANEL" --run_name "$RUN_NAME" --reporting_max 2007-12-31
    echo "[2] validate split"
    python scripts/validate_splits.py --dir "$PROC"
fi

# --- 3. encode-once shards for train + val (shared frozen tokenizer.json; skip if already done) --
if gsutil ls "$ENC/train/manifest.json" >/dev/null 2>&1 && gsutil ls "$ENC/val/manifest.json" >/dev/null 2>&1; then
    echo "[3] encoded shards exist at $ENC — skipping"
else
    echo "[3] encode train + val shards   ($(date))"
    python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --run_name "$RUN_NAME" --split train --workers 32
    python scripts/encode_dataset.py -c configs/mortgage_performance/encode.yaml --run_name "$RUN_NAME" --split val   --workers 32
fi

# --- 4. pretrain the crisis-blind backbone (26M, same recipe; smaller batch for long loans) ------
echo "[4] pretrain crisis-blind backbone -> $CKPT  (batch $PRETRAIN_BATCH)   ($(date))"
python scripts/pretrain.py -c configs/mortgage_performance/pretrain.yaml \
    --run_name "$RUN_NAME" --checkpoint.out "$CKPT" --data.batch_size "$PRETRAIN_BATCH"

# --- 5. calendar-OOT fine-tune on the crisis window (full mode) ----------------------------------
echo "[5] fine-tune crisis-OOT (full)   ($(date))"
python scripts/finetune.py -c configs/mortgage_performance/finetune_crisis.yaml --mode full \
    --report reports/mortgage_oot_crisis_ft_full.md

# --- 6. compare ----------------------------------------------------------------------------------
echo "================================================================"
echo " CRISIS-OOT DONE — $(date)"
echo " XGBoost crisis bar:   ROC 0.757 / PR 0.024  (reports/mortgage_oot_crisis.md)"
echo " FM crisis-blind:      see reports/mortgage_oot_crisis_ft_full.md + the '=== Fine-tune ===' block above"
echo " If FM > 0.757, the sequence FM generalizes to the UNSEEN 2008 crisis — the strongest claim."
echo " (Caveat: cal= tokens for 2008-2011 are near-untrained in a <=2007 backbone; see finetune_crisis.yaml.)"
echo "================================================================"
