#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
#
# SCALING experiment, end-to-end and unattended: 10% data + 100M model.
# The genuine "does scaling pay?" test (the 65M-on-4% probe was flat/data-bound). Re-ingests at 10%
# (~3B tokens), pretrains a ~100M model (FlashAttention makes the big batch fit), then runs the
# calendar-OOT fine-tune and compares to the 26M headline (0.8257) and the XGB bar (0.784).
#
# Run in the background (writes its own timestamped log, survives disconnect):
#     nohup bash scripts/run_scale_100m.sh >/dev/null 2>&1 &
#     tail -f runs_scale100m_*.log
#
# Stages: 0 preflight · 1 ingest 10% · 2 split(<=2022) · 3 encode · 4 pretrain 100M · 5 OOT FT · 6 compare.
# Resume-safe: stages whose GCS outputs already exist are skipped. set -e aborts on any failure.

set -euo pipefail
cd /workspace/credit-foundation-model

LOG="runs_scale100m_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True   # (pretrain.py also sets this; belt-and-suspenders)

ROOT="${CREDIT_FM_BUCKET:-gs://sriram-credit-fm-data}"   # override: export CREDIT_FM_BUCKET=gs://<yours>
RAW="$ROOT/output/raw/fannie_mae"
PANEL10="$RAW/panel_2000_2024_10pct.parquet"
RUN_NAME="run_2000_2022_10pct"
PROC="$ROOT/output/processed/fannie_mae/$RUN_NAME"
ENC="$ROOT/output/encoded/fannie_mae/$RUN_NAME"
CKPT="$ROOT/runs/m_100m.pt"
FT="$ROOT/runs/m_100m_ft.pt"

echo "================================================================"
echo " SCALING experiment (10% data + 100M model) — started $(date)   log: $LOG"
echo "================================================================"

# --- 0. preflight ---------------------------------------------------------------------------------
echo "[0] preflight ..."
python -c "import torch; assert torch.cuda.is_available(), 'no GPU'; print('    cuda ok:', torch.cuda.device_count(), 'devices')"
python -c "from credit_fm.models.base import MultiHeadSelfAttention as A; import inspect; assert 'scaled_dot_product_attention' in inspect.getsource(A.forward), 'FlashAttention/SDPA NOT in base.py — need feat/flashattention-sdpa merged'; print('    FlashAttention: present')"

# --- 1. ingest the 10% panel (skip if it exists) --------------------------------------------------
if gsutil ls "$PANEL10" >/dev/null 2>&1; then
    echo "[1] 10% panel exists at $PANEL10 — skipping ingest"
else
    echo "[1] ingest 10% -> $PANEL10   ($(date))"
    python scripts/ingest_fannie_mae.py -c configs/fannie_mae/ingest_2000_2024.yaml \
        --sample_pct 10 --combined_name panel_2000_2024_10pct.parquet
    python scripts/validate_ingest.py --panel "$PANEL10" --sample-pct 10
fi

# --- 2. split the pretrain corpus (reporting_max 2022-12-31; skip if it exists) --------------------
if gsutil ls "$PROC/train.parquet" >/dev/null 2>&1; then
    echo "[2] split exists at $PROC — skipping"
else
    echo "[2] split <=2022 corpus -> $PROC   ($(date))"
    python scripts/prepare_data.py -c configs/fannie_mae/prepare.yaml \
        --input "$PANEL10" --run_name "$RUN_NAME" --reporting_max 2022-12-31
    python scripts/validate_splits.py --dir "$PROC"
fi

# --- 3. encode shards (more workers — this is the ~3B-token bottleneck; skip if done) --------------
if gsutil ls "$ENC/train/manifest.json" >/dev/null 2>&1 && gsutil ls "$ENC/val/manifest.json" >/dev/null 2>&1; then
    echo "[3] encoded shards exist at $ENC — skipping"
else
    echo "[3] encode train + val shards   ($(date))"
    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml --run_name "$RUN_NAME" --split train --workers 64
    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml --run_name "$RUN_NAME" --split val   --workers 64
fi

# --- 4. pretrain the 100M model (FlashAttention enables batch 256) ---------------------------------
echo "[4] pretrain 100M -> $CKPT   ($(date))"
python scripts/pretrain.py -c configs/fannie_mae/pretrain_100m.yaml --run_name "$RUN_NAME"

# --- 5. calendar-OOT fine-tune with the 100M checkpoint (full mode; observe the 10% panel) --------
echo "[5] OOT fine-tune (full) with the 100M checkpoint   ($(date))"
python scripts/finetune.py -c configs/fannie_mae/finetune_oot.yaml --mode full \
    --checkpoint "$CKPT" --panel "$PANEL10" --save "$FT" \
    --report reports/m_100m_oot_ft_full.md

# --- 6. compare -----------------------------------------------------------------------------------
echo "================================================================"
echo " SCALING experiment DONE — $(date)"
echo " 26M on 4% data (M5):  OOT ROC 0.8257 / AP 0.0113"
echo " 65M on 4% data:       OOT ROC 0.8223 (flat — data-bound)"
echo " 100M on 10% data:     see reports/m_100m_oot_ft_full.md + the '=== Fine-tune (full) ===' block above"
echo " If 100M-on-10% > 0.8257, scaling DATA+MODEL together pays -> the scaling-law result for the paper."
echo "================================================================"
