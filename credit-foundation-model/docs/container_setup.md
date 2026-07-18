# Container Setup (H100 / NGC PyTorch)

Bring-up guide for running this framework inside the NVIDIA NGC PyTorch container on
H100 GPUs. Designed so the environment **survives container restarts** and is reproducible
across participants.

## Container

The container is defined in your `docker-compose.yml` (one service per participant):

```yaml
participant1:
  image: nvcr.io/nvidia/pytorch:24.05-py3
  working_dir: /workspace
  shm_size: '32gb'
  ulimits: { memlock: -1, stack: 67108864 }
  volumes:
    - ./participant1_data:/workspace          # volume → everything here persists
  environment:
    - WANDB_PROJECT=credit-foundation-model
    - NVIDIA_VISIBLE_DEVICES=0,1              # this container sees GPUs 0 and 1
  # ... tailscale command, gpu reservations ...
```

> **GPU note:** `NVIDIA_VISIBLE_DEVICES=0,1` exposes **2 GPUs** to this container. Phases A–C
> (tokenizer, data, model) only need 1–2 GPUs. The full pretraining run (Phase D) assumes
> 8× H100 — confirm whether you get all 8 for that run, or plan it around 2 GPUs with
> gradient accumulation. (Project Open Question #2.)

## What persists vs what doesn't

`./participant1_data:/workspace` is a Docker volume, so **anything under `/workspace`
persists**, including `/workspace/.venv`. The container's home dir (`/root`) is **not** on the
volume:

| Action | `/workspace` (incl. `.venv`) | `/root` (`~/.bashrc`, `~/.netrc`) |
|--------|------------------------------|-----------------------------------|
| `docker restart` (`unless-stopped`) | survives | survives |
| `compose down && up` / image change (recreate) | survives | wiped |

Implications:
- Put the **venv, code, data, and secrets under `/workspace`**.
- Auto-activation and secrets driven from `~/.bashrc` survive a *restart* but not a *recreate* —
  use the grep-guarded approach below (and optionally add it to the compose `command`).
- The venv symlinks the image's Python, so it works across restarts **as long as the image
  tag stays `24.05-py3`**. If you bump the image, recreate the venv.

## Step-by-step

### 1. Clone the code (private repo → authenticate first)
```bash
cd /workspace
git clone https://github.com/sriramarun/credit-foundation-model.git
cd credit-foundation-model
```

### 2. Verify GPUs (before building anything)
```bash
python -c "import torch; print('torch', torch.__version__, 'cuda', torch.cuda.is_available())"
python -c "import torch; print('GPUs:', torch.cuda.device_count())"     # expect 2
nvidia-smi --query-gpu=name,memory.total --format=csv
python -c "import torch.distributed as d; print('NCCL:', d.is_nccl_available())"
```

### 3. Create a restart-proof venv

**Critical:** use `--system-site-packages` so the venv inherits the image's CUDA-matched
torch. A plain venv would be empty and force a generic PyPI torch reinstall that can break GPU.

```bash
cd /workspace
python -m venv .venv --system-site-packages
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e "/workspace/credit-foundation-model[dev]" --no-build-isolation
# confirm the image's torch is still the active one:
python -c "import torch; print(torch.__version__, torch.cuda.is_available(), torch.cuda.device_count())"
```

Auto-activate on shell login (grep-guarded to avoid duplicate lines on re-run):
```bash
grep -q '/workspace/.venv/bin/activate' ~/.bashrc || \
  echo 'source /workspace/.venv/bin/activate' >> ~/.bashrc
```
For recreate-proofing, also add the same guarded line to the compose `command` before
`tail -f /dev/null`.

### 4. Secrets + storage env (keep out of git; survive via the volume)

```bash
cat > /workspace/secrets.env <<'EOF'
export WANDB_API_KEY=...
export WANDB_PROJECT=credit-foundation-model
export HF_TOKEN=...
export HF_HOME=/workspace/.hf_cache      # persist the HF download cache on the volume
EOF
chmod 600 /workspace/secrets.env
grep -q '/workspace/secrets.env' ~/.bashrc || \
  echo 'source /workspace/secrets.env' >> ~/.bashrc
source /workspace/secrets.env
wandb login --verify
```

Storage environment (v1.1 G5.1; `setup_container.sh` step 5b exports these to `~/.bashrc`):
`CREDIT_FM_GCS_KEY` points the package at the GCS service-account JSON (defaults to
`/workspace/.gcloud/credit-fm-sa.json` if unset), and `CREDIT_FM_BUCKET` re-points the
`run_*.sh` experiment scripts at a different bucket — set both when moving off the reference
box.

### 5. Git: identity + credentials (push-ready from the container)

The container is the primary dev box — you edit and push from here. Persist git identity and
credentials under `/workspace` so they survive restarts/recreates:
```bash
git config --global user.name  "Your Name"
git config --global user.email "you@example.com"
# store the PAT under the volume (NOT the ephemeral home dir):
git config --global credential.helper 'store --file=/workspace/.git-credentials'
git config --global --add safe.directory /workspace/credit-foundation-model
# first push prompts once for username + PAT, then it's remembered
```
`/workspace/.git-credentials` holds a token — treat it as a secret; it lives outside the repo
so it won't be committed.

### 6. Confirm the scaffold wiring
```bash
cd /workspace/credit-foundation-model
python -c "import credit_fm; print('credit_fm', credit_fm.__version__)"
ruff check .
pytest -q        # stubs are skip-marked today; confirms collection + CI parity
```

### 7. Day 2 — pull the dataset + inspect the schema

# single-file dataset → download straight into data/raw (repo-id + filename, not a URL):
cd /workspace/credit-foundation-model
python - <<'PY'
import os
from huggingface_hub import hf_hub_download
p = hf_hub_download("Algoritmica/green-lion-2024-2025",
                    "Overall_2024_2025_all_months.parquet",
                    repo_type="dataset", local_dir="data/raw",
                    token=os.environ.get("HF_TOKEN"))
print("downloaded to:", p)
PY
python - <<'PY'
import pandas as pd
df = pd.read_parquet('data/raw/Overall_2024_2025_all_months.parquet')
print('rows:', len(df), 'cols:', len(df.columns))
print(df.dtypes)
PY

## Quick start (after first setup)

A helper script automates steps 3–6 (venv, install, auto-activation, secrets, git):
```bash
GIT_USER_NAME="Your Name" GIT_USER_EMAIL="you@example.com" \
  bash /workspace/credit-foundation-model/scripts/setup_container.sh
```
It is idempotent — safe to re-run after a restart or recreate. (Identity env vars are optional
if you already ran `git config --global user.name/.email`.)

## Dev workflow

The container is the primary dev environment: edit and run here (e.g. VS Code / Cursor
Remote-SSH over Tailscale), since all GPU work runs on it. GitHub is the single source of
truth; git has no file locking — work on **branches** and sync through GitHub. Always
`git pull` before editing.

```bash
cd /workspace/credit-foundation-model
git checkout main && git pull              # latest
git checkout -b feat/<short-topic>         # branch per task
# ... edit + run on GPUs ...
ruff check . && pytest -q                  # local gate (mirrors CI)
git add -A && git commit -m "scope: what changed"
git push -u origin feat/<short-topic>      # open a PR → CI must pass → merge
```

Never commit data, checkpoints, secrets, or `.venv` (all gitignored; weights via Git LFS).
See [`../CONTRIBUTING.md`](../CONTRIBUTING.md) for the full workflow and rules.
Secrets (`WANDB_API_KEY`, `HF_TOKEN`, `HF_HOME`) live in `/workspace/secrets.env` — never
committed. Datasets download into the gitignored `data/raw/`; nothing under `data/` (except
`.gitkeep` and this README) is tracked.
