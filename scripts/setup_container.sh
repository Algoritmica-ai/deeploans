#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Idempotent environment bring-up for the NGC PyTorch H100 container.
# Creates a restart-proof venv under /workspace, installs the credit_fm package,
# wires auto-activation + secrets, configures git for push-ready dev, and verifies.
#
# Usage:  bash scripts/setup_container.sh
#   Optional git identity (set once per participant):
#     GIT_USER_NAME="Your Name" GIT_USER_EMAIL="you@example.com" bash scripts/setup_container.sh
# Safe to re-run after a container restart or recreate.
#
# See docs/container_setup.md for the full explanation.
set -euo pipefail

WORKSPACE="${WORKSPACE:-/workspace}"
VENV="$WORKSPACE/.venv"
REPO="${REPO:-$WORKSPACE/credit-foundation-model}"
SECRETS="$WORKSPACE/secrets.env"

echo "==> 1. GPU / torch check (image build)"
python -c "import torch; print('torch', torch.__version__, 'cuda', torch.cuda.is_available(), 'gpus', torch.cuda.device_count())"

echo "==> 2. Create venv (inherits image's CUDA torch via --system-site-packages)"
ensure_venv_pkg() {
  # NGC/Ubuntu images often ship Python without ensurepip; install the matching venv pkg.
  local pyver; pyver="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  echo "    installing python${pyver}-venv (ensurepip missing)..."
  apt-get update -qq && apt-get install -y -qq "python${pyver}-venv" python3-venv
}
if [ ! -f "$VENV/bin/activate" ]; then
  [ -d "$VENV" ] && { echo "    removing incomplete venv at $VENV"; rm -rf "$VENV"; }
  if ! python -m venv "$VENV" --system-site-packages 2>/tmp/venv.err; then
    if grep -q ensurepip /tmp/venv.err; then
      ensure_venv_pkg
      rm -rf "$VENV"
      python -m venv "$VENV" --system-site-packages
    else
      cat /tmp/venv.err >&2
      echo "ERROR: 'python -m venv' failed." >&2; exit 1
    fi
  fi
  echo "    created $VENV"
else
  echo "    $VENV already exists — reusing"
fi
if [ ! -f "$VENV/bin/activate" ]; then
  echo "ERROR: $VENV/bin/activate missing after venv creation." >&2; exit 1
fi

echo "==> 3. Install credit_fm (editable, dev extras; do not reinstall torch)"
pip install -e "${REPO}[dev]" --no-build-isolation
python -c "import torch; print('    torch still', torch.__version__, '| cuda', torch.cuda.is_available())"

echo "==> 4. Wire auto-activation (grep-guarded, survives restarts)"
grep -q "$VENV/bin/activate" ~/.bashrc 2>/dev/null \
  || echo "source $VENV/bin/activate" >> ~/.bashrc

echo "==> 5. Secrets (sourced if present; never commit this file)"
if [ -f "$SECRETS" ]; then
  grep -q "$SECRETS" ~/.bashrc 2>/dev/null || echo "source $SECRETS" >> ~/.bashrc
  # shellcheck disable=SC1090
  source "$SECRETS"
  echo "    sourced $SECRETS"
else
  echo "    no $SECRETS found — create it with WANDB_API_KEY / HF_TOKEN (see docs)"
fi

echo "==> 5b. Storage env (v1.1 G5.1 — the package reads these instead of hardcoded paths)"
GCS_KEY="${CREDIT_FM_GCS_KEY:-$WORKSPACE/.gcloud/credit-fm-sa.json}"
grep -q "CREDIT_FM_GCS_KEY" ~/.bashrc 2>/dev/null \
  || echo "export CREDIT_FM_GCS_KEY=$GCS_KEY" >> ~/.bashrc
export CREDIT_FM_GCS_KEY="$GCS_KEY"
[ -f "$GCS_KEY" ] && echo "    CREDIT_FM_GCS_KEY=$GCS_KEY" \
  || echo "    CREDIT_FM_GCS_KEY=$GCS_KEY (key file not present yet — place it there for gs:// access)"
# Optional: export CREDIT_FM_BUCKET=gs://<your-bucket> to repoint the run_*.sh experiment scripts.

echo "==> 6. Git: identity + credential persistence (push-ready from the container)"
# Persist the PAT under the volume so pushes survive restarts/recreates.
git config --global credential.helper "store --file=$WORKSPACE/.git-credentials"
git config --global --add safe.directory "$REPO" 2>/dev/null || true
[ -n "${GIT_USER_NAME:-}" ]  && git config --global user.name  "$GIT_USER_NAME"
[ -n "${GIT_USER_EMAIL:-}" ] && git config --global user.email "$GIT_USER_EMAIL"
if git config --global --get user.name >/dev/null && git config --global --get user.email >/dev/null; then
  echo "    identity: $(git config --global user.name) <$(git config --global user.email)>"
else
  echo "    set identity: re-run with GIT_USER_NAME / GIT_USER_EMAIL, or 'git config --global user.name/.email'"
fi
echo "    credentials stored at $WORKSPACE/.git-credentials on first push (keep secret, never commit)"

echo "==> 7. Verify scaffold"
python -c "import credit_fm; print('    credit_fm', credit_fm.__version__)"
( cd "$REPO" && ruff check . && pytest -q ) || echo "    (ruff/pytest reported issues — review above)"

echo "==> Done. Open a new shell or 'source $VENV/bin/activate' to use the env."
echo "    Dev loop: git pull → git checkout -b feat/... → edit+test → commit → push → PR (see CONTRIBUTING.md)."
