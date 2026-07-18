# Contributing

Thanks for your interest in the Credit Foundation Model Framework.

## Development setup
```bash
pip install -e ".[dev]"
pytest -q
ruff check .
```

On the H100 / NGC container, use the one-command bring-up instead (creates the venv,
installs the package, and configures git for push-ready development):
```bash
GIT_USER_NAME="Your Name" GIT_USER_EMAIL="you@example.com" \
  bash scripts/setup_container.sh
```
See [`docs/container_setup.md`](docs/container_setup.md) for details.

## Dev workflow

GitHub is the single source of truth. There is no file locking (git is not check-out/
check-in) — everyone works on **branches** and syncs through GitHub. The container is the
primary dev box: edit and run there (e.g. VS Code / Cursor Remote-SSH over Tailscale), since
all GPU work has to run on it. Keep any other clone (e.g. a laptop) secondary, and always
`git pull` before editing in either place.

```
  edit + run + test (container)  ──push/pull──▶  GitHub (hub)  ◀──pull──  secondary clone
```

### Daily loop
```bash
cd /workspace/credit-foundation-model
git checkout main && git pull              # start from latest
git checkout -b feat/<short-topic>         # one branch per task
# ... edit + run on GPUs ...
ruff check . && pytest -q                  # local gate (mirrors CI)
git add -A && git commit -m "scope: what changed"
git push -u origin feat/<short-topic>
# open a PR against main on GitHub → CI must pass → merge
```

### Rules
- **Branch per feature; PR into `main`.** Never commit directly to `main` (multi-participant repo).
- **`git pull` before editing** in any clone — this is how clones stay in sync.
- **Never commit** data, checkpoints, secrets, or `.venv`. `.gitignore` excludes `data/`,
  weights, and caches; large model files go through Git LFS (`.gitattributes` routes
  `*.bin`/`*.safetensors`). Secrets live in `/workspace/secrets.env`, outside the repo.
- Keep PRs focused and small; let CI run before merging.

## Standards
- Python 3.10+. `ruff` for lint/format. Type hints on all public APIs. Google-style docstrings.
- Every `src/credit_fm/` module has a matching test in `tests/`.
- Every script accepts `--seed` and logs the seed + git commit hash.
- Conventional, descriptive commits. Open a PR against `main`; CI must pass.

## Licensing
By contributing you agree your contributions are licensed under Apache 2.0.
