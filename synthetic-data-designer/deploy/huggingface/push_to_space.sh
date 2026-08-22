#!/usr/bin/env bash
#
# Publish this project as a Hugging Face Space.
#
#   ./deploy/huggingface/push_to_space.sh <owner>/<space-name>
#
# A Space is its own git repo. This assembles one from the project — the source,
# the packs, the packaging metadata — plus the two files a Space needs at its
# root: a Dockerfile, and a README whose front-matter tells Spaces to build it.
#
# The Space's README is *not* this project's README. It is the card visitors
# land on, and it leads with the fact that a hosted instance is shared, because
# the app itself promises the opposite when you run it locally.
#
# Requires: git, and `hf auth login` with write access to the target.

set -euo pipefail

SPACE="${1:-}"
if [[ -z "$SPACE" || "$SPACE" != */* ]]; then
    echo "usage: $0 <owner>/<space-name>" >&2
    echo "  e.g. $0 sriramarun/synthetic-data-designer" >&2
    exit 2
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT

echo "==> Checking you are logged in"
hf auth whoami >/dev/null || {
    echo "Not logged in. Run: hf auth login" >&2
    exit 1
}

echo "==> Creating the Space if it does not exist"
if ! hf repo create "$SPACE" --repo-type space --space-sdk docker --exist-ok; then
    cat >&2 <<'NOTE'

A Docker Space is not on the free tier. Personal accounts need PRO
(https://huggingface.co/pro); organisations need Team or Enterprise
(https://huggingface.co/enterprise). Free covers Static Spaces only, which
cannot run a Python server.

The Dockerfile beside this script is a plain one and runs anywhere — see
docs/DEPLOYMENT.md for hosts that do have a free tier.
NOTE
    exit 1
fi

# `hf auth login` stores a token for the Python client. It does not teach git
# about it, so a plain clone URL prompts for a username git cannot ask for and
# the push dies with "could not read Username". Carry the token in the remote
# instead. The staging directory is a mktemp that the EXIT trap removes, so the
# token never outlives this run — nothing is written to the keychain, and the
# project repo's own remotes are untouched.
TOKEN="$(hf auth token 2>/dev/null || true)"
if [[ -z "$TOKEN" ]]; then
    echo "No token from 'hf auth token'. Run: hf auth login" >&2
    exit 1
fi

echo "==> Cloning https://huggingface.co/spaces/$SPACE"
git clone "https://user:${TOKEN}@huggingface.co/spaces/$SPACE" "$STAGING/space"
cd "$STAGING/space"

echo "==> Assembling"
# Only what the image builds from. No tests, no docs, no generated data, and no
# .git — the Space has its own history.
rm -rf src packs pyproject.toml LICENSE Dockerfile README.md
cp -R "$ROOT/src" "$ROOT/packs" .
cp "$ROOT/pyproject.toml" "$ROOT/LICENSE" .
cp "$HERE/Dockerfile" .
cp "$HERE/README.md" .
find . -path ./.git -prune -o -name '__pycache__' -type d -print0 | xargs -0 rm -rf

echo "==> Pushing"
git add -A
if git diff --cached --quiet; then
    echo "Nothing changed."
    exit 0
fi
git commit -m "Deploy Synthetic Data Designer"
git push

echo
echo "Done: https://huggingface.co/spaces/$SPACE"
echo "The first build takes a few minutes. Watch the Logs tab for progress."
