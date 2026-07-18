# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Packaging contract tests (v1.1 G5.1).

Locks the pip-installable surface: the version is single-sourced (``credit_fm.__version__`` ==
``pyproject.toml``), the advertised top-level API imports, and the CORE dependency list stays
honest — no heavy never-imported deps (transformers/peft/wandb/lightgbm/matplotlib were removed;
this test fails if one creeps back into core instead of an extra).
"""

from __future__ import annotations

import re
from pathlib import Path

import credit_fm

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = (ROOT / "pyproject.toml").read_text()

# tomllib is 3.11+ and this package supports 3.10, so parse the two fields we need directly.
_VERSION = re.search(r'^version\s*=\s*"([^"]+)"', PYPROJECT, re.M)
_CORE = re.search(r"^dependencies\s*=\s*\[(.*?)\]", PYPROJECT, re.M | re.S)


def test_version_is_single_sourced():
    assert _VERSION, "pyproject.toml has no version"
    assert credit_fm.__version__ == _VERSION.group(1)


def test_top_level_api_imports_and_matches_all():
    from credit_fm import (CreditDataModule, CreditFoundationModel,  # noqa: F401
                           KVTTokenizer, train_mlm)
    for name in credit_fm.__all__:
        assert hasattr(credit_fm, name), name


def test_core_dependencies_are_the_lean_set():
    assert _CORE, "pyproject.toml has no dependencies list"
    deps = {re.split(r"[<>=\[]", d.strip().strip('",'))[0]
            for d in _CORE.group(1).split("\n") if d.strip().strip('",')}
    assert deps == {"torch", "numpy", "pandas", "pyarrow", "pyyaml", "fsspec", "scikit-learn"}, (
        f"core deps drifted: {sorted(deps)} — heavy/optional deps belong in "
        "[project.optional-dependencies], not core")


def test_optional_backends_are_not_core():
    for banned in ("transformers", "peft", "wandb", "lightgbm", "matplotlib", "xgboost",
                   "gcsfs", "tensorboard", "tqdm"):
        assert not re.search(rf'^\s*"{banned}', _CORE.group(1), re.M), (
            f"{banned} must not be a core dependency (use an extra)")
