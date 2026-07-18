# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""★ Asset-blindness enforcement (v1.1 G1.4) — the framework/reference boundary as a TEST.

The ``credit_fm`` package must import cleanly with ZERO asset-specific modules: no Fannie, no
Dutch, nothing from ``reference_implementations/``. Asset code plugs in through the dataset
contract (``dataset.yaml`` + ``DatasetAdapter``), never the other way round. If this test fails,
someone has re-leaked an asset into the core — move it to ``reference_implementations/<asset>/``.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BANNED_MODULE_MARKERS = ("fannie", "dutch", "reference_implementations")


def test_no_asset_files_inside_the_package():
    """No file or directory under src/credit_fm may be named after an asset."""
    offenders = [str(p.relative_to(REPO)) for p in (REPO / "src" / "credit_fm").rglob("*")
                 if "__pycache__" not in p.parts
                 and any(m in p.name.lower() for m in ("fannie", "dutch"))]
    assert not offenders, f"asset-named files inside the core package: {offenders}"


def test_no_asset_imports_in_package_source():
    """No import statement in the core may reference an asset or reference_implementations.

    (get_adapter's *lazy, name-driven* importlib call is allowed — it never names an asset.)
    """
    offenders = []
    for py in (REPO / "src" / "credit_fm").rglob("*.py"):
        for i, line in enumerate(py.read_text().splitlines(), 1):
            stripped = line.split("#")[0]
            if ("import" in stripped
                    and any(m in stripped for m in ("fannie", "dutch"))):
                offenders.append(f"{py.relative_to(REPO)}:{i}: {line.strip()}")
    assert not offenders, "asset imports inside the core package:\n" + "\n".join(offenders)


def test_importing_every_credit_fm_module_loads_no_asset_code():
    """Import credit_fm and ALL its submodules in a clean interpreter; assert sys.modules stays
    free of asset code. Runs in a subprocess so this test's own imports can't contaminate it."""
    code = f"""
import importlib, pkgutil, sys
import credit_fm
for m in pkgutil.walk_packages(credit_fm.__path__, "credit_fm."):
    importlib.import_module(m.name)
bad = [n for n in sys.modules
       if any(marker in n.lower() for marker in {BANNED_MODULE_MARKERS!r})]
assert not bad, f"asset modules loaded by the core: {{bad}}"
print("asset-blind OK:", len([n for n in sys.modules if n.startswith('credit_fm')]), "modules")
"""
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          env={"PYTHONPATH": str(REPO / "src"), "PATH": "/usr/bin:/bin"})
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "asset-blind OK" in proc.stdout


def test_reference_implementation_registers_via_the_contract():
    """The OTHER direction must work: importing the ref-impl registers its adapter."""
    sys.path.insert(0, str(REPO))
    try:
        importlib.import_module("reference_implementations.fannie_mae")
        from credit_fm.data.adapter import REGISTRY
        assert "fannie_mae" in REGISTRY
    finally:
        sys.path.remove(str(REPO))
