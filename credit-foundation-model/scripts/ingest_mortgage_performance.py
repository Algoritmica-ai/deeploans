# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""DEPRECATED shim — the Mortgage ingest moved behind the dataset-adapter interface (v1.1 G1.4).

The derivation logic now lives in ``reference_implementations/mortgage_performance/adapter.py`` and the
asset-blind driver is ``scripts/ingest.py``. This shim forwards so existing commands and run
scripts keep working for one release::

    python scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml [overrides...]
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

print("DEPRECATED: scripts/ingest_mortgage_performance.py is now a shim — use\n"
      "  python scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml [overrides...]\n"
      "forwarding ...", flush=True)
sys.argv[0] = "scripts/ingest.py"
runpy.run_path(str(Path(__file__).resolve().parent / "ingest.py"), run_name="__main__")
