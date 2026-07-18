# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Reference implementations — asset-specific adapters and ingest logic (v1.1 G1.4).

Each subpackage owns everything specific to one asset (column derivations, glossaries,
the :class:`~credit_fm.data.adapter.DatasetAdapter` implementation). The ``credit_fm``
package itself imports nothing from here — enforced by ``tests/test_asset_blind.py``;
``credit_fm.data.adapter.get_adapter`` imports a subpackage lazily by configured name.
"""
