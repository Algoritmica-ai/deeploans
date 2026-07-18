# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Shared pytest fixtures (toy panel, tiny tokenizer/model configs).
"""

import pytest


@pytest.fixture
def toy_panel():
    """100 synthetic loans for fast end-to-end tests."""
    pytest.skip('toy_panel fixture not yet implemented')
