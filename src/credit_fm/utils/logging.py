# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Logging setup.
"""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s')
    return logging.getLogger(name)
