# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""credit_fm — an open-source framework for training credit foundation models.

Encoder-only (PRAGMA-style) architecture with masked-language-modelling
pretraining over tabular credit panel data, configured per asset class via YAML.

The four symbols below are the stable top-level API — tokenize, batch, model,
train::

    from credit_fm import KVTTokenizer, CreditDataModule, CreditFoundationModel, train_mlm

Everything else is importable from its subpackage (``credit_fm.data``,
``credit_fm.models``, ``credit_fm.tokenizer``, ``credit_fm.training``,
``credit_fm.inference``, ``credit_fm.utils``).
"""

from credit_fm.data import CreditDataModule
from credit_fm.models import CreditFoundationModel
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.training import train_mlm

__version__ = "1.1.0.dev0"

__all__ = [
    "CreditDataModule",
    "CreditFoundationModel",
    "KVTTokenizer",
    "train_mlm",
    "__version__",
]
