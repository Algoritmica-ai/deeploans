# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Downstream classification head over the per-loan [USR] embedding.
"""

from __future__ import annotations

import torch.nn as nn


class ClassificationHead(nn.Module):
    def __init__(self, hidden_size: int, num_classes: int = 2):
        super().__init__()
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, loan_embedding):
        return self.fc(loan_embedding)
