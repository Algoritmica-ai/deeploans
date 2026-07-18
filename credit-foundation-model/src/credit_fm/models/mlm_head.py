# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""MLM pretraining head — the 3-vector concat (DL-002 / docs/architecture.md).

For every token position it concatenates three contexts and projects to the vocabulary:

* **local**   — the token's own encoder output (Event encoder for event tokens, Profile encoder
  for profile tokens): the within-month / within-profile field interactions.
* **segment** — the History-contextualised vector of the token's segment (its event, or the
  profile vector): the cross-event / regime context.
* **loan**    — the History ``[LOAN]`` embedding broadcast to every token: the global context.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class MLMHead(nn.Module):
    def __init__(self, hidden_size: int, vocab_size: int):
        super().__init__()
        self.proj = nn.Linear(hidden_size * 3, vocab_size)

    def forward(self, local_ctx, event_ctx, user_ctx):
        return self.proj(torch.cat([local_ctx, event_ctx, user_ctx], dim=-1))
