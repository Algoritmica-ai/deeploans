# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Package a pretrained checkpoint into a self-contained, publishable model directory.

Reads a ``.pt`` checkpoint (local or ``gs://``) and writes a release folder containing:

* ``model.safetensors`` — the weights in the safetensors format (no pickle; loads anywhere),
* ``config.json``       — the architecture config needed to rebuild ``CreditFoundationModel``,
* ``tokenizer.json``    — a copy of the frozen KVT tokenizer (required to encode inputs),
* ``README.md``         — the model card (copied from ``--model-card``),
* ``load_example.py``   — a minimal snippet that reconstructs the model and loads the weights,
* ``LICENSE``           — Apache-2.0 (copied from the repo root if present).

The folder is then uploaded to Hugging Face Hub (``huggingface-cli upload``) or committed via
Git LFS — see the printed next-steps. Blueprint parity: the NVIDIA transaction-FM example ships
HF-loadable safetensors; ours needs the ``credit_fm`` package to instantiate the custom
three-branch architecture, so we ship weights + config + a load example rather than an
``AutoModel`` shim.

    python scripts/publish_model.py -c configs/fannie_mae/publish.yaml
    python scripts/publish_model.py -c configs/fannie_mae/publish.yaml --out models/fannie_mae_m5
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import fsspec
import torch
from safetensors.torch import save_file

from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize

_LOAD_EXAMPLE = '''# SPDX-License-Identifier: Apache-2.0
"""Load this published credit foundation model and extract a per-loan embedding."""
import json
from pathlib import Path

import torch
from safetensors.torch import load_file

from credit_fm.models import CreditFoundationModel  # pip install -e . from the credit_fm repo

here = Path(__file__).parent
cfg = json.loads((here / "config.json").read_text())
model = CreditFoundationModel(
    cfg["vocab_size"], cfg["n_field_types"], dim=cfg["dim"], n_heads=cfg["n_heads"],
    profile_layers=cfg["profile_layers"], event_layers=cfg["event_layers"],
    history_layers=cfg["history_layers"])
model.load_state_dict(load_file(here / "model.safetensors"))
model.eval()
print(f"loaded {model.num_parameters()/1e6:.1f}M-param credit FM; encode loans with tokenizer.json")
'''


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/publish.yaml")
    print(f"config: {cfg.config_path}\n{summarize(cfg, 'checkpoint', 'tokenizer', 'model_card', 'out')}",
          flush=True)

    out = Path(cfg.out)
    out.mkdir(parents=True, exist_ok=True)

    # 1) load checkpoint (local or gs://) and split weights / config
    storage.ensure_auth(cfg.checkpoint, cfg.key)
    with fsspec.open(cfg.checkpoint, "rb") as f:
        ckpt = torch.load(f, map_location="cpu", weights_only=False)
    state = {k: v.contiguous() for k, v in ckpt["model"].items()}
    mc = ckpt["config"]

    # 2) weights as safetensors (no pickle)
    save_file(state, str(out / "model.safetensors"),
              metadata={"format": "pt", "framework": "credit_fm"})
    n_params = sum(v.numel() for v in state.values())

    # 3) architecture config + provenance
    config = {**mc,
              "architecture": "CreditFoundationModel (encoder-MLM, three-branch KVT)",
              "n_parameters": int(n_params),
              "framework": "credit_fm",
              "license": "Apache-2.0",
              "source_checkpoint": cfg.checkpoint,
              "run_config": ckpt.get("run_config"),
              "pretrain_steps": ckpt.get("steps")}
    (out / "config.json").write_text(json.dumps(config, indent=2, default=str))

    # 4) tokenizer, load example, license, model card
    shutil.copyfile(cfg.tokenizer, out / "tokenizer.json")
    (out / "load_example.py").write_text(_LOAD_EXAMPLE)
    if Path("LICENSE").exists():
        shutil.copyfile("LICENSE", out / "LICENSE")
    card = Path(cfg.model_card)
    if card.exists():
        shutil.copyfile(card, out / "README.md")
    else:
        print(f"  warning: model card {card} not found — README.md not written", flush=True)

    print(f"\npackaged {n_params/1e6:.1f}M params -> {out}/", flush=True)
    for p in sorted(out.iterdir()):
        print(f"  {p.name}  ({p.stat().st_size/1e6:.1f} MB)", flush=True)
    print("\nnext — publish one of:", flush=True)
    print(f"  HF Hub:  huggingface-cli upload <org>/credit-fm-fannie-m5 {out} .", flush=True)
    print(f"  Git LFS: git lfs track '{out}/*.safetensors' && git add {out} && git commit", flush=True)


if __name__ == "__main__":
    main()
