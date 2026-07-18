# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Encode a processed panel split into token-id shards (M2 data layer, encode-once).

Loads a frozen ``tokenizer.json``, reads a per-loan monthly panel (local or ``gs://``), and writes
sharded parquet where each row is one loan with aligned ragged columns
(``input_ids``/``event_index``/``field_type``/``branch``) + ``n_tokens``/``n_events``. A
``manifest.json`` records the resolved config, loan/token counts, and shard list.

**Streaming input (v1.1 G3.2):** when ``input`` is a bucketed split directory (written by
``prepare_data --stream true`` — ``<split>/bucket-<k>/part-*.parquet``), the buckets are encoded
ONE AT A TIME: each bucket holds whole loans (loan-hash routing), so only ``rows/buckets`` ever
sits in RAM — that's what makes the 100% corpus encodable on one box. Shards are named
``shard-<bucket>-<i>.parquet``; the manifest is identical in shape either way, so pretraining
does not change. A single parquet input is the unchanged v1.0 path.

Config-driven (recipe: ``configs/fannie_mae/encode.yaml``)::

    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml            # train split
    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml --split val
    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml --workers 8
    # streamed split (input is a dir): same command — bucket layout is auto-detected
    python scripts/encode_dataset.py -c configs/fannie_mae/encode.yaml \
        --input gs://.../processed/.../train
"""

from __future__ import annotations

import json
import re
import time

from credit_fm.data.encode import encode_to_shards
from credit_fm.data.streaming import list_buckets
from credit_fm.tokenizer import KVTTokenizer
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def _encode_panel_to(tok, cfg, panel, name_prefix: str):
    """Encode one in-RAM panel to shards under ``cfg.output``; return (shards, loans, tokens)."""
    engine = cfg.get_path("engine", "cpu")
    if engine in ("vector", "gpu"):                    # vectorized (NumPy) / RAPIDS (cuDF+CuPy)
        from credit_fm.tokenizer.fast_encode import encode_panel_fast
        frame = encode_panel_fast(tok, panel, gpu=(engine == "gpu"))
        shards = []
        for i in range(0, len(frame), cfg.shard_size):
            name = f"{name_prefix}{i // cfg.shard_size:05d}.parquet"
            sub = frame.iloc[i:i + cfg.shard_size]
            storage.write_parquet(sub, storage.join(cfg.output, name))
            shards.append(name)
            print(f"  wrote {name}  ({len(sub):,} loans, {int(sub['n_tokens'].sum()):,} tokens)",
                  flush=True)
        return shards, len(frame), int(frame["n_tokens"].sum())
    return encode_to_shards(tok, cfg.tokenizer, panel, cfg.output, shard_size=cfg.shard_size,
                            workers=cfg.workers, key=cfg.key, name_prefix=name_prefix)


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/encode.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'split', 'input', 'output', 'shard_size', 'workers', 'engine')}",
          flush=True)

    storage.ensure_auth(cfg.input, cfg.key)
    storage.ensure_auth(cfg.output, cfg.key)
    tok = KVTTokenizer.load(cfg.tokenizer)
    print(f"loaded tokenizer ({tok.vocab_size:,} tokens) <- {cfg.tokenizer}", flush=True)

    t0 = time.time()
    buckets = list_buckets(cfg.input)
    if buckets:                                        # G3.2: streamed split — one bucket at a time
        print(f"bucketed input: {len(buckets)} loan-hash buckets (encoding one at a time)",
              flush=True)
        shards, n_loans, n_tokens = [], 0, 0
        for burl in buckets:
            b = int(re.search(r"bucket-(\d+)$", burl.rstrip("/")).group(1))
            panel = storage.read_parquet(burl)         # whole loans, rows/buckets of the split
            print(f"bucket {b:03d}: {panel[tok.id_col].nunique():,} loans "
                  f"({len(panel):,} rows) ...", flush=True)
            sh, nl, nt = _encode_panel_to(tok, cfg, panel, name_prefix=f"shard-{b:03d}-")
            shards += sh
            n_loans += nl
            n_tokens += nt
    else:
        print(f"reading {cfg.input} ...", flush=True)
        panel = storage.read_parquet(cfg.input)
        print(f"encoding {panel[tok.id_col].nunique():,} loans with workers={cfg.workers} ...",
              flush=True)
        shards, n_loans, n_tokens = _encode_panel_to(tok, cfg, panel, name_prefix="shard-")

    manifest = {
        "tokenizer": cfg.tokenizer, "vocab_size": tok.vocab_size,
        "source": cfg.input, "n_loans": n_loans, "n_tokens": n_tokens,
        "n_shards": len(shards), "shard_size": cfg.shard_size, "shards": sorted(shards),
        "columns": ["input_ids", "event_index", "field_type", "branch", "n_tokens", "n_events"],
        "config": cfg.to_dict(),                                   # lineage
    }
    storage.write_text(json.dumps(manifest, indent=2, default=str),
                       storage.join(cfg.output, "manifest.json"))
    print(f"done: {n_loans:,} loans, {n_tokens:,} tokens, {len(shards)} shards "
          f"-> {cfg.output}  ({time.time()-t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
