# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Fit the KVT tokenizer on a credit panel, save it, and write a token QA report (Milestone M1).

Reads a per-loan **monthly** panel (loan_id × reporting_date rows), fits the per-field
bucketers/categoricals on TRAIN only, serializes the tokenizer to JSON, then encodes a sample of
loans to report vocabulary size, sequence-length distribution, and token health (roundtrip,
unseen-category and missing rates).

Config-driven (recipe: ``configs/fannie_mae/tokenizer_fit.yaml``; the field schema itself
comes from the recipe's ``schema`` key, i.e. ``configs/fannie_mae/tokenizer.yaml``)::

    python scripts/train_tokenizer.py -c configs/fannie_mae/tokenizer_fit.yaml
    python scripts/train_tokenizer.py -c configs/fannie_mae/tokenizer_fit.yaml --max_fit_rows 500000
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import yaml

from credit_fm.tokenizer import KVTTokenizer
from credit_fm.tokenizer.vocabulary import SPECIAL_TOKENS
from credit_fm.utils import storage
from credit_fm.utils.config import parse_cli, summarize


def _prune_to_panel(cfg: dict, columns) -> list[str]:
    """Drop configured fields not present in the panel; return the dropped names."""
    present = set(columns)
    dropped: list[str] = []
    for branch in ("profile", "event"):
        for kind in ("numeric", "categorical"):
            fields = cfg.get(branch, {}).get(kind, [])
            cfg[branch][kind] = [f for f in fields if f in present]
            dropped += [f for f in fields if f not in present]
    return dropped


def main() -> None:
    cfg = parse_cli(__doc__, default_config="configs/fannie_mae/tokenizer_fit.yaml")
    print(f"config: {cfg.config_path}\n"
          f"{summarize(cfg, 'schema', 'train', 'out', 'report', 'qa_loans', 'max_fit_rows')}",
          flush=True)

    schema = yaml.safe_load(open(cfg.schema))
    storage.ensure_auth(cfg.train, cfg.key)
    print(f"Loading {cfg.train} ...")
    panel = storage.read_parquet(cfg.train)

    dropped = _prune_to_panel(schema, panel.columns)
    if dropped:
        print(f"  skipping {len(dropped)} configured fields absent from panel: "
              f"{dropped[:8]}{' ...' if len(dropped) > 8 else ''}")
    for req in (schema["id_col"], schema["time_col"], schema["time_field"]):
        if req not in panel.columns:
            raise SystemExit(f"required column '{req}' missing from panel")

    fit_df = panel
    max_fit_rows = cfg.get_path("max_fit_rows")
    if max_fit_rows and len(panel) > max_fit_rows:
        fit_df = panel.sample(max_fit_rows, random_state=42)
    t0 = time.time()
    tok = KVTTokenizer(schema).fit(fit_df)
    print(f"fit: {tok.vocab_size:,} vocab tokens on {len(fit_df):,} rows in {time.time()-t0:.0f}s")

    Path(cfg.out).parent.mkdir(parents=True, exist_ok=True)
    tok.save(cfg.out)
    print(f"saved tokenizer -> {cfg.out}")

    # ---- QA on a sample of loans ----
    idc = schema["id_col"]
    loan_ids = panel[idc].drop_duplicates()
    qa_ids = set(loan_ids.sample(min(len(loan_ids), cfg.qa_loans), random_state=1))
    sample = panel[panel[idc].isin(qa_ids)]
    lengths, unk, na, total, lossless, n_loans = [], 0, 0, 0, 0, 0
    for _, loan in sample.groupby(idc):
        toks = tok.tokens(loan)
        ids = [tok.vocabulary.encode(t) for t in toks]
        lossless += int(tok.decode(ids) == toks)
        lengths.append(len(ids))
        n_loans += 1
        for tk in toks:
            total += 1
            unk += int(tk.endswith("=UNK"))
            na += int(tk.endswith("=NA"))
    lens = np.array(lengths)

    def pct(x: int) -> float:
        return 100.0 * x / max(total, 1)

    n_pf = len(schema["profile"]["numeric"]) + len(schema["profile"]["categorical"])
    n_ev = len(schema["event"]["numeric"]) + len(schema["event"]["categorical"])
    lines = [
        "# Fannie Mae — KVT Tokenizer Report (M1)", "",
        f"Fitted on `{cfg.train}` ({len(fit_df):,} rows). Schema `{cfg.schema}`; "
        f"saved to `{cfg.out}`.", "",
        "## Vocabulary", "",
        f"- **{tok.vocab_size:,} tokens** ({len(SPECIAL_TOKENS)} special + field value tokens).",
        f"- **Profile** {n_pf} fields ({len(schema['profile']['numeric'])} numeric / "
        f"{len(schema['profile']['categorical'])} categorical); **Event** {n_ev} fields "
        f"({len(schema['event']['numeric'])} numeric / {len(schema['event']['categorical'])} categorical); "
        f"time field `{schema['time_field']}`.",
        "", f"## Sequence length (QA sample: {n_loans:,} loans)", "",
        "| stat | tokens / loan |", "|---|--:|",
        f"| min | {int(lens.min())} |",
        f"| median | {int(np.median(lens))} |",
        f"| p95 | {int(np.percentile(lens, 95))} |",
        f"| max | {int(lens.max())} |",
        "", "## Token health", "",
        "| metric | value |", "|---|--:|",
        f"| roundtrip lossless | {100.0*lossless/max(n_loans,1):.1f}% of loans |",
        f"| unseen-category tokens (`=UNK`) | {pct(unk):.2f}% |",
        f"| missing tokens (`=NA`) | {pct(na):.2f}% |",
        "", "## Notes",
        "- Bins/categories fit on TRAIN only; unseen values map to `=UNK`, missing to `=NA`.",
        "- Roundtrip is token-level lossless (fused `field=value` tokens); numeric exact values are "
        "bucketed by design, so the QA target is losslessness + low OOV, not value reconstruction.",
    ]
    Path(cfg.report).parent.mkdir(parents=True, exist_ok=True)
    Path(cfg.report).write_text("\n".join(lines))
    print(f"wrote {cfg.report}  (roundtrip {100.0*lossless/max(n_loans,1):.1f}%, "
          f"median {int(np.median(lens))} tokens/loan)")


if __name__ == "__main__":
    main()
