# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Config engine: YAML recipes with includes, interpolation, and dotted CLI overrides.

Every pipeline script runs from a YAML recipe (NVIDIA-blueprint style) instead of a wall of
argparse flags::

    python scripts/pretrain.py -c configs/mortgage_performance/pretrain.yaml \
        --model.dim 512 --schedule.steps 2000 --data.limit 1000

Features (PyYAML only — no hydra/omegaconf dependency):

- ``include: common.yaml`` — deep-merge a base file (path relative to the including file);
- ``${a.b}`` — interpolate another key's value (full-string references keep the value's type);
- dotted CLI overrides — ``--a.b.c value`` or ``--a.b.c=value``, YAML-parsed (so ``null``,
  ``true``, ``0.5``, ``[1,2]`` all work); a bare ``--a.b.c`` sets ``true``;
- attribute access — ``cfg.model.dim``; missing keys raise ``AttributeError`` with the full path;
- lineage — ``cfg.to_dict()`` is stored in checkpoints/manifests so every artifact records the
  exact resolved config that produced it.
"""

from __future__ import annotations

import argparse
import datetime
import re
from pathlib import Path
from typing import Any, Sequence

import yaml

_VAR = re.compile(r"\$\{([^}]+)\}")
_MAX_PASSES = 10


class Config(dict):
    """Dict with attribute access; nested dicts are wrapped on the way out."""

    def __getattr__(self, key: str) -> Any:
        try:
            val = self[key]
        except KeyError:
            raise AttributeError(f"config has no key '{key}' (have: {sorted(self)})") from None
        return Config(val) if isinstance(val, dict) else val

    def get_path(self, dotted: str, default: Any = None) -> Any:
        """Look up ``'a.b.c'``; return ``default`` if any segment is missing."""
        cur: Any = self
        for part in dotted.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    def to_dict(self) -> dict:
        """Plain nested dict (for checkpoints / manifests / json)."""
        return {k: (Config(v).to_dict() if isinstance(v, dict) else v) for k, v in self.items()}


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        out[k] = _deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _load_yaml(path: Path) -> dict:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top level must be a mapping, got {type(data).__name__}")
    includes = data.pop("include", None)
    if includes:
        merged: dict = {}
        for inc in [includes] if isinstance(includes, str) else includes:
            merged = _deep_merge(merged, _load_yaml((path.parent / inc).resolve()))
        data = _deep_merge(merged, data)
    return data


def _lookup(root: dict, dotted: str) -> Any:
    cur: Any = root
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"interpolation '${{{dotted}}}' not found in config")
        cur = cur[part]
    return cur


def _interpolate(node: Any, root: dict) -> Any:
    if isinstance(node, dict):
        return {k: _interpolate(v, root) for k, v in node.items()}
    if isinstance(node, list):
        return [_interpolate(v, root) for v in node]
    if isinstance(node, str):
        full = _VAR.fullmatch(node)
        if full:                                   # whole-string reference keeps the type
            return _lookup(root, full.group(1))
        return _VAR.sub(lambda m: str(_lookup(root, m.group(1))), node)
    return node


def _set_path(cfg: dict, dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    cur = cfg
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def parse_overrides(tokens: Sequence[str]) -> dict[str, Any]:
    """``['--a.b', '1', '--c=x', '--flag']`` -> ``{'a.b': 1, 'c': 'x', 'flag': True}``."""
    out: dict[str, Any] = {}
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if not tok.startswith("--"):
            raise SystemExit(f"unexpected argument '{tok}' (overrides look like --key.path value)")
        key = tok[2:]
        if "=" in key:
            key, raw = key.split("=", 1)
        elif i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
            i += 1
            raw = tokens[i]
        else:
            raw = "true"
        out[key] = yaml.safe_load(raw)
        i += 1
    return out


def _normalize(node: Any) -> Any:
    """YAML parses unquoted ISO dates into ``datetime.date`` objects; coerce them back to
    strings so configs stay plain-JSON-serializable (checkpoint/manifest lineage)."""
    if isinstance(node, dict):
        return {k: _normalize(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_normalize(v) for v in node]
    if isinstance(node, (datetime.date, datetime.datetime)):
        return node.isoformat()
    return node


def load_config(path: str | Path, overrides: dict[str, Any] | None = None) -> Config:
    """Load a recipe: includes -> CLI overrides -> ``${...}`` interpolation."""
    cfg = _load_yaml(Path(path).resolve())
    for dotted, value in (overrides or {}).items():
        _set_path(cfg, dotted, value)
    cfg = _normalize(cfg)
    for _ in range(_MAX_PASSES):
        resolved = _interpolate(cfg, cfg)
        if resolved == cfg:
            break
        cfg = resolved
    return Config(cfg)


def parse_cli(description: str | None = None, default_config: str | None = None) -> Config:
    """Standard entrypoint: ``-c/--config recipe.yaml`` plus dotted overrides."""
    ap = argparse.ArgumentParser(description=description,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-c", "--config", default=default_config, required=default_config is None,
                    help="YAML recipe (see configs/); remaining args are --key.path value overrides")
    args, extra = ap.parse_known_args()
    cfg = load_config(args.config, parse_overrides(extra))
    cfg["config_path"] = str(args.config)
    return cfg


def summarize(cfg: Config, *sections: str) -> str:
    """One line per key for the chosen sections (or all top-level scalars)."""
    lines = []
    src = {s: cfg.get_path(s) for s in sections} if sections else cfg
    for name, val in src.items():
        if isinstance(val, dict):
            body = ", ".join(f"{k}={v}" for k, v in val.items())
            lines.append(f"  {name}: {body}")
        else:
            lines.append(f"  {name}: {val}")
    return "\n".join(lines)
