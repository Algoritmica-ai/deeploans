# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Tests for the YAML config engine (includes, interpolation, overrides)."""

import pytest

from credit_fm.utils.config import Config, load_config, parse_overrides


@pytest.fixture()
def tree(tmp_path):
    (tmp_path / "common.yaml").write_text(
        "gcs_root: gs://bucket\n"
        "run_name: run_a\n"
        "seed: 42\n"
        "paths:\n"
        "  encoded: ${gcs_root}/encoded/${run_name}\n"
    )
    (tmp_path / "pretrain.yaml").write_text(
        "include: common.yaml\n"
        "data:\n"
        "  train_dir: ${paths.encoded}/train\n"
        "  limit: null\n"
        "model:\n"
        "  dim: 384\n"
        "  dropout: 0.1\n"
    )
    return tmp_path


def test_include_and_interpolation(tree):
    cfg = load_config(tree / "pretrain.yaml")
    assert cfg.seed == 42                                       # merged from include
    assert cfg.data.train_dir == "gs://bucket/encoded/run_a/train"


def test_full_string_reference_keeps_type(tmp_path):
    (tmp_path / "c.yaml").write_text("a: 7\nb: ${a}\nc: 'x-${a}'\n")
    cfg = load_config(tmp_path / "c.yaml")
    assert cfg.b == 7 and isinstance(cfg.b, int)
    assert cfg.c == "x-7"


def test_overrides_are_yaml_typed(tree):
    cfg = load_config(tree / "pretrain.yaml",
                      {"model.dim": 512, "data.limit": 1000, "run_name": "run_b"})
    assert cfg.model.dim == 512
    assert cfg.data.limit == 1000
    assert cfg.data.train_dir.endswith("run_b/train")           # override wins pre-interpolation


def test_parse_overrides_forms():
    ov = parse_overrides(["--model.dim", "512", "--data.limit=null", "--runtime.bf16"])
    assert ov == {"model.dim": 512, "data.limit": None, "runtime.bf16": True}


def test_parse_overrides_rejects_positional():
    with pytest.raises(SystemExit):
        parse_overrides(["oops"])


def test_missing_key_raises_with_context(tree):
    cfg = load_config(tree / "pretrain.yaml")
    with pytest.raises(AttributeError, match="no key 'optimizer'"):
        _ = cfg.optimizer
    assert cfg.get_path("optimizer.lr", 3e-4) == 3e-4


def test_missing_interpolation_raises(tmp_path):
    (tmp_path / "bad.yaml").write_text("a: ${nope.x}\n")
    with pytest.raises(KeyError, match="nope.x"):
        load_config(tmp_path / "bad.yaml")


def test_to_dict_roundtrip(tree):
    cfg = load_config(tree / "pretrain.yaml")
    d = cfg.to_dict()
    assert isinstance(d, dict) and not isinstance(d["model"], Config)
    assert d["model"]["dim"] == 384


def test_yaml_dates_normalize_to_iso_strings(tmp_path):
    import json

    import yaml as _yaml
    (tmp_path / "d.yaml").write_text("cutoff: 2022-12-31\nnested:\n  dates: [2016-12-31, 2017-12-31]\n")
    cfg = load_config(tmp_path / "d.yaml", {"reporting_max": _yaml.safe_load("2022-12-31")})
    assert cfg.cutoff == "2022-12-31" and isinstance(cfg.cutoff, str)
    assert cfg.nested.dates == ["2016-12-31", "2017-12-31"]
    assert cfg.reporting_max == "2022-12-31"
    json.dumps(cfg.to_dict())
