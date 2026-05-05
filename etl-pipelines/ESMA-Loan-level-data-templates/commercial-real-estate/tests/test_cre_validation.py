from __future__ import annotations

import csv
import json
from pathlib import Path

from cre_etl_pipeline import pipeline
from cre_etl_pipeline.taxonomy import TaxonomyField


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "CREL1",
        "CREL2",
        "CREL3",
        "CREL4",
        "CREL5",
        "CREL19",
        "CREL26",
        "CREL32",
        "CREL40",
        "CREL41",
        "CREL44",
        "CREL59",
        "CREL68",
        "CREL71",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_cre_validation_summary_and_rule_flags(tmp_path, monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "load_cre_taxonomy",
        lambda _path: [TaxonomyField("sec", code, "n", "c", "f") for code in [
            "CREL1", "CREL2", "CREL3", "CREL4", "CREL5", "CREL19", "CREL26", "CREL32", "CREL40", "CREL41", "CREL44", "CREL59", "CREL68", "CREL71"
        ]],
    )

    input_csv = tmp_path / "input.csv"
    output_json = tmp_path / "output.json"
    _write_csv(
        input_csv,
        [
            {
                "CREL1": "deal-a",
                "CREL4": "loan-a",
                "CREL5": "loan-a",
                "CREL32": "1500000",
                "CREL41": "1000000",
                "CREL44": "1.4",
                "CREL59": "95",
            },
            {
                "CREL1": "deal-b",
                "CREL4": "loan-b",
                "CREL32": "1000000",
                "CREL41": "-500",
                "CREL44": "abc",
                "CREL59": "120",
            },
            {
                "CREL1": "deal-c",
                "CREL4": "",
                "CREL5": "",
                "CREL41": "500",
                "CREL44": "1.1",
                "CREL59": "50",
            },
        ],
    )

    result = pipeline.transform_underlying_exposures(
        input_csv=input_csv,
        taxonomy_xlsx="unused.xlsx",
        output_json=output_json,
        dl_code="CRE-TEST-1",
    )

    summary = result["quality"]["validation_summary"]
    assert summary["valid_record_count"] == 1
    assert summary["invalid_record_count"] == 2
    assert summary["rule_failures"]["negative_current_balance"] == 1
    assert summary["rule_failures"]["occupancy_out_of_range"] == 1
    assert summary["rule_failures"]["missing_loan_id"] == 1
    assert summary["rule_failures"]["non_numeric_dscr"] == 1

    records = result["records"]
    assert records[0]["validation"]["is_valid"] is True
    assert records[1]["validation"]["is_valid"] is False
    assert records[2]["validation"]["is_valid"] is False

    persisted = json.loads(output_json.read_text(encoding="utf-8"))
    assert persisted["quality"]["validation_summary"]["invalid_record_count"] == 2
