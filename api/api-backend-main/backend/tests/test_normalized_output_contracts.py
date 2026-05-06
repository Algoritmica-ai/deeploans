"""Contract tests for normalized loan-level ETL outputs."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from tests.normalized_output_contracts import (
    DATA_CENTER_FACILITY_SILVER_CONTRACT,
    NORMALIZED_OUTPUT_CONTRACTS,
    FieldContract,
    NormalizedOutputContract,
    matches_json_type,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
DATA_CENTER_PIPELINE_SRC = REPO_ROOT / "etl-pipelines" / "exotic" / "data-centers" / "src"
sys.path.insert(0, str(DATA_CENTER_PIPELINE_SRC))

from data_center_etl_pipeline.generate_asset_silver import generate_asset_silver  # noqa: E402
from data_center_etl_pipeline.generate_bronze_tables import generate_bronze_tables  # noqa: E402
from data_center_etl_pipeline.io import read_csv  # noqa: E402

DATA_CENTER_SAMPLE_INPUT = (
    REPO_ROOT / "etl-pipelines" / "exotic" / "data-centers" / "sample_data" / "raw_facilities.csv"
)


def _validate_record_contract(
    record: Mapping[str, Any], contract: NormalizedOutputContract
) -> list[str]:
    """Validate one JSON-like normalized output record and return drift messages."""

    failures: list[str] = []
    for field_name, field_contract in contract.required_fields.items():
        if field_name not in record:
            failures.append(
                f"{contract.asset_class}.{contract.dataset_name}: missing required field "
                f"'{field_name}'"
            )
            continue

        value = record[field_name]
        if value is None:
            if not field_contract.nullable:
                failures.append(
                    f"{contract.asset_class}.{contract.dataset_name}.{field_name}: "
                    "expected non-null value"
                )
            continue

        if not matches_json_type(value, field_contract.json_type):
            failures.append(
                f"{contract.asset_class}.{contract.dataset_name}.{field_name}: expected "
                f"{field_contract.json_type}, got {type(value).__name__}"
            )

    return failures


def assert_records_match_contract(
    records: Sequence[Mapping[str, Any]], contract: NormalizedOutputContract
) -> None:
    """Assert every normalized output record satisfies its required schema contract."""

    assert records, f"{contract.asset_class}.{contract.dataset_name}: no records to validate"

    failures = []
    for index, record in enumerate(records):
        for failure in _validate_record_contract(record, contract):
            failures.append(f"record {index}: {failure}")

    assert not failures, "Normalized output schema contract drift detected:\n" + "\n".join(
        failures
    )


def _generate_data_center_facility_silver_json_records() -> list[dict[str, Any]]:
    raw_rows = read_csv(str(DATA_CENTER_SAMPLE_INPUT))
    bronze_rows = generate_bronze_tables(raw_rows)
    silver_rows = generate_asset_silver(bronze_rows)

    # Contract tests validate the JSON shape exposed to downstream consumers,
    # so force a JSON round trip to catch non-serializable values early.
    return json.loads(json.dumps(silver_rows))


@pytest.mark.parametrize("contract", NORMALIZED_OUTPUT_CONTRACTS)
def test_contract_definitions_use_supported_basic_json_types(
    contract: NormalizedOutputContract,
) -> None:
    for field_name, field_contract in contract.required_fields.items():
        assert field_contract.json_type in {
            "string",
            "number",
            "boolean",
            "object",
            "array",
        }, f"{contract.asset_class}.{contract.dataset_name}.{field_name} has unsupported type"


def test_data_center_facility_silver_normalized_json_matches_contract() -> None:
    records = _generate_data_center_facility_silver_json_records()

    assert_records_match_contract(records, DATA_CENTER_FACILITY_SILVER_CONTRACT)


def test_contract_validation_reports_removed_required_field() -> None:
    record = _generate_data_center_facility_silver_json_records()[0]
    record.pop("facility_id")

    failures = _validate_record_contract(record, DATA_CENTER_FACILITY_SILVER_CONTRACT)

    assert any("missing required field 'facility_id'" in failure for failure in failures)


def test_contract_validation_reports_incompatible_required_field_type() -> None:
    record = _generate_data_center_facility_silver_json_records()[0]
    record["dscr"] = "1.667"

    failures = _validate_record_contract(record, DATA_CENTER_FACILITY_SILVER_CONTRACT)

    assert any("facility_silver.dscr: expected number, got str" in failure for failure in failures)


def test_nullable_contract_fields_allow_null_values() -> None:
    contract = NormalizedOutputContract(
        asset_class="example",
        dataset_name="normalized_output",
        required_fields={"optional_metric": FieldContract("number", nullable=True)},
    )

    assert _validate_record_contract({"optional_metric": None}, contract) == []
