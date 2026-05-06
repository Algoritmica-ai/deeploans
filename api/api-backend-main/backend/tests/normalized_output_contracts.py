"""Schema contracts for normalized ETL outputs.

The contracts in this module intentionally focus on stable downstream
compatibility guarantees: required field presence and broad JSON-compatible
basic types. They are small and declarative so additional asset classes or
normalized outputs can be added without changing the validation test logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Real
from typing import Any, Mapping


@dataclass(frozen=True)
class FieldContract:
    """Expected basic JSON type for a required normalized output field."""

    json_type: str
    nullable: bool = False


@dataclass(frozen=True)
class NormalizedOutputContract:
    """Required-field contract for one normalized output dataset."""

    asset_class: str
    dataset_name: str
    required_fields: Mapping[str, FieldContract]


JSON_TYPE_NAMES = {"string", "number", "boolean", "object", "array"}


DATA_CENTER_FACILITY_SILVER_CONTRACT = NormalizedOutputContract(
    asset_class="data_centers",
    dataset_name="facility_silver",
    required_fields={
        "facility_id": FieldContract("string"),
        "sponsor": FieldContract("string"),
        "country": FieldContract("string"),
        "report_date": FieldContract("string"),
        "noi_eur": FieldContract("number"),
        "dscr": FieldContract("number"),
        "ltv_pct": FieldContract("number"),
        "occupancy_pct": FieldContract("number"),
        "energy_cost_ratio_pct": FieldContract("number"),
        "junior_note_watch_status": FieldContract("string"),
    },
)


NORMALIZED_OUTPUT_CONTRACTS = (DATA_CENTER_FACILITY_SILVER_CONTRACT,)


def matches_json_type(value: Any, expected_json_type: str) -> bool:
    """Return whether a value matches a broad JSON-compatible basic type."""

    if expected_json_type not in JSON_TYPE_NAMES:
        raise ValueError(f"Unsupported JSON type in contract: {expected_json_type}")

    if expected_json_type == "string":
        return isinstance(value, str)
    if expected_json_type == "number":
        return isinstance(value, Real) and not isinstance(value, bool)
    if expected_json_type == "boolean":
        return isinstance(value, bool)
    if expected_json_type == "object":
        return isinstance(value, dict)
    if expected_json_type == "array":
        return isinstance(value, list)

    return False
