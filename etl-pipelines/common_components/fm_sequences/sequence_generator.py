"""Sequence generation utilities for SME and residential mortgage snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class FeatureToken:
    """Map a source column to an output token key."""

    column: str
    token: str
    missing_token: str = "__MISSING__"


@dataclass(frozen=True)
class SequenceSchema:
    """Schema that converts ETL snapshots into token events."""

    name: str
    entity_type: str
    id_fields: Tuple[str, ...]
    time_fields: Tuple[str, ...]
    features: Tuple[FeatureToken, ...]


@dataclass(frozen=True)
class SequenceEvent:
    entity_type: str
    entity_id: str
    as_of_date: str
    sequence_index: int
    event_kind: str
    token: str
    value: str
    source_schema: str

    def to_dict(self) -> Dict[str, str | int]:
        return {
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "as_of_date": self.as_of_date,
            "sequence_index": self.sequence_index,
            "event_kind": self.event_kind,
            "token": self.token,
            "value": self.value,
            "source_schema": self.source_schema,
        }


SME_FIRST_PASS_SCHEMA = SequenceSchema(
    name="ecb_sme_asset_silver_v1",
    entity_type="sme_loan",
    id_fields=("AS3", "loan_id", "asset_id"),
    time_fields=("AS1", "snapshot_date", "as_of_date", "report_date"),
    features=(
        FeatureToken("dl_code", "asset_class"),
        FeatureToken("AS2", "obligor_id"),
        FeatureToken("AS27", "orig_balance"),
        FeatureToken("AS30", "current_balance"),
        FeatureToken("AS54", "interest_rate"),
        FeatureToken("AS115", "arrears_balance"),
        FeatureToken("AS121", "forbearance_flag"),
    ),
)

RMB_FIRST_PASS_SCHEMA = SequenceSchema(
    name="ecb_rmb_asset_silver_v1",
    entity_type="resi_mortgage",
    id_fields=("AR3", "loan_id", "asset_id"),
    time_fields=("AR1", "snapshot_date", "as_of_date", "report_date"),
    features=(
        FeatureToken("dl_code", "asset_class"),
        FeatureToken("AR2", "originator_loan_ref"),
        FeatureToken("AR19", "orig_balance"),
        FeatureToken("AR24", "current_balance"),
        FeatureToken("AR31", "interest_rate"),
        FeatureToken("AR90", "arrears_balance"),
        FeatureToken("AR122", "restructured_flag"),
    ),
)


def build_sme_rmb_event_sequence(
    sme_snapshots: Sequence[Mapping[str, Any]],
    rmb_snapshots: Sequence[Mapping[str, Any]],
    *,
    include_missing_values: bool = True,
) -> List[SequenceEvent]:
    """Build a merged event sequence across SME and RMB snapshots."""

    events = []
    events.extend(
        generate_events(
            sme_snapshots,
            SME_FIRST_PASS_SCHEMA,
            include_missing_values=include_missing_values,
        )
    )
    events.extend(
        generate_events(
            rmb_snapshots,
            RMB_FIRST_PASS_SCHEMA,
            include_missing_values=include_missing_values,
        )
    )

    events.sort(
        key=lambda e: (
            e.as_of_date,
            e.entity_type,
            e.entity_id,
            e.sequence_index,
            e.token,
        )
    )

    return [
        SequenceEvent(
            entity_type=e.entity_type,
            entity_id=e.entity_id,
            as_of_date=e.as_of_date,
            sequence_index=index,
            event_kind=e.event_kind,
            token=e.token,
            value=e.value,
            source_schema=e.source_schema,
        )
        for index, e in enumerate(events)
    ]


def generate_events(
    snapshots: Sequence[Mapping[str, Any]],
    schema: SequenceSchema,
    *,
    include_missing_values: bool = True,
) -> List[SequenceEvent]:
    """Convert snapshots for one schema into tokenizable events."""

    grouped: Dict[str, List[Tuple[str, Mapping[str, Any], int]]] = {}
    for row_index, snapshot in enumerate(snapshots):
        entity_id = _first_non_empty(snapshot, schema.id_fields)
        if entity_id is None:
            continue

        as_of_date = _extract_as_of_date(snapshot, schema.time_fields)
        grouped.setdefault(entity_id, []).append((as_of_date, snapshot, row_index))

    events: List[SequenceEvent] = []
    for entity_id, rows in grouped.items():
        rows.sort(key=lambda item: (item[0], item[2]))
        previous_token_values: Dict[str, str] = {}

        for snapshot_idx, (as_of_date, row, _) in enumerate(rows):
            event_kind = "entity_initialized" if snapshot_idx == 0 else "attribute_updated"
            for feature in schema.features:
                raw_value = row.get(feature.column)
                normalized = _normalize_value(raw_value)

                if normalized is None:
                    if not include_missing_values:
                        continue
                    normalized = feature.missing_token

                previous_value = previous_token_values.get(feature.token)
                if snapshot_idx == 0 or normalized != previous_value:
                    events.append(
                        SequenceEvent(
                            entity_type=schema.entity_type,
                            entity_id=entity_id,
                            as_of_date=as_of_date,
                            sequence_index=snapshot_idx,
                            event_kind=event_kind,
                            token=feature.token,
                            value=normalized,
                            source_schema=schema.name,
                        )
                    )

                previous_token_values[feature.token] = normalized

    return events


def render_for_tokenization(events: Iterable[SequenceEvent]) -> List[str]:
    return [
        (
            f"schema={event.source_schema} type={event.entity_type} "
            f"id={event.entity_id} date={event.as_of_date} "
            f"token={event.token} value={event.value}"
        )
        for event in events
    ]


def _extract_as_of_date(snapshot: Mapping[str, Any], time_fields: Sequence[str]) -> str:
    raw_value = _first_non_empty(snapshot, time_fields)
    if raw_value:
        parsed = _parse_date(raw_value)
        if parsed is not None:
            return parsed
        return str(raw_value).strip()

    year = snapshot.get("pcd_year")
    month = snapshot.get("pcd_month")
    if year is not None and month is not None:
        return f"{int(year):04d}-{int(month):02d}-01"

    return "1970-01-01"


def _parse_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _first_non_empty(snapshot: Mapping[str, Any], candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate not in snapshot:
            continue
        normalized = _normalize_value(snapshot[candidate])
        if normalized is not None:
            return normalized
    return None


def _normalize_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
