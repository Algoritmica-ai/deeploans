import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fm_sequences.sequence_generator import (  # noqa: E402
    SME_FIRST_PASS_SCHEMA,
    generate_events,
)


def test_sequence_ordering_for_same_entity_by_date_then_input_order():
    snapshots = [
        {"AS3": "L-1", "AS1": "2025-02-01", "AS30": 90},
        {"AS3": "L-1", "AS1": "2025-01-01", "AS30": 100},
        {"AS3": "L-1", "AS1": "2025-02-01", "AS30": 80},
    ]

    events = generate_events(snapshots, SME_FIRST_PASS_SCHEMA)
    current_balance_events = [e for e in events if e.token == "current_balance"]

    assert [e.as_of_date for e in current_balance_events] == [
        "2025-01-01",
        "2025-02-01",
        "2025-02-01",
    ]
    assert [e.value for e in current_balance_events] == ["100", "90", "80"]


def test_missing_values_emit_missing_token_and_missing_id_rows_are_skipped():
    snapshots = [
        {"AS3": "L-2", "AS1": "2025-03-01", "AS121": None},
        {"AS1": "2025-04-01", "AS121": "Y"},
    ]

    events = generate_events(snapshots, SME_FIRST_PASS_SCHEMA, include_missing_values=True)

    forbearance_events = [e for e in events if e.token == "forbearance_flag"]
    assert len(forbearance_events) == 1
    assert forbearance_events[0].value == "__MISSING__"
    assert all(event.entity_id == "L-2" for event in events)
