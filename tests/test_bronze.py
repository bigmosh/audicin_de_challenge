import pytest
from pathlib import Path

from pipeline.bronze.parser import parse_events_ndjson, stringify_record_values


def test_parse_events_ndjson_skips_corrupted_lines(tmp_path: Path) -> None:
    ndjson = tmp_path / "events.ndjson"
    ndjson.write_text(
        '\n'.join([
            '{"event_id": "e1", "user_id": "u1", "event_type": "login", "timestamp": "2026-01-01T00:00:00Z"}',
            '{"event_id": "broken", "user_id": "u2",',
            '',
        ]),
        encoding="utf-8",
    )

    df, skipped = parse_events_ndjson(ndjson)

    assert skipped == 1
    assert len(df) == 1
    assert df.iloc[0]["event_id"] == "e1"


def test_stringify_record_values_turns_non_numeric_into_string() -> None:
    record = {
        "event_id": "e1",
        "amount": "ten",
        "tax": 0.5,
        "meta": {"a": 1},
        "items": [1, 2],
        "noneval": None,
    }

    out = stringify_record_values(record)

    assert out["amount"] == "ten"
    assert out["tax"] == "0.5"
    assert out["meta"] == '{"a": 1}'
    assert out["items"] == "[1, 2]"
    assert out["noneval"] is None