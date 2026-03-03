from pathlib import Path
from typing import Any, Dict, List, Tuple
import json
import pandas as pd


def stringify_record_values(record: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in record.items():
        if v is None:
            out[k] = None
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = str(v)
    return out


def parse_events_ndjson(events_path: Path) -> Tuple[pd.DataFrame, int]:
    records: List[Dict[str, Any]] = []
    skipped = 0

    with events_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                skipped += 1
                continue

            if not isinstance(parsed, dict):
                skipped += 1
                continue

            records.append(stringify_record_values(parsed))

    return pd.DataFrame(records), skipped