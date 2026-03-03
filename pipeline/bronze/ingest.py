from __future__ import annotations

from dataclasses import dataclass
from pipeline.utils.db import get_connection, export_table_to_parquet
from pipeline.bronze.parser import parse_events_ndjson
from pipeline.config.paths import (
    BRONZE_DIR,
    BRONZE_EVENTS,
    BRONZE_SUBSCRIPTIONS,
    BRONZE_MARKETING,
    RAW_EVENTS,
    RAW_SUBSCRIPTIONS,
    RAW_MARKETING,
)
@dataclass
class BronzeIngestResult:
    table: str
    rows: int
    notes: str = ""

def ingest_events(conn) -> BronzeIngestResult:
    events_df, skipped_events = parse_events_ndjson(RAW_EVENTS)

    conn.register("events_df", events_df)
    conn.execute("CREATE OR REPLACE TABLE events_raw AS SELECT * FROM events_df")
    conn.unregister("events_df")

    export_table_to_parquet(conn, "events_raw", BRONZE_EVENTS)

    rows = conn.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
    return BronzeIngestResult(
        table="events_raw",
        rows=rows,
        notes=f"skipped_corrupted_lines={skipped_events}",
    )


def ingest_subscriptions(conn) -> BronzeIngestResult:
    conn.execute(
        """
        CREATE OR REPLACE TABLE subscriptions_raw AS
        SELECT * FROM read_json_auto(?)
        """,
        [str(RAW_SUBSCRIPTIONS)],
    )

    export_table_to_parquet(conn, "subscriptions_raw", BRONZE_SUBSCRIPTIONS)

    rows = conn.execute("SELECT COUNT(*) FROM subscriptions_raw").fetchone()[0]
    return BronzeIngestResult(table="subscriptions_raw", rows=rows)


def ingest_marketing_spend(conn) -> BronzeIngestResult:
    conn.execute(
        """
        CREATE OR REPLACE TABLE marketing_spend_raw AS
        SELECT * FROM read_csv_auto(?, header=true)
        """,
        [str(RAW_MARKETING)],
    )

    export_table_to_parquet(conn, "marketing_spend_raw", BRONZE_MARKETING)

    rows = conn.execute("SELECT COUNT(*) FROM marketing_spend_raw").fetchone()[0]
    return BronzeIngestResult(table="marketing_spend_raw", rows=rows)

def load_bronze() -> None:
    """
    Bronze layer (full refresh):
    - loads events.ndjson -> events_raw
    - loads subscriptions.json -> subscriptions_raw
    - loads marketing_spend.csv -> marketing_spend_raw
    - exports Parquet to data/bronze/
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        results = [
            ingest_events(conn),
            ingest_subscriptions(conn),
            ingest_marketing_spend(conn),
        ]

    for r in results:
        msg = f"{r.table} rows loaded: {r.rows}"
        if r.notes:
            msg += f" ({r.notes})"
        print(msg)


if __name__ == "__main__":
    load_bronze()