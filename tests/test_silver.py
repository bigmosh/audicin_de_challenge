import duckdb
import pandas as pd
from pathlib import Path


def test_silver_dedup_and_quality(tmp_path: Path):
    bronze_path = tmp_path / "events_raw.parquet"

    df = pd.DataFrame([
        {
            "event_id": "e1",
            "user_id": "u1",
            "event_type": "purchase",
            "timestamp": "2026-01-01T10:00:00Z",
            "schema_version": "1",
            "amount": "10",
            "currency": "usd",
            "tax": "1",
            "page": None,
        },
        {
            "event_id": "e1",
            "user_id": "u1",
            "event_type": "purchase",
            "timestamp": "2026-01-01T12:00:00Z",
            "schema_version": "1",
            "amount": "20",
            "currency": "usd",
            "tax": "2",
            "page": None,
        },
        {
            "event_id": "e2",
            "user_id": "u2",
            "event_type": "purchase",
            "timestamp": "invalid_timestamp",
            "schema_version": "1",
            "amount": "5",
            "currency": "usd",
            "tax": "0.5",
            "page": None,
        },
    ])

    df.to_parquet(bronze_path, index=False)

    conn = duckdb.connect()

    conn.execute(f"""
        CREATE OR REPLACE TABLE events_typed AS
        SELECT
          event_id,
          user_id,
          lower(trim(event_type)) AS event_type,
          try_cast(timestamp AS TIMESTAMP) AS event_ts,
          CAST(try_cast(timestamp AS TIMESTAMP) AS DATE) AS event_date,
          try_cast(schema_version AS INTEGER) AS schema_version,
          upper(trim(currency)) AS currency,
          try_cast(amount AS DOUBLE) AS amount,
          try_cast(tax AS DOUBLE) AS tax,
          page
        FROM read_parquet('{bronze_path}')
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE events_dedup AS
        SELECT * EXCLUDE (rn)
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY event_id
                   ORDER BY event_ts DESC NULLS LAST
                 ) AS rn
          FROM events_typed
        )
        WHERE rn = 1
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE events_quality_issues AS
        SELECT *
        FROM events_dedup
        WHERE event_id IS NULL
           OR user_id IS NULL
           OR event_ts IS NULL
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE events_silver AS
        SELECT *
        FROM events_dedup
        WHERE event_id IS NOT NULL
          AND user_id IS NOT NULL
          AND event_ts IS NOT NULL
    """)

    result = conn.execute("""
        SELECT event_id, amount
        FROM events_silver
    """).fetchall()

    assert len(result) == 1
    assert result[0][0] == "e1"
    assert result[0][1] == 20.0

    issues = conn.execute("""
        SELECT event_id
        FROM events_quality_issues
    """).fetchall()

    assert len(issues) == 1
    assert issues[0][0] == "e2"

    conn.close()