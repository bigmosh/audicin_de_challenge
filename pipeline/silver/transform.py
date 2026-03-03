from __future__ import annotations

from pipeline.config.paths import (
    BRONZE_EVENTS,
    BRONZE_MARKETING,
    BRONZE_SUBSCRIPTIONS,
    DATA_DIR,
)
from pipeline.utils.db import export_table_to_parquet, get_connection

SILVER_DIR = DATA_DIR / "silver"
SILVER_EVENTS = SILVER_DIR / "events_silver.parquet"
SILVER_EVENTS_QUALITY_ISSUES = SILVER_DIR / "events_quality_issues.parquet"
SILVER_SUBSCRIPTIONS = SILVER_DIR / "subscriptions_silver.parquet"
SILVER_MARKETING_SPEND = SILVER_DIR / "marketing_spend_silver.parquet"


def _count_rows(conn, table_name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def build_silver() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        conn.execute(
            """
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
            FROM read_parquet(?)
            """,
            [str(BRONZE_EVENTS)],
        )

        conn.execute(
            """
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
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE events_quality_issues AS
            SELECT *,
              CASE
                WHEN event_id IS NULL THEN 'missing_event_id'
                WHEN user_id IS NULL THEN 'missing_user_id'
                WHEN event_ts IS NULL THEN 'invalid_timestamp'
              END AS quality_issue_reason
            FROM events_dedup
            WHERE event_id IS NULL
               OR user_id IS NULL
               OR event_ts IS NULL
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE events_silver AS
            SELECT *
            FROM events_dedup
            WHERE event_id IS NOT NULL
              AND user_id IS NOT NULL
              AND event_ts IS NOT NULL
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE subscriptions_typed AS
            SELECT
              subscription_id,
              user_id,
              lower(trim(plan_id)) AS plan_id,
              try_cast(price AS DOUBLE) AS price,
              upper(trim(currency)) AS currency,
              try_cast(start_date AS DATE) AS start_date,
              try_cast(end_date AS DATE) AS end_date,
              lower(trim(status)) AS status,
              try_cast(created_at AS TIMESTAMP) AS created_at
            FROM read_parquet(?)
            """,
            [str(BRONZE_SUBSCRIPTIONS)],
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE subscriptions_silver AS
            SELECT * EXCLUDE (rn)
            FROM (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY subscription_id
                       ORDER BY created_at DESC NULLS LAST
                     ) AS rn
              FROM subscriptions_typed
            )
            WHERE rn = 1
            """
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE marketing_typed AS
            SELECT
              try_cast(date AS DATE) AS date,
              lower(trim(channel)) AS channel,
              try_cast(spend AS DOUBLE) AS spend
            FROM read_parquet(?)
            """,
            [str(BRONZE_MARKETING)],
        )

        conn.execute(
            """
            CREATE OR REPLACE TABLE marketing_spend_silver AS
            SELECT
              date,
              channel,
              SUM(spend) AS spend
            FROM marketing_typed
            WHERE spend >= 0
              AND date IS NOT NULL
              AND channel IS NOT NULL
            GROUP BY date, channel
            """
        )

        export_table_to_parquet(conn, "events_quality_issues", SILVER_EVENTS_QUALITY_ISSUES)
        export_table_to_parquet(conn, "events_silver", SILVER_EVENTS)
        export_table_to_parquet(conn, "subscriptions_silver", SILVER_SUBSCRIPTIONS)
        export_table_to_parquet(conn, "marketing_spend_silver", SILVER_MARKETING_SPEND)

        row_counts = {
            "events_quality_issues": _count_rows(conn, "events_quality_issues"),
            "events_silver": _count_rows(conn, "events_silver"),
            "subscriptions_silver": _count_rows(conn, "subscriptions_silver"),
            "marketing_spend_silver": _count_rows(conn, "marketing_spend_silver"),
        }

    for table_name, row_count in row_counts.items():
        print(f"{table_name} rows: {row_count}")


if __name__ == "__main__":
    build_silver()
