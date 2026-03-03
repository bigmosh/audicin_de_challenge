from __future__ import annotations

from pipeline.config.paths import DATA_DIR
from pipeline.utils.db import export_table_to_parquet, get_connection

GOLD_DIR = DATA_DIR / "gold"
SILVER_DIR = DATA_DIR / "silver"

SILVER_EVENTS = SILVER_DIR / "events_silver.parquet"
SILVER_SUBSCRIPTIONS = SILVER_DIR / "subscriptions_silver.parquet"
SILVER_MARKETING_SPEND = SILVER_DIR / "marketing_spend_silver.parquet"

GOLD_DAILY_ACTIVE_USERS = GOLD_DIR / "daily_active_users.parquet"
GOLD_DAILY_REVENUE_GROSS = GOLD_DIR / "daily_revenue_gross.parquet"
GOLD_DAILY_REVENUE_NET = GOLD_DIR / "daily_revenue_net.parquet"
GOLD_MRR_MONTHLY = GOLD_DIR / "mrr_monthly.parquet"
GOLD_WEEKLY_COHORT_RETENTION = GOLD_DIR / "weekly_cohort_retention.parquet"
GOLD_CAC_BY_CHANNEL = GOLD_DIR / "cac_by_channel.parquet"
GOLD_LTV_PER_USER = GOLD_DIR / "ltv_per_user.parquet"
GOLD_LTV_CAC_RATIO = GOLD_DIR / "ltv_cac_ratio.parquet"


def _count_rows(conn, table_name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def build_gold() -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW events_silver AS
            SELECT * FROM read_parquet('{SILVER_EVENTS}')
        """)

        conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW subscriptions_silver AS
            SELECT * FROM read_parquet('{SILVER_SUBSCRIPTIONS}')
        """)

        conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW marketing_spend_silver AS
            SELECT * FROM read_parquet('{SILVER_MARKETING_SPEND}')
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE daily_active_users AS
            SELECT
              event_date,
              COUNT(DISTINCT user_id) AS dau
            FROM events_silver
            GROUP BY event_date
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE daily_revenue_gross AS
            SELECT
              event_date,
              SUM(
                CASE
                  WHEN event_type = 'purchase' THEN COALESCE(amount, 0)
                  ELSE 0
                END
              ) AS gross_revenue
            FROM events_silver
            GROUP BY event_date
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE daily_revenue_net AS
            SELECT
              event_date,
              SUM(
                CASE
                  WHEN event_type = 'purchase' THEN COALESCE(amount, 0)
                  WHEN event_type = 'refund' THEN -COALESCE(amount, 0)
                  ELSE 0
                END
              ) AS net_revenue
            FROM events_silver
            GROUP BY event_date
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE mrr_monthly AS
            SELECT
              date_trunc('month', start_date) AS month,
              SUM(price) AS mrr
            FROM subscriptions_silver
            WHERE status = 'active'
            GROUP BY month
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE cac_by_channel AS
            WITH signups AS (
              SELECT
                event_date,
                COUNT(*) AS signups
              FROM events_silver
              WHERE event_type = 'signup'
              GROUP BY event_date
            )
            SELECT
              m.date,
              m.channel,
              m.spend,
              COALESCE(s.signups, 0) AS signups,
              m.spend / NULLIF(COALESCE(s.signups, 0), 0) AS cac
            FROM marketing_spend_silver m
            LEFT JOIN signups s
              ON m.date = s.event_date
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE ltv_per_user AS
            SELECT
              user_id,
              SUM(
                CASE
                  WHEN event_type = 'purchase' THEN COALESCE(amount, 0)
                  WHEN event_type = 'refund' THEN -COALESCE(amount, 0)
                  ELSE 0
                END
              ) AS lifetime_value
            FROM events_silver
            GROUP BY user_id
        """)


        conn.execute("""
            CREATE OR REPLACE TABLE ltv_cac_ratio AS
            WITH avg_ltv AS (
              SELECT AVG(lifetime_value) AS avg_ltv
              FROM ltv_per_user
            ),
            avg_cac AS (
              SELECT AVG(cac) AS avg_cac
              FROM cac_by_channel
              WHERE cac IS NOT NULL
            )
            SELECT
              avg_ltv,
              avg_cac,
              avg_ltv / NULLIF(avg_cac, 0) AS ltv_cac_ratio
            FROM avg_ltv, avg_cac
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE weekly_cohort_retention AS
            WITH cohorts AS (
              SELECT
                user_id,
                date_trunc('week', MIN(event_date)) AS cohort_week
              FROM events_silver
              WHERE event_type = 'signup'
              GROUP BY user_id
            ),
            activity AS (
              SELECT DISTINCT
                user_id,
                date_trunc('week', event_date) AS activity_week
              FROM events_silver
            ),
            cohort_activity AS (
              SELECT
                c.cohort_week,
                a.user_id,
                date_diff('week', c.cohort_week, a.activity_week) AS weeks_since_signup
              FROM cohorts c
              JOIN activity a
                ON c.user_id = a.user_id
              WHERE a.activity_week >= c.cohort_week
            ),
            cohort_sizes AS (
              SELECT
                cohort_week,
                COUNT(DISTINCT user_id) AS cohort_size
              FROM cohorts
              GROUP BY cohort_week
            ),
            retained AS (
              SELECT
                cohort_week,
                weeks_since_signup,
                COUNT(DISTINCT user_id) AS active_users
              FROM cohort_activity
              GROUP BY cohort_week, weeks_since_signup
            )
            SELECT
              r.cohort_week,
              r.weeks_since_signup,
              cs.cohort_size,
              r.active_users,
              CAST(r.active_users AS DOUBLE) / NULLIF(cs.cohort_size, 0) AS retention_rate
            FROM retained r
            JOIN cohort_sizes cs
              ON r.cohort_week = cs.cohort_week
        """)

        export_table_to_parquet(conn, "daily_active_users", GOLD_DAILY_ACTIVE_USERS)
        export_table_to_parquet(conn, "daily_revenue_gross", GOLD_DAILY_REVENUE_GROSS)
        export_table_to_parquet(conn, "daily_revenue_net", GOLD_DAILY_REVENUE_NET)
        export_table_to_parquet(conn, "mrr_monthly", GOLD_MRR_MONTHLY)
        export_table_to_parquet(conn, "weekly_cohort_retention", GOLD_WEEKLY_COHORT_RETENTION)
        export_table_to_parquet(conn, "cac_by_channel", GOLD_CAC_BY_CHANNEL)
        export_table_to_parquet(conn, "ltv_per_user", GOLD_LTV_PER_USER)
        export_table_to_parquet(conn, "ltv_cac_ratio", GOLD_LTV_CAC_RATIO)

        row_counts = {
            "daily_active_users": _count_rows(conn, "daily_active_users"),
            "daily_revenue_gross": _count_rows(conn, "daily_revenue_gross"),
            "daily_revenue_net": _count_rows(conn, "daily_revenue_net"),
            "mrr_monthly": _count_rows(conn, "mrr_monthly"),
            "weekly_cohort_retention": _count_rows(conn, "weekly_cohort_retention"),
            "cac_by_channel": _count_rows(conn, "cac_by_channel"),
            "ltv_per_user": _count_rows(conn, "ltv_per_user"),
            "ltv_cac_ratio": _count_rows(conn, "ltv_cac_ratio"),
        }

    for table_name, row_count in row_counts.items():
        print(f"{table_name} rows: {row_count}")


if __name__ == "__main__":
    build_gold()