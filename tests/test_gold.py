import duckdb

def test_ltv_calculation(tmp_path):

    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE events_silver AS
        SELECT * FROM (
            VALUES
              ('u1', 'purchase', 100.0),
              ('u1', 'refund', 30.0)
        ) AS t(user_id, event_type, amount)
    """)

    result = conn.execute("""
        SELECT
          user_id,
          SUM(
            CASE
              WHEN event_type='purchase' THEN amount
              WHEN event_type='refund' THEN -amount
              ELSE 0
            END
          ) AS lifetime_value
        FROM events_silver
        GROUP BY user_id
    """).fetchone()

    assert result[1] == 70.0 # type: ignore