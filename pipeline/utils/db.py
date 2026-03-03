from pathlib import Path
import duckdb


def get_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=database)


def export_table_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)

    conn.execute(
        f"COPY {table_name} TO ? (FORMAT PARQUET)",
        [str(output_path)],
    )