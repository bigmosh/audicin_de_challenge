# Audicin Data Engineer Take-Home

This project implements a runnable data pipeline using a medallion
architecture:

Raw -> Bronze -> Silver -> Gold

The pipeline processes analytics data and produces the
required business metrics.

------------------------------------------------------------------------

## How to Run

Install dependencies:

    pip install -r requirements.txt

Run the full pipeline:

    python -m pipeline.run

Run tests:

    python -m pytest

Gold outputs will be written to:

    data/gold/
    The pipeline also prints information to the terminal during execution.

------------------------------------------------------------------------

## Architecture Overview

### Bronze

-   Loads raw files (NDJSON, JSON, CSV)
-   Parses NDJSON safely (skips corrupted rows)
-   Preserves raw data without applying business logic
-   Exports raw tables to Parquet

### Silver

-   Enforces schema using safe casting (`try_cast`)
-   Normalizes fields (case, trimming)
-   Deduplicates events using window functions
-   Separates invalid records 

### Gold

Produces the required business metrics:

-   `daily_active_users`
-   `daily_revenue_gross`
-   `daily_revenue_net`
-   `mrr_monthly`
-   `weekly_cohort_retention`
-   `cac_by_channel`
-   `ltv_per_user`
-   `ltv_cac_ratio`

All gold tables are exported to Parquet.

------------------------------------------------------------------------

## Design Decisions

- The pipeline runs as a full refresh to keep things simple and easy to rerun.
- Parquet is used for intermediate and final outputs.
- Revenue calculations account for refunds.
- try_cast is used to avoid crashes from bad data.
- Invalid records are separated instead of being silently dropped.
- Weekly retention was chosen to keep the analysis clear and readable.
- No indexes were added since DuckDB is optimized for analytical workloads.


------------------------------------------------------------------------


## Stack
- Python
- DuckDB
- Parquet
- Sql

------------------------------------------------------------------------


## Testing
Run:

```bash
python -m pytest

