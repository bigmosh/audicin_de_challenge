from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT/"data"
BRONZE_DIR = ROOT/"data/bronze"

BRONZE_EVENTS = BRONZE_DIR/"events_raw.parquet"
BRONZE_SUBSCRIPTIONS = BRONZE_DIR/"subscriptions_raw.parquet"
BRONZE_MARKETING = BRONZE_DIR/"marketing_spend_raw.parquet"

RAW_EVENTS = DATA_DIR/"events.ndjson"
RAW_SUBSCRIPTIONS = DATA_DIR/"subscriptions.json"
RAW_MARKETING = DATA_DIR/"marketing_spend.csv"



