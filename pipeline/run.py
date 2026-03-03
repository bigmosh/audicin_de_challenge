from pipeline.bronze.ingest import load_bronze
from pipeline.silver.transform import build_silver
from pipeline.gold.metrics import build_gold


def main():
    print("Starting Data Pipeline")
    print("=" * 50)

    print("\n[1/3] Bronze Layer")
    load_bronze()
    print("Bronze completed.")

    print("\n[2/3] Silver Layer")
    build_silver()
    print("Silver completed.")

    print("\n[3/3] Gold Layer")
    build_gold()
    print("Gold completed.")

    print("\n" + "=" * 50)
    print("Pipeline finished successfully.")
    print("=" * 50)


if __name__ == "__main__":
    main()