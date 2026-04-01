import sys
import traceback
from pathlib import Path


def run_pipeline():
    print("=" * 55)
    print("  Data Platform — Medallion Pipeline")
    print("=" * 55)

    try:
        # Bronze
        print("\n[1/4] Bronze — Raw ingestion")
        from pipeline.bronze import run as bronze_run
        b = bronze_run()
        last_file = b["details"][-1]["file"] if b["details"] else ""
        bronze_files = b["files_processed"]
        print(f"  ✓ {bronze_files} files ingested")

        # Silver
        print("\n[2/4] Silver — Cleaning & validation")
        from pipeline.silver import run as silver_run
        s = silver_run()
        silver_rows = sum(d["rows_out"] for d in s["details"])
        print(f"  ✓ {silver_rows} clean rows")

        # Gold
        print("\n[3/4] Gold — Aggregations (DuckDB)")
        from pipeline.gold import run as gold_run
        g = gold_run()
        print(f"  ✓ {g['total_transactions']} transactions aggregated")

        # Load → Warehouse
        print("\n[4/4] Load — Warehouse (SQLAlchemy)")
        from pipeline.load import run as load_run
        load_run(
            bronze_files=bronze_files,
            silver_rows=silver_rows,
            last_file=last_file,
        )
        print("  ✓ Tables upserted")

        print("\n" + "=" * 55)
        print("  ✅ Pipeline completed successfully")
        print("=" * 55)

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()