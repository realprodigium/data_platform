"""
Bronze Layer — Raw ingestion
Reads XLSX files from data/raw/ and saves them as Parquet in data/lake/bronze/.
No transformations; data is stored as-is.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/lake/bronze")


def run() -> dict:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    xlsx_files = sorted(RAW_DIR.glob("*.xlsx"))
    if not xlsx_files:
        raise FileNotFoundError(f"No XLSX files found in {RAW_DIR}")

    for xlsx_path in xlsx_files:
        df = pd.read_excel(xlsx_path, sheet_name="Hoja2", engine="openpyxl")
        parquet_name = xlsx_path.stem + ".parquet"
        out_path = BRONZE_DIR / parquet_name
        df.to_parquet(out_path, index=False)
        results.append({
            "file": xlsx_path.name,
            "rows": len(df),
            "parquet": str(out_path),
        })
        print(f"  [bronze] {xlsx_path.name} → {parquet_name} ({len(df)} rows)")

    return {
        "layer": "bronze",
        "files_processed": len(results),
        "details": results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    run()
