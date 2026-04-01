"""
Silver Layer — Cleaning & validation
Reads Bronze Parquet files, applies cleaning rules, and writes Silver Parquet.
Rules:
  - Drop rows where DESCRIPCIÓN == 'FIN ESTADO DE CUENTA' or FECHA is null
  - Cast FECHA to datetime
  - Cast VALOR and SALDO to float (numeric)
  - Strip whitespace from string columns
  - Drop fully empty rows
  - Add source_file and ingestion_ts columns
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

BRONZE_DIR = Path("data/lake/bronze")
SILVER_DIR = Path("data/lake/silver")


def _clean_df(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    # Drop sentinel / footer rows
    df = df[df["DESCRIPCIÓN"].notna()]
    df = df[df["DESCRIPCIÓN"].str.strip() != "FIN ESTADO DE CUENTA"]

    # Drop rows without a date
    df = df[df["FECHA"].notna()]

    # Cast types
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df = df[df["FECHA"].notna()]  # drop rows that couldn't be parsed

    df["VALOR"] = pd.to_numeric(df["VALOR"], errors="coerce")
    df["SALDO"] = pd.to_numeric(df["SALDO"], errors="coerce")

    # Strip whitespace from strings
    for col in ["DESCRIPCIÓN", "SUCURSAL", "DCTO."]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", "")

    # Drop useless columns
    drop_cols = [c for c in ["Column1", "_1"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Deduplicate
    df = df.drop_duplicates()

    # Add metadata
    df["source_file"] = source_file
    df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    df = df.reset_index(drop=True)
    return df


def run() -> dict:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    parquet_files = sorted(BRONZE_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {BRONZE_DIR}. Run bronze first.")

    for pq_path in parquet_files:
        df_raw = pd.read_parquet(pq_path)
        df_clean = _clean_df(df_raw, source_file=pq_path.stem)

        out_path = SILVER_DIR / pq_path.name
        df_clean.to_parquet(out_path, index=False)
        results.append({
            "file": pq_path.name,
            "rows_in": len(df_raw),
            "rows_out": len(df_clean),
            "dropped": len(df_raw) - len(df_clean),
        })
        print(f"  [silver] {pq_path.name}: {len(df_raw)} → {len(df_clean)} rows")

    return {
        "layer": "silver",
        "files_processed": len(results),
        "details": results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    run()
