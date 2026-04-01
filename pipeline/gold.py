"""
Gold Layer — Aggregations with DuckDB
Reads all Silver Parquet files and produces three Gold tables:
  1. gold_monthly_balance   — monthly income, expenses, net, and closing balance
  2. gold_category_summary  — spending/income by transaction category
  3. gold_kpis              — single-row executive KPIs
All tables are saved as Parquet in data/lake/gold/.
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

SILVER_DIR = Path("data/lake/silver")
GOLD_DIR = Path("data/lake/gold")


def _classify_description(desc: str) -> str:
    """Map raw description to a clean category."""
    desc = str(desc).upper()
    if "NOMIN" in desc or "NOMINA" in desc:
        return "Nómina"
    if "PROVE" in desc or "PROVEE" in desc:
        return "Proveedores"
    if "IMPTO" in desc or "IMPUESTO" in desc or "4X1000" in desc:
        return "Impuestos"
    if "LEASING" in desc:
        return "Leasing"
    if "INTERBANC" in desc or "CONSORCIO" in desc:
        return "Pagos Interbanc"
    if "INTERESES" in desc or "AHORROS" in desc:
        return "Intereses Ahorros"
    if "CONSIGNACION" in desc:
        return "Consignaciones"
    if "PSE" in desc:
        return "Pagos PSE"
    if "SERVICIO" in desc:
        return "Servicios Bancarios"
    if "IVA" in desc:
        return "IVA"
    return "Otros"


def run() -> dict:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Load all silver files
    parquet_files = sorted(SILVER_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files in {SILVER_DIR}. Run silver first.")

    frames = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(frames, ignore_index=True)

    # Add category
    df["categoria"] = df["DESCRIPCIÓN"].apply(_classify_description)

    # Register in DuckDB
    con = duckdb.connect()
    con.register("transactions", df)

    # ── 1. Monthly balance ────────────────────────────────────────────────────
    monthly = con.execute("""
        SELECT
            strftime(FECHA, '%Y-%m')                          AS mes,
            SUM(CASE WHEN VALOR > 0 THEN VALOR ELSE 0 END)   AS ingresos,
            SUM(CASE WHEN VALOR < 0 THEN ABS(VALOR) ELSE 0 END) AS egresos,
            SUM(VALOR)                                        AS neto,
            MAX(SALDO)                                        AS saldo_cierre,
            COUNT(*)                                          AS num_transacciones
        FROM transactions
        GROUP BY mes
        ORDER BY mes
    """).df()
    monthly.to_parquet(GOLD_DIR / "gold_monthly_balance.parquet", index=False)
    print(f"  [gold] gold_monthly_balance: {len(monthly)} months")

    # ── 2. Category summary ───────────────────────────────────────────────────
    cat_summary = con.execute("""
        SELECT
            categoria,
            COUNT(*)                                          AS num_transacciones,
            SUM(CASE WHEN VALOR > 0 THEN VALOR ELSE 0 END)   AS total_ingresos,
            SUM(CASE WHEN VALOR < 0 THEN ABS(VALOR) ELSE 0 END) AS total_egresos
        FROM transactions
        GROUP BY categoria
        ORDER BY total_egresos DESC
    """).df()
    cat_summary.to_parquet(GOLD_DIR / "gold_category_summary.parquet", index=False)
    print(f"  [gold] gold_category_summary: {len(cat_summary)} categories")

    # ── 3. KPIs (single row) ──────────────────────────────────────────────────
    kpis = con.execute("""
        SELECT
            COUNT(*)                                               AS total_transacciones,
            SUM(CASE WHEN VALOR > 0 THEN VALOR ELSE 0 END)        AS ingresos_totales,
            SUM(CASE WHEN VALOR < 0 THEN ABS(VALOR) ELSE 0 END)   AS egresos_totales,
            SUM(VALOR)                                             AS balance_neto,
            MAX(SALDO)                                             AS saldo_maximo,
            MIN(FECHA)                                             AS fecha_inicio,
            MAX(FECHA)                                             AS fecha_fin
        FROM transactions
    """).df()
    kpis["updated_at"] = datetime.now(timezone.utc).isoformat()
    kpis.to_parquet(GOLD_DIR / "gold_kpis.parquet", index=False)
    print(f"  [gold] gold_kpis saved")

    con.close()

    return {
        "layer": "gold",
        "tables": ["gold_monthly_balance", "gold_category_summary", "gold_kpis"],
        "total_transactions": int(kpis["total_transacciones"].iloc[0]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    run()
