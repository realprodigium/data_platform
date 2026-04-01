"""
Warehouse loader — upserts Gold Parquet data into PostgreSQL / SQLite via SQLAlchemy.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import inspect

from db.session import engine
from db.models import Base, MonthlyBalance, CategorySummary, KPIs, PipelineRun

GOLD_DIR = Path("data/lake/gold")


def _upsert_monthly(session, df: pd.DataFrame):
    for _, row in df.iterrows():
        obj = session.get(MonthlyBalance, row["mes"])
        if obj:
            obj.ingresos = float(row["ingresos"])
            obj.egresos = float(row["egresos"])
            obj.neto = float(row["neto"])
            obj.saldo_cierre = float(row["saldo_cierre"]) if pd.notna(row["saldo_cierre"]) else None
            obj.num_transacciones = int(row["num_transacciones"])
        else:
            session.add(MonthlyBalance(
                mes=row["mes"],
                ingresos=float(row["ingresos"]),
                egresos=float(row["egresos"]),
                neto=float(row["neto"]),
                saldo_cierre=float(row["saldo_cierre"]) if pd.notna(row["saldo_cierre"]) else None,
                num_transacciones=int(row["num_transacciones"]),
            ))


def _upsert_categories(session, df: pd.DataFrame):
    for _, row in df.iterrows():
        obj = session.get(CategorySummary, row["categoria"])
        if obj:
            obj.num_transacciones = int(row["num_transacciones"])
            obj.total_ingresos = float(row["total_ingresos"])
            obj.total_egresos = float(row["total_egresos"])
        else:
            session.add(CategorySummary(
                categoria=row["categoria"],
                num_transacciones=int(row["num_transacciones"]),
                total_ingresos=float(row["total_ingresos"]),
                total_egresos=float(row["total_egresos"]),
            ))


def _upsert_kpis(session, df: pd.DataFrame):
    row = df.iloc[0]
    obj = session.get(KPIs, 1)
    data = dict(
        id=1,
        total_transacciones=int(row["total_transacciones"]),
        ingresos_totales=float(row["ingresos_totales"]),
        egresos_totales=float(row["egresos_totales"]),
        balance_neto=float(row["balance_neto"]),
        saldo_maximo=float(row["saldo_maximo"]) if pd.notna(row["saldo_maximo"]) else None,
        fecha_inicio=pd.to_datetime(row["fecha_inicio"]) if pd.notna(row["fecha_inicio"]) else None,
        fecha_fin=pd.to_datetime(row["fecha_fin"]) if pd.notna(row["fecha_fin"]) else None,
        updated_at=str(row["updated_at"]),
    )
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        session.add(KPIs(**data))


def run(bronze_files: int = 0, silver_rows: int = 0, last_file: str = "") -> dict:
    Base.metadata.create_all(engine)

    from sqlalchemy.orm import Session

    with Session(engine) as session:
        # Monthly balance
        df_monthly = pd.read_parquet(GOLD_DIR / "gold_monthly_balance.parquet")
        _upsert_monthly(session, df_monthly)
        print(f"  [load] monthly_balance: {len(df_monthly)} rows upserted")

        # Category summary
        df_cat = pd.read_parquet(GOLD_DIR / "gold_category_summary.parquet")
        _upsert_categories(session, df_cat)
        print(f"  [load] category_summary: {len(df_cat)} rows upserted")

        # KPIs
        df_kpis = pd.read_parquet(GOLD_DIR / "gold_kpis.parquet")
        _upsert_kpis(session, df_kpis)
        print(f"  [load] kpis: 1 row upserted")

        # Pipeline run audit log
        total_tx = int(df_kpis.iloc[0]["total_transacciones"])
        session.add(PipelineRun(
            ran_at=datetime.utcnow(),
            bronze_files=bronze_files,
            silver_rows=silver_rows,
            gold_transactions=total_tx,
            last_file=last_file,
            status="success",
        ))

        session.commit()

    return {
        "layer": "warehouse",
        "tables_loaded": ["gold_monthly_balance", "gold_category_summary", "gold_kpis"],
        "pipeline_run_logged": True,
    }
