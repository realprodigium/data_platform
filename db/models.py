"""
SQLAlchemy models for the Gold warehouse tables.
"""

from datetime import datetime
from sqlalchemy import (
    Column, String, Float, Integer, DateTime, Text
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class MonthlyBalance(Base):
    __tablename__ = "gold_monthly_balance"

    mes = Column(String(7), primary_key=True)          # e.g. "2025-01"
    ingresos = Column(Float, nullable=False, default=0)
    egresos = Column(Float, nullable=False, default=0)
    neto = Column(Float, nullable=False, default=0)
    saldo_cierre = Column(Float, nullable=True)
    num_transacciones = Column(Integer, nullable=False, default=0)


class CategorySummary(Base):
    __tablename__ = "gold_category_summary"

    categoria = Column(String(100), primary_key=True)
    num_transacciones = Column(Integer, nullable=False, default=0)
    total_ingresos = Column(Float, nullable=False, default=0)
    total_egresos = Column(Float, nullable=False, default=0)


class KPIs(Base):
    __tablename__ = "gold_kpis"

    id = Column(Integer, primary_key=True, default=1)
    total_transacciones = Column(Integer, nullable=False)
    ingresos_totales = Column(Float, nullable=False)
    egresos_totales = Column(Float, nullable=False)
    balance_neto = Column(Float, nullable=False)
    saldo_maximo = Column(Float, nullable=True)
    fecha_inicio = Column(DateTime, nullable=True)
    fecha_fin = Column(DateTime, nullable=True)
    updated_at = Column(Text, nullable=True)


class PipelineRun(Base):
    """Audit log: one row per pipeline execution."""
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ran_at = Column(DateTime, default=datetime.utcnow)
    bronze_files = Column(Integer)
    silver_rows = Column(Integer)
    gold_transactions = Column(Integer)
    last_file = Column(String(200))
    status = Column(String(20), default="success")
    error_msg = Column(Text, nullable=True)
