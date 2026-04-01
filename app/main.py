import io
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, Depends, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import KPIs, CategorySummary, MonthlyBalance

SILVER_DIR = Path("data/lake/silver")

app = FastAPI(title="DataPlatform — Financial Intel")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="templates")


# ── Helpers ───────────────────────────────────────────────────────────────────

SEMESTER_MAP = {
    "S1_2025": [1, 2, 3, 4, 5, 6],
    "S2_2025": [7, 8, 9, 10, 11, 12],
    "all":     None,
}

QUARTER_FILES = {
    1: "transacciones_Q1_2025.parquet",
    2: "transacciones_Q2_2025.parquet",
    3: "transacciones_Q3_2025.parquet",
    4: "transacciones_Q4_2025.parquet",
}

QUARTER_MONTHS = {1: [1,2,3], 2: [4,5,6], 3: [7,8,9], 4: [10,11,12]}


def _load_silver(semester: str) -> pd.DataFrame:
    """Load Silver Parquet files and filter by semester."""
    frames = []
    for q, fname in QUARTER_FILES.items():
        fpath = SILVER_DIR / fname
        if fpath.exists():
            frames.append(pd.read_parquet(fpath))

    if not frames:
        raise FileNotFoundError("No Silver data found. Run the pipeline first.")

    df = pd.concat(frames, ignore_index=True)
    df["FECHA"] = pd.to_datetime(df["FECHA"], utc=True)

    months = SEMESTER_MAP.get(semester)
    if months is not None:
        df = df[df["FECHA"].dt.month.isin(months)]

    df = df.sort_values("FECHA")

    # Clean output columns
    keep = ["FECHA", "DESCRIPCIÓN", "SUCURSAL", "VALOR", "SALDO", "source_file"]
    df = df[[c for c in keep if c in df.columns]]
    df["FECHA"] = df["FECHA"].dt.strftime("%Y-%m-%d")

    return df


def _to_sql_dump(df: pd.DataFrame, semester: str) -> str:
    """Generate a SQL file with CREATE TABLE + INSERT statements."""
    period = semester if semester != "all" else "2025_completo"
    table = f"transacciones_{period}"

    lines: list[str] = [
        f"-- DataPlatform Export · Período: {period}",
        f"-- Generado: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"-- Total filas: {len(df)}",
        "",
        f"CREATE TABLE IF NOT EXISTS {table} (",
        "    id          SERIAL PRIMARY KEY,",
        "    fecha       DATE,",
        "    descripcion TEXT,",
        "    sucursal    TEXT,",
        "    valor       NUMERIC(20, 2),",
        "    saldo       NUMERIC(20, 2),",
        "    fuente      TEXT",
        ");",
        "",
        f"INSERT INTO {table} (fecha, descripcion, sucursal, valor, saldo, fuente) VALUES",
    ]

    rows = []
    for _, row in df.iterrows():
        def esc(v) -> str:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return "NULL"
            return "'" + str(v).replace("'", "''") + "'"

        fecha = esc(row.get("FECHA"))
        desc  = esc(row.get("DESCRIPCIÓN"))
        suc   = esc(row.get("SUCURSAL"))
        valor = str(row["VALOR"]) if pd.notna(row.get("VALOR")) else "NULL"
        saldo = str(row["SALDO"]) if pd.notna(row.get("SALDO")) else "NULL"
        src   = esc(row.get("source_file"))
        rows.append(f"  ({fecha}, {desc}, {suc}, {valor}, {saldo}, {src})")

    lines.append(",\n".join(rows) + ";")
    return "\n".join(lines)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def read_dashboard(request: Request, db: Session = Depends(get_db)):
    kpis       = db.query(KPIs).first()
    categories = db.query(CategorySummary).order_by(CategorySummary.total_egresos.desc()).all()
    monthly    = db.query(MonthlyBalance).order_by(MonthlyBalance.mes).all()

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"kpis": kpis, "categories": categories, "monthly": monthly},
    )


@app.get("/export", response_class=HTMLResponse)
async def export_page(request: Request, db: Session = Depends(get_db)):
    kpis = db.query(KPIs).first()
    return templates.TemplateResponse(
        request=request,
        name="export.html",
        context={"kpis": kpis},
    )


@app.get("/export/download")
async def export_download(
    semester: str = Query("all",  enum=["S1_2025", "S2_2025", "all"]),
    fmt:      str = Query("csv",  enum=["csv", "xlsx", "sql"]),
):
    df = _load_silver(semester)
    period = semester if semester != "all" else "2025_completo"
    fname_base = f"reporte_{period}"

    # ── CSV ─────────────────────────────────────────────────────────────────
    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False, encoding="utf-8")
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.csv"'},
        )

    # ── XLSX ─────────────────────────────────────────────────────────────────
    if fmt == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Transacciones")
        buf.seek(0)
        return Response(
            content=buf.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.xlsx"'},
        )

    # ── SQL ──────────────────────────────────────────────────────────────────
    if fmt == "sql":
        sql_content = _to_sql_dump(df, semester)
        return StreamingResponse(
            iter([sql_content]),
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.sql"'},
        )
