import io
import os
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, Depends, Request, Query, HTTPException, Response
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from db.session import get_db
from db.models import KPIs, CategorySummary, MonthlyBalance

# ── CONFIG ───────────────────────────────────────────────────────────────────
# If SECRET_TOKEN is set in ENV, we require it in URL (?token=...) or Cookie
SECRET_TOKEN = os.getenv("SECRET_TOKEN")

SILVER_DIR = Path("data/lake/silver")

app = FastAPI(title="DataPlatform — Financial Intel")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="templates")

# Maps period key → list of months to include (None = all)
PERIOD_MAP = {
    "Q1_2025": [1, 2, 3],
    "Q2_2025": [4, 5, 6],
    "Q3_2025": [7, 8, 9],
    "Q4_2025": [10, 11, 12],
    "all":     None,
}

QUARTER_FILES = [
    "transacciones_Q1_2025.parquet",
    "transacciones_Q2_2025.parquet",
    "transacciones_Q3_2025.parquet",
    "transacciones_Q4_2025.parquet",
]


def _load_silver(period: str) -> pd.DataFrame:
    """Load Silver Parquet files and filter by quarter period."""
    frames = []
    for fname in QUARTER_FILES:
        fpath = SILVER_DIR / fname
        if fpath.exists():
            frames.append(pd.read_parquet(fpath))

    if not frames:
        raise FileNotFoundError("No Silver data found. Run the pipeline first.")

    df = pd.concat(frames, ignore_index=True)
    df["FECHA"] = pd.to_datetime(df["FECHA"], utc=True)

    months = PERIOD_MAP.get(period)
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
    table = f"transacciones_{period.lower()}"

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


# ── Auth Helper ──────────────────────────────────────────────────────────────

def verify_access(request: Request):
    if not SECRET_TOKEN:
        return True
    
    # Check query param or cookie
    token = request.query_params.get("token") or request.cookies.get("access_token")
    if token == SECRET_TOKEN:
        return True
    
    raise HTTPException(
        status_code=403, 
        detail="Unauthorized. Access token required for this demo."
    )

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def read_dashboard(request: Request, db: Session = Depends(get_db), _ = Depends(verify_access)):
    kpis       = db.query(KPIs).first()
    categories = db.query(CategorySummary).order_by(CategorySummary.total_egresos.desc()).all()
    monthly    = db.query(MonthlyBalance).order_by(MonthlyBalance.mes).all()

    response = templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"kpis": kpis, "categories": categories, "monthly": monthly},
    )

    # Set cookie if token is in URL to 'remember' the session
    token = request.query_params.get("token")
    if token == SECRET_TOKEN:
        response.set_cookie(key="access_token", value=token, httponly=True)
    
    return response


@app.get("/export", response_class=HTMLResponse)
async def export_page(request: Request, db: Session = Depends(get_db), _ = Depends(verify_access)):
    kpis = db.query(KPIs).first()
    return templates.TemplateResponse(
        request=request,
        name="export.html",
        context={"kpis": kpis},
    )


@app.get("/export/download")
async def export_download(
    period: str = Query("all", enum=["Q1_2025", "Q2_2025", "Q3_2025", "Q4_2025", "all"]),
    fmt:    str = Query("csv", enum=["csv", "xlsx", "sql"]),
    _ = Depends(verify_access)
):
    df = _load_silver(period)
    label = period if period != "all" else "2025_completo"
    fname_base = f"reporte_{label}"

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
        sql_content = _to_sql_dump(df, period)
        return StreamingResponse(
            iter([sql_content]),
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname_base}.sql"'},
        )
