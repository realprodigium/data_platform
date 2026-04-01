# DataPlatform — Financial Intelligence E2E Pipeline

A production-ready Data Engineering MVP demonstrating a complete Medallion architecture (Bronze, Silver, Gold), automated processing, and a high-performance analytics dashboard.

## 🚀 Key Features

*   **Medallion Architecture**: Automated flow from raw XLSX (Bronze) → Cleaned Parquet (Silver) → Aggregated Data (Gold).
*   **Dual-Storage Strategy**: Data lake backed by Parquet files (PyArrow) and a relational warehouse via SQLAlchemy (SQLite/PostgreSQL).
*   **Nothing-Inspired UI**: A high-contrast, professional analytics dashboard focusing on clarity and data density.
*   **Advanced Export Engine**: Custom filters for quarterly data retrieval in CSV, XLSX, and multi-dialect SQL.
*   **Dockerized Deployment**: Ready for immediate cloud synchronization via Docker Compose.

---

## 🛠 Tech Stack

*   **Pipeline**: Python, pandas, openpyxl, DuckDB.
*   **ORM / Database**: SQLAlchemy, SQLite (default), PostgreSQL (supported).
*   **Frontend / API**: FastAPI, Jinja2, Nothing Design CSS System.
*   **Ops**: Docker, UV (dep management).

---

## 📦 Getting Started

### 1. Prerrequisites
Ensure you have `uv` installed (recommended) or `pip`:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Run the Data Pipeline
This processes the raw transaction data through all Medallion layers:
```bash
uv run python main.py
```

### 3. Launch the Dashboard
```bash
uv run uvicorn app.main:app --reload
```
Visit `http://localhost:8000/`

---

## 🔒 Security & Deployment

### Stealth Demo Access
This platform includes a lightweight "token-based" security layer for demo purposes. 

1. Set the `SECRET_TOKEN` environment variable:
   ```bash
   export SECRET_TOKEN="prodigium_demo"
   ```
2. Users must now visit the site with the token in the URL:
   `http://localhost:8000/?token=prodigium_demo`

This will set a secure `httponly` cookie, allowing subsequent navigation without the URL parameter.

### Deployment with Docker
```bash
docker-compose up --build -d
```
All data and generated files are persisted in the `/data` volume.

---

## 📂 Project Structure
*   `/app`: FastAPI logic and static assets (Nothing Design CSS).
*   `/pipeline`: Medallion logic (BRONZE → SILVER → GOLD → LOAD).
*   `/templates`: Jinga2 UI components.
*   `/data`: Local storage for Lake (Parquet) and Warehouse (SQLite).

---
**Developed by realprodigium** · 2025
