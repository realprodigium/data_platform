"""
Database session — SQLAlchemy engine + session factory.
Reads DB_URL from environment; defaults to SQLite for local dev.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Use SQLite by default (no server needed). Set DB_URL env to switch to PostgreSQL.
# Example PostgreSQL: postgresql+psycopg2://user:pass@localhost:5432/data_platform
DB_URL = os.getenv("DB_URL", "sqlite:///data/warehouse.db")

engine = create_engine(
    DB_URL,
    echo=False,
    connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    """FastAPI dependency: yields a DB session and closes it after use."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
