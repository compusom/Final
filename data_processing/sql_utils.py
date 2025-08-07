"""Utilities for SQL operations used across the project.

This module centralises the creation of a SQLAlchemy engine, hashing of
files and basic helpers to reset the database schema.  The functions are
intentionally simple so that they can operate with SQLite during tests or
with any SQL database supported by SQLAlchemy in production.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import SQL_HOST, SQL_PORT, SQL_DB, SQL_USER, SQL_PASSWORD


def get_engine(url: Optional[str] = None, *, echo: bool = False) -> Engine:
    """Return a SQLAlchemy engine.

    Parameters
    ----------
    url:
        Optional SQLAlchemy URL.  If not provided, the function will build
        one from the configuration variables.  By default a SQLite database
        ``SQL_DB`` is used, which keeps the test environment light-weight.
    echo:
        Whether SQLAlchemy should log all statements.
    """

    if url is None:
        env_url = os.getenv("SQL_URI")
        if env_url:
            url = env_url
        else:
            # Default to SQLite for local development / tests
            if SQL_HOST in {"localhost", "sqlite"}:
                url = f"sqlite:///{SQL_DB}"
            else:
                url = (
                    f"postgresql://{SQL_USER}:{SQL_PASSWORD}@{SQL_HOST}:{SQL_PORT}/{SQL_DB}"
                )

    return create_engine(url, echo=echo, future=True)


def file_sha256(path: str | Path) -> str:
    """Calculate the SHA256 hash of a file."""

    sha = hashlib.sha256()
    with open(path, "rb") as f:  # type: ignore[arg-type]
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


SCHEMA_SQL = [
    # Dropping order considers foreign keys (children first)
    "DROP TABLE IF EXISTS metricas",
    "DROP TABLE IF EXISTS archivos_reporte",
    "DROP TABLE IF EXISTS archivos_url",
    "DROP TABLE IF EXISTS vistas_preview",
    "DROP TABLE IF EXISTS clientes",
    # Creation
    """
    CREATE TABLE clientes (
        id_cliente     INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre_cuenta  VARCHAR(255) NOT NULL UNIQUE,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE archivos_reporte (
        id_reporte     INTEGER PRIMARY KEY AUTOINCREMENT,
        id_cliente     INTEGER NOT NULL REFERENCES clientes(id_cliente),
        nombre_archivo VARCHAR(255) NOT NULL,
        hash_archivo   CHAR(64) NOT NULL UNIQUE,
        uploaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE metricas (
        id_metricas INTEGER PRIMARY KEY AUTOINCREMENT,
        id_reporte  INTEGER NOT NULL REFERENCES archivos_reporte(id_reporte),
        date        DATE NOT NULL,
        Campaign    VARCHAR(255),
        AdSet       VARCHAR(255),
        Anuncio     VARCHAR(255),
        publicos_in TEXT,
        spend       DECIMAL(12,2),
        purchases   INTEGER,
        value       DECIMAL(12,2),
        impr        INTEGER,
        clicks      INTEGER,
        reach       INTEGER,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE archivos_url (
        id_url      INTEGER PRIMARY KEY AUTOINCREMENT,
        id_cliente  INTEGER NOT NULL REFERENCES clientes(id_cliente),
        nombre_archivo VARCHAR(255) NOT NULL,
        hash_archivo  CHAR(64) NOT NULL UNIQUE,
        uploaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE vistas_preview (
        id_cliente           INTEGER NOT NULL REFERENCES clientes(id_cliente),
        nombre_ad            VARCHAR(255) NOT NULL,
        reach                INTEGER,
        ad_preview_link      TEXT NOT NULL,
        ad_creative_thumburl TEXT NOT NULL,
        updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id_cliente, nombre_ad)
    )
    """,
]


def reset_database(engine: Optional[Engine] = None) -> None:
    """Drop and recreate all tables defined in ``SCHEMA_SQL``."""

    engine = engine or get_engine()
    with engine.begin() as conn:
        for stmt in SCHEMA_SQL:
            conn.execute(text(stmt))


def truncate_all_tables(engine: Optional[Engine] = None) -> None:
    """Remove all data from tables without dropping them."""

    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM metricas"))
        conn.execute(text("DELETE FROM archivos_reporte"))
        conn.execute(text("DELETE FROM archivos_url"))
        conn.execute(text("DELETE FROM vistas_preview"))
        conn.execute(text("DELETE FROM clientes"))

