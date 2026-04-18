"""
DuckDB warehouse connection manager.

Provides a singleton-style connection to the local DuckDB file,
initialises the bronze schema on first use, and exposes helpers
used by all loaders.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_DB_PATH_ENV = "COMMERCEPULSE_DB_PATH"
_DEFAULT_DB_PATH = "data/warehouse/commercepulse.duckdb"


def get_db_path() -> Path:
    raw = os.environ.get(_DB_PATH_ENV, _DEFAULT_DB_PATH)
    path = Path(raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a new DuckDB connection to the warehouse file."""
    db_path = get_db_path()
    conn = duckdb.connect(str(db_path), read_only=read_only)
    conn.execute("SET timezone = 'UTC'")
    return conn


@contextmanager
def managed_connection(read_only: bool = False):
    """Context manager that always closes the connection on exit."""
    conn = get_connection(read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def initialise_schema(ddl_path: str | Path | None = None) -> None:
    """
    Execute the bronze DDL file to create all tables if they don't exist.
    Safe to call on every pipeline run — uses CREATE TABLE IF NOT EXISTS.
    """
    if ddl_path is None:
        ddl_path = Path(__file__).parent / "schema" / "bronze_ddl.sql"

    ddl_path = Path(ddl_path)
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")

    sql = ddl_path.read_text(encoding="utf-8")
    with managed_connection() as conn:
        # DuckDB has no executescript — split on ";" and run each statement
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)

    logger.info("Bronze schema initialised from %s", ddl_path)


def table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    result = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return result is not None and result[0] > 0


def get_max_watermark(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    watermark_col: str,
) -> str | None:
    """Return the current high-water mark for incremental loads."""
    if not table_exists(conn, schema, table):
        return None
    try:
        result = conn.execute(
            f'SELECT MAX("{watermark_col}") FROM "{schema}"."{table}"'
        ).fetchone()
        return result[0] if result is not None else None
    except Exception:
        return None
