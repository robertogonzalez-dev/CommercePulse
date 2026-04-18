import threading
from collections.abc import Generator

import duckdb

from api.config import settings

_local = threading.local()


def _get_or_create() -> duckdb.DuckDBPyConnection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = duckdb.connect(str(settings.db_path), read_only=True)
    return _local.conn


def get_db_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """FastAPI dependency — yields a thread-local read-only DuckDB connection."""
    conn = _get_or_create()
    try:
        yield conn
    except duckdb.Error:
        _local.conn = None  # reset on error so next request gets a fresh connection
        raise
