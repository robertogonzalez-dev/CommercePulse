import duckdb
from fastapi import APIRouter, Depends

from api.config import settings
from api.db.connection import get_db_conn
from api.models.responses import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse, summary="Service health check")
def health(conn: duckdb.DuckDBPyConnection = Depends(get_db_conn)) -> HealthResponse:
    row = conn.execute("SELECT version()").fetchone()
    version = row[0] if row else "unknown"
    return HealthResponse(
        status="ok",
        database=version,
        warehouse_path=str(settings.db_path),
    )
