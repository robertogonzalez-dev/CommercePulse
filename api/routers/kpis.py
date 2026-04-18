import datetime
from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from api.db.connection import get_db_conn
from api.models.responses import KPISummary
from api.services import kpi_service

router = APIRouter(prefix="/kpis", tags=["kpis"])


@router.get(
    "/summary",
    response_model=KPISummary,
    summary="Aggregated KPI summary across all metrics",
)
def kpi_summary(
    start_date: Optional[datetime.date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[datetime.date] = Query(None, description="End date (YYYY-MM-DD)"),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> KPISummary:
    return kpi_service.get_kpi_summary(conn, start_date, end_date)
