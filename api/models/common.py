import datetime
from typing import Optional

from fastapi import Query


class ReportFilters:
    """Shared query-parameter filter class injected via FastAPI Depends."""

    def __init__(
        self,
        start_date: Optional[datetime.date] = Query(
            None, description="Inclusive start date (YYYY-MM-DD)"
        ),
        end_date: Optional[datetime.date] = Query(
            None, description="Inclusive end date (YYYY-MM-DD)"
        ),
        region: Optional[str] = Query(
            None, description="State or country filter"
        ),
        channel: Optional[str] = Query(
            None, description="Channel name (e.g. organic_search)"
        ),
        category: Optional[str] = Query(
            None, description="Product category L1"
        ),
        limit: int = Query(100, ge=1, le=1000, description="Max rows"),
        offset: int = Query(0, ge=0, description="Pagination offset"),
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.region = region
        self.channel = channel
        self.category = category
        self.limit = limit
        self.offset = offset


def build_where(pairs: list[tuple[str, object]]) -> tuple[str, list]:
    """
    Build a SQL WHERE clause from a list of (condition, value) pairs.
    Pairs where value is None are skipped.

    Returns (where_clause_string, params_list).
    The where_clause_string is empty string if no conditions apply.
    """
    conditions: list[str] = []
    params: list = []
    for condition, value in pairs:
        if value is not None:
            conditions.append(condition)
            params.append(value)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where, params
