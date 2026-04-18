import datetime
from typing import Optional

import duckdb

from api.models.common import ReportFilters, build_where


def get_sales_trend(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    where, params = build_where([
        ("date >= ?", filters.start_date),
        ("date <= ?", filters.end_date),
    ])

    sql = f"""
        SELECT
            date,
            year,
            year_month,
            year_quarter,
            total_orders,
            fulfilled_orders,
            cancelled_orders,
            gross_revenue,
            net_revenue,
            total_discounts,
            total_refunded,
            avg_order_value,
            unique_customers,
            refund_rate_pct,
            cancellation_rate_pct
        FROM reporting.mart_sales_summary
        {where}
        ORDER BY date
        LIMIT ? OFFSET ?
    """
    params.extend([filters.limit, filters.offset])

    df = conn.execute(sql, params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def count_sales_trend(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> int:
    where, params = build_where([
        ("date >= ?", filters.start_date),
        ("date <= ?", filters.end_date),
    ])
    row = conn.execute(
        f"SELECT COUNT(*) FROM reporting.mart_sales_summary {where}", params
    ).fetchone()
    return int(row[0]) if row else 0
