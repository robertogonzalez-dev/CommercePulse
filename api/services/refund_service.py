import duckdb

from api.models.common import ReportFilters, build_where


def get_refund_analysis(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    # Convert date boundaries to YYYY-MM strings for year_month comparison
    start_ym = filters.start_date.strftime("%Y-%m") if filters.start_date else None
    end_ym = filters.end_date.strftime("%Y-%m") if filters.end_date else None

    where, params = build_where(
        [
            ("year_month >= ?", start_ym),
            ("year_month <= ?", end_ym),
            ("channel_name = ?", filters.channel),
            ("category_l1 = ?", filters.category),
        ]
    )

    sql = f"""
        SELECT
            year_month,
            year,
            refund_reason,
            channel_name,
            category_l1,
            refund_count,
            orders_refunded,
            total_refunded,
            avg_refund_amount,
            refund_rate_pct,
            revenue_impact_pct
        FROM reporting.mart_refund_analysis
        {where}
        ORDER BY year_month, total_refunded DESC
        LIMIT ? OFFSET ?
    """
    params.extend([filters.limit, filters.offset])

    df = conn.execute(sql, params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def count_refunds(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> int:
    start_ym = filters.start_date.strftime("%Y-%m") if filters.start_date else None
    end_ym = filters.end_date.strftime("%Y-%m") if filters.end_date else None
    where, params = build_where(
        [
            ("year_month >= ?", start_ym),
            ("year_month <= ?", end_ym),
            ("channel_name = ?", filters.channel),
            ("category_l1 = ?", filters.category),
        ]
    )
    row = conn.execute(
        f"SELECT COUNT(*) FROM reporting.mart_refund_analysis {where}", params
    ).fetchone()
    return int(row[0]) if row else 0
