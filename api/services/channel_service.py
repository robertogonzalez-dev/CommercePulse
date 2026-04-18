import duckdb

from api.models.common import ReportFilters, build_where


def get_channel_performance(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    where, params = build_where([
        ("channel_name = ?", filters.channel),
    ])

    sql = f"""
        SELECT
            channel_name,
            channel_type,
            is_paid,
            total_orders,
            unique_customers,
            gross_revenue,
            net_revenue,
            avg_order_value,
            total_sessions,
            unique_visitors,
            session_conversions,
            bounces,
            avg_session_duration_secs,
            avg_pages_per_session,
            bounce_rate_pct,
            session_conversion_rate_pct,
            total_spend,
            total_impressions,
            total_clicks,
            revenue_attributed,
            avg_roas,
            avg_cpa,
            revenue_per_spend_dollar
        FROM reporting.mart_channel_performance
        {where}
        ORDER BY gross_revenue DESC
    """

    df = conn.execute(sql, params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")
