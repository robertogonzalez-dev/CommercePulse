import datetime

import duckdb

from api.models.common import build_where
from api.models.responses import KPISummary


def get_kpi_summary(
    conn: duckdb.DuckDBPyConnection,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> KPISummary:
    where, params = build_where([
        ("date >= ?", start_date),
        ("date <= ?", end_date),
    ])

    sales_sql = f"""
        SELECT
            COALESCE(SUM(total_orders), 0)                                      AS total_orders,
            COALESCE(SUM(fulfilled_orders), 0)                                  AS fulfilled_orders,
            COALESCE(SUM(cancelled_orders), 0)                                  AS cancelled_orders,
            COALESCE(SUM(gross_revenue), 0)                                     AS total_gross_revenue,
            COALESCE(SUM(net_revenue), 0)                                       AS total_net_revenue,
            COALESCE(
                SUM(gross_revenue) / NULLIF(SUM(total_orders), 0), 0
            )                                                                   AS avg_order_value,
            COALESCE(SUM(total_discounts), 0)                                   AS total_discounts,
            COALESCE(SUM(total_refunded), 0)                                    AS total_refunded,
            COALESCE(AVG(refund_rate_pct), 0)                                   AS avg_refund_rate_pct
        FROM reporting.mart_sales_summary
        {where}
    """
    s = conn.execute(sales_sql, params).fetchone()

    cust = conn.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_repeat_customer) FROM reporting.mart_customer_ltv"
    ).fetchone()

    top_ch = conn.execute(
        "SELECT channel_name FROM reporting.mart_channel_performance ORDER BY gross_revenue DESC LIMIT 1"
    ).fetchone()

    return KPISummary(
        total_orders=int(s[0] or 0),
        fulfilled_orders=int(s[1] or 0),
        cancelled_orders=int(s[2] or 0),
        total_gross_revenue=float(s[3] or 0),
        total_net_revenue=float(s[4] or 0),
        avg_order_value=float(s[5] or 0),
        total_discounts=float(s[6] or 0),
        total_refunded=float(s[7] or 0),
        avg_refund_rate_pct=float(s[8] or 0),
        total_customers=int(cust[0] or 0),
        repeat_customers=int(cust[1] or 0),
        top_channel=top_ch[0] if top_ch else None,
        date_range_start=start_date,
        date_range_end=end_date,
    )
