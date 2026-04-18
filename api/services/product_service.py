import duckdb

from api.models.common import ReportFilters, build_where


def get_top_products(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    where, params = build_where(
        [
            ("category_l1 = ?", filters.category),
        ]
    )

    sql = f"""
        SELECT
            product_id,
            product_name,
            category_l1,
            category_l2,
            brand,
            sku,
            price_tier,
            units_sold,
            gross_revenue,
            net_revenue,
            total_contribution_margin,
            realized_margin_pct,
            refund_rate_pct,
            current_stock_status
        FROM reporting.mart_product_performance
        {where}
        ORDER BY net_revenue DESC
        LIMIT ? OFFSET ?
    """
    params.extend([filters.limit, filters.offset])

    df = conn.execute(sql, params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def count_products(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> int:
    where, params = build_where([("category_l1 = ?", filters.category)])
    row = conn.execute(
        f"SELECT COUNT(*) FROM reporting.mart_product_performance {where}", params
    ).fetchone()
    return int(row[0]) if row else 0
