import duckdb

from api.models.common import ReportFilters, build_where


def get_inventory_risk(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    where, params = build_where([
        ("category_l1 = ?", filters.category),
        ("risk_level = ?", filters.region),  # risk_level exposed via region param for simplicity
    ])

    sql = f"""
        SELECT
            product_id,
            product_name,
            category_l1,
            brand,
            warehouse_id,
            warehouse_name,
            stock_status,
            quantity_on_hand,
            quantity_available,
            reorder_level,
            days_of_cover,
            risk_level,
            at_risk_revenue,
            units_sold_last_90d,
            avg_daily_units,
            inventory_cost_value
        FROM reporting.mart_inventory_risk
        {where}
        ORDER BY
            CASE risk_level
                WHEN 'critical' THEN 1
                WHEN 'high'     THEN 2
                WHEN 'medium'   THEN 3
                ELSE 4
            END,
            at_risk_revenue DESC
        LIMIT ? OFFSET ?
    """
    params.extend([filters.limit, filters.offset])

    df = conn.execute(sql, params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def count_inventory(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> int:
    where, params = build_where([
        ("category_l1 = ?", filters.category),
        ("risk_level = ?", filters.region),
    ])
    row = conn.execute(
        f"SELECT COUNT(*) FROM reporting.mart_inventory_risk {where}", params
    ).fetchone()
    return int(row[0]) if row else 0
