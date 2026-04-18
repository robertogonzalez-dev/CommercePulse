import duckdb

from api.models.common import ReportFilters, build_where


def get_customer_ltv(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> list[dict]:
    # region maps to state or country
    region_cond = "(state = ? OR country = ?)" if filters.region else None
    region_params = [filters.region, filters.region] if filters.region else []

    pairs = [
        ("acquisition_channel = ?", filters.channel),
        ("category_l1 = ?", filters.category),  # unused for customers — ignored below
    ]
    # build standard conditions
    std_where, std_params = build_where([
        ("acquisition_channel = ?", filters.channel),
    ])

    # combine region condition
    conditions = []
    all_params: list = []
    if region_cond:
        conditions.append(region_cond)
        all_params.extend(region_params)
    if std_where:
        conditions.append(std_where.replace("WHERE ", ""))
        all_params.extend(std_params)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    sql = f"""
        SELECT
            customer_id,
            full_name,
            gender,
            age_band,
            city,
            state,
            country,
            acquisition_channel,
            customer_segment,
            rfm_segment,
            total_orders,
            fulfilled_orders,
            avg_order_value,
            historical_clv,
            predicted_clv_2yr,
            days_since_last_order,
            recency_score,
            frequency_score,
            monetary_score,
            rfm_total_score
        FROM reporting.mart_customer_ltv
        {where}
        ORDER BY historical_clv DESC
        LIMIT ? OFFSET ?
    """
    all_params.extend([filters.limit, filters.offset])

    df = conn.execute(sql, all_params).fetchdf()
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def count_customers(
    conn: duckdb.DuckDBPyConnection,
    filters: ReportFilters,
) -> int:
    conditions = []
    all_params: list = []
    if filters.region:
        conditions.append("(state = ? OR country = ?)")
        all_params.extend([filters.region, filters.region])
    if filters.channel:
        conditions.append("acquisition_channel = ?")
        all_params.append(filters.channel)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    row = conn.execute(
        f"SELECT COUNT(*) FROM reporting.mart_customer_ltv {where}", all_params
    ).fetchone()
    return int(row[0]) if row else 0
