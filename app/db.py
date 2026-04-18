"""
Direct DuckDB query layer for the Streamlit application.

All functions return pandas DataFrames (preferred for plotting) or dicts.
Results are cached with @st.cache_data (TTL = 5 minutes) to avoid
re-querying DuckDB on every Streamlit rerun.
"""

import datetime

import duckdb
import pandas as pd
import streamlit as st

from app.config import settings


@st.cache_resource
def _conn() -> duckdb.DuckDBPyConnection:
    """Shared read-only DuckDB connection (singleton per Streamlit process)."""
    return duckdb.connect(str(settings.db_path), read_only=True)


def _q(sql: str, params: list | None = None) -> pd.DataFrame:
    return _conn().execute(sql, params or []).fetchdf()


# ── KPI Summary ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_kpi_summary(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict:
    conditions, params = [], []
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    df = _q(
        f"""
        SELECT
            COALESCE(SUM(total_orders), 0)                                      AS total_orders,
            COALESCE(SUM(fulfilled_orders), 0)                                  AS fulfilled_orders,
            COALESCE(SUM(cancelled_orders), 0)                                  AS cancelled_orders,
            COALESCE(SUM(gross_revenue), 0)                                     AS total_gross_revenue,
            COALESCE(SUM(net_revenue), 0)                                       AS total_net_revenue,
            COALESCE(SUM(gross_revenue) / NULLIF(SUM(total_orders), 0), 0)      AS avg_order_value,
            COALESCE(SUM(total_discounts), 0)                                   AS total_discounts,
            COALESCE(SUM(total_refunded), 0)                                    AS total_refunded,
            COALESCE(AVG(refund_rate_pct), 0)                                   AS avg_refund_rate_pct,
            COALESCE(AVG(cancellation_rate_pct), 0)                             AS avg_cancellation_rate_pct
        FROM reporting.mart_sales_summary {where}
        """,
        params,
    )

    cust_df = _q(
        "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_repeat_customer) AS repeat_c "
        "FROM reporting.mart_customer_ltv"
    )

    result: dict = df.to_dict("records")[0]
    result["total_customers"] = int(cust_df["total"].iloc[0])
    result["repeat_customers"] = int(cust_df["repeat_c"].iloc[0])
    return result


# ── Sales ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_sales_trend(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> pd.DataFrame:
    conditions, params = [], []
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return _q(
        f"SELECT * FROM reporting.mart_sales_summary {where} ORDER BY date",
        params,
    )


# ── Products ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_product_performance(category: str | None = None) -> pd.DataFrame:
    conditions, params = [], []
    if category:
        conditions.append("category_l1 = ?")
        params.append(category)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return _q(
        f"SELECT * FROM reporting.mart_product_performance {where} ORDER BY net_revenue DESC",
        params,
    )


# ── Customers ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_customer_ltv(
    channel: str | None = None,
    segment: str | None = None,
) -> pd.DataFrame:
    conditions, params = [], []
    if channel:
        conditions.append("acquisition_channel = ?")
        params.append(channel)
    if segment:
        conditions.append("rfm_segment = ?")
        params.append(segment)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return _q(
        f"SELECT * FROM reporting.mart_customer_ltv {where} ORDER BY historical_clv DESC",
        params,
    )


# ── Channels ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_channel_performance() -> pd.DataFrame:
    return _q(
        "SELECT * FROM reporting.mart_channel_performance ORDER BY gross_revenue DESC"
    )


# ── Inventory ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_inventory_risk(
    category: str | None = None,
    risk_level: str | None = None,
) -> pd.DataFrame:
    conditions, params = [], []
    if category:
        conditions.append("category_l1 = ?")
        params.append(category)
    if risk_level:
        conditions.append("risk_level = ?")
        params.append(risk_level)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return _q(
        f"""
        SELECT * FROM reporting.mart_inventory_risk {where}
        ORDER BY
            CASE risk_level
                WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4
            END, at_risk_revenue DESC
        """,
        params,
    )


# ── Refunds ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=settings.cache_ttl_seconds)
def get_refund_analysis(
    start_ym: str | None = None,
    end_ym: str | None = None,
    channel: str | None = None,
    category: str | None = None,
) -> pd.DataFrame:
    conditions, params = [], []
    if start_ym:
        conditions.append("year_month >= ?")
        params.append(start_ym)
    if end_ym:
        conditions.append("year_month <= ?")
        params.append(end_ym)
    if channel:
        conditions.append("channel_name = ?")
        params.append(channel)
    if category:
        conditions.append("category_l1 = ?")
        params.append(category)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return _q(
        f"SELECT * FROM reporting.mart_refund_analysis {where} ORDER BY year_month, total_refunded DESC",
        params,
    )


# ── Filter option lists ───────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_categories() -> list[str]:
    df = _q(
        "SELECT DISTINCT category_l1 FROM reporting.mart_product_performance "
        "WHERE category_l1 IS NOT NULL ORDER BY 1"
    )
    return df["category_l1"].tolist()


@st.cache_data(ttl=3600)
def get_channels() -> list[str]:
    df = _q("SELECT channel_name FROM reporting.mart_channel_performance ORDER BY channel_name")
    return df["channel_name"].tolist()


@st.cache_data(ttl=3600)
def get_acquisition_channels() -> list[str]:
    df = _q(
        "SELECT DISTINCT acquisition_channel FROM reporting.mart_customer_ltv "
        "WHERE acquisition_channel IS NOT NULL ORDER BY 1"
    )
    return df["acquisition_channel"].tolist()


@st.cache_data(ttl=3600)
def get_date_bounds() -> tuple[datetime.date, datetime.date]:
    row = _q("SELECT MIN(date), MAX(date) FROM reporting.mart_sales_summary").iloc[0]
    return row.iloc[0], row.iloc[1]
