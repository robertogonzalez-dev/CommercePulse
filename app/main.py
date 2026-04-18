"""
CommercePulse — Executive Overview (home page)

Run:
    streamlit run app/main.py
    # or from project root:
    make app
"""

import streamlit as st

import app.db as db
from app.components import charts, filters, kpi_cards

st.set_page_config(
    page_title="CommercePulse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
start_date, end_date = filters.render_date_filters()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("Executive Overview")
st.caption("Top-level KPIs across all channels, products, and customers.")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading data…"):
    kpis = db.get_kpi_summary(start_date, end_date)
    sales_df = db.get_sales_trend(start_date, end_date)
    channel_df = db.get_channel_performance()
    product_df = db.get_product_performance()
    customer_df = db.get_customer_ltv()

# ── Row 1: Revenue KPIs ───────────────────────────────────────────────────────
st.subheader("Revenue")
kpi_cards.revenue_kpis(kpis)

st.divider()

# ── Row 2: Orders KPIs ────────────────────────────────────────────────────────
st.subheader("Orders & Customers")
kpi_cards.orders_kpis(kpis)

st.divider()

# ── Row 3: Revenue Trend ──────────────────────────────────────────────────────
st.subheader("Revenue Trend")
granularity = st.radio(
    "Granularity",
    options=["daily", "monthly", "quarterly"],
    horizontal=True,
    label_visibility="collapsed",
    key="overview_granularity",
)
if not sales_df.empty:
    st.plotly_chart(
        charts.revenue_trend(sales_df, granularity),
        use_container_width=True,
    )
else:
    st.info("No sales data for the selected date range.")

st.divider()

# ── Row 4: Channel + Customer Segment ────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Revenue by Channel")
    if not channel_df.empty:
        st.plotly_chart(charts.revenue_by_channel(channel_df), use_container_width=True)
    else:
        st.info("No channel data available.")

with col_right:
    st.subheader("Customer Segments")
    if not customer_df.empty:
        st.plotly_chart(charts.customer_segment_donut(customer_df), use_container_width=True)
    else:
        st.info("No customer data available.")

st.divider()

# ── Row 5: Top 10 Products ────────────────────────────────────────────────────
st.subheader("Top 10 Products by Net Revenue")
if not product_df.empty:
    st.plotly_chart(charts.top_products_bar(product_df, n=10), use_container_width=True)

    with st.expander("View full product table"):
        show_cols = [
            "product_name",
            "category_l1",
            "brand",
            "price_tier",
            "units_sold",
            "net_revenue",
            "realized_margin_pct",
            "current_stock_status",
        ]
        st.dataframe(
            product_df[show_cols].rename(
                columns={
                    "product_name": "Product",
                    "category_l1": "Category",
                    "brand": "Brand",
                    "price_tier": "Tier",
                    "units_sold": "Units",
                    "net_revenue": "Net Revenue",
                    "realized_margin_pct": "Margin %",
                    "current_stock_status": "Stock",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("No product data available.")
