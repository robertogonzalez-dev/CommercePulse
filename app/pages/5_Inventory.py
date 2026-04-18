"""Inventory risk page — stock-out risk, days of cover, and reorder alerts."""

import plotly.express as px
import streamlit as st

import app.db as db
from app.components import charts, filters, kpi_cards

st.set_page_config(page_title="Inventory — CommercePulse", page_icon="🏭", layout="wide")

st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
category = filters.render_category_filter()
risk_level = filters.render_risk_level_filter()

st.title("Inventory Risk")
st.caption("Stock-out exposure, days of cover, and revenue at risk by SKU and category.")

with st.spinner("Loading…"):
    inv_df = db.get_inventory_risk(category=category, risk_level=risk_level)

# ── KPIs ──────────────────────────────────────────────────────────────────────
kpi_cards.inventory_kpis(inv_df)

if inv_df.empty:
    st.stop()

st.divider()

# ── Risk distribution + At-risk revenue ───────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("SKUs by Risk Level")
    st.plotly_chart(charts.inventory_risk_bar(inv_df), use_container_width=True)

with col_right:
    st.subheader("Revenue at Risk (Critical + High)")
    st.plotly_chart(charts.at_risk_revenue_by_category(inv_df), use_container_width=True)

st.divider()

# ── Stock status breakdown ────────────────────────────────────────────────────
st.subheader("Stock Status Distribution")

stock_counts = (
    inv_df.groupby("stock_status", as_index=False).size().rename(columns={"size": "count"})
)
color_map = {
    "in_stock": "#2ca02c",
    "low_stock": "#ff7f0e",
    "out_of_stock": "#d62728",
}
fig = px.pie(
    stock_counts,
    names="stock_status",
    values="count",
    color="stock_status",
    color_discrete_map=color_map,
    hole=0.5,
    template="plotly_white",
)
fig.update_traces(textposition="outside", textinfo="percent+label")
fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
col1, col2 = st.columns([1, 2])
with col1:
    st.plotly_chart(fig, use_container_width=True)
with col2:
    st.subheader("Days of Cover Distribution")
    cover_df = inv_df.dropna(subset=["days_of_cover"])
    if not cover_df.empty:
        fig2 = px.histogram(
            cover_df,
            x="days_of_cover",
            nbins=20,
            template="plotly_white",
            color_discrete_sequence=["#1f77b4"],
            labels={"days_of_cover": "Days of Cover"},
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=30, b=0), yaxis_title="SKUs")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No days-of-cover data available.")

st.divider()

# ── Risk table ────────────────────────────────────────────────────────────────
st.subheader("Inventory Risk Detail")

RISK_COLORS = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "low": "🟢",
}

display = inv_df[
    [
        "risk_level",
        "product_name",
        "category_l1",
        "brand",
        "warehouse_name",
        "stock_status",
        "quantity_available",
        "days_of_cover",
        "at_risk_revenue",
        "units_sold_last_90d",
        "avg_daily_units",
    ]
].copy()
display["risk_level"] = display["risk_level"].map(
    lambda v: f"{RISK_COLORS.get(v, '')} {v}" if v else v
)
display["at_risk_revenue"] = display["at_risk_revenue"].map("${:,.2f}".format)
display["days_of_cover"] = display["days_of_cover"].apply(
    lambda v: f"{v:.1f}" if v is not None and str(v) != "nan" else "—"
)
display["avg_daily_units"] = display["avg_daily_units"].apply(
    lambda v: f"{v:.2f}" if v is not None else "0"
)
display.columns = [
    "Risk",
    "Product",
    "Category",
    "Brand",
    "Warehouse",
    "Status",
    "Qty Available",
    "Days Cover",
    "Revenue at Risk",
    "Units/90d",
    "Daily Velocity",
]
st.dataframe(display, use_container_width=True, hide_index=True)
