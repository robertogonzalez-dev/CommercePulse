"""Product performance page — revenue, margin, and category analysis."""

import streamlit as st

import app.db as db
from app.components import charts, filters

st.set_page_config(page_title="Products — CommercePulse", page_icon="📦", layout="wide")

st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
category = filters.render_category_filter()

# Price tier filter
tiers = ["All tiers", "budget", "mid_range", "premium", "luxury"]
tier_choice = st.sidebar.selectbox("Price Tier", tiers, key="filter_tier")
tier = None if tier_choice == "All tiers" else tier_choice

st.title("Products")
st.caption("Revenue contribution, margin analysis, and stock positioning by product and category.")

with st.spinner("Loading…"):
    product_df = db.get_product_performance(category=category)

if product_df.empty:
    st.warning("No product data for the selected filters.")
    st.stop()

if tier:
    product_df = product_df[product_df["price_tier"] == tier]

# ── KPIs ──────────────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Products", f"{len(product_df):,}")
col2.metric("Total Units Sold", f"{int(product_df['units_sold'].sum()):,}")
col3.metric(
    "Avg Realized Margin",
    f"{product_df['realized_margin_pct'].mean():.1f}%"
    if product_df["realized_margin_pct"].notna().any() else "—",
)
top_cat = product_df.groupby("category_l1")["net_revenue"].sum().idxmax()
col4.metric("Top Category", top_cat)

st.divider()

# ── Top products + Category breakdown ────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 Products by Net Revenue")
    st.plotly_chart(charts.top_products_bar(product_df, n=10), use_container_width=True)

with col_right:
    st.subheader("Revenue by Category")
    st.plotly_chart(charts.category_revenue_bar(product_df), use_container_width=True)

st.divider()

# ── Margin vs Revenue scatter ─────────────────────────────────────────────────
st.subheader("Margin vs Revenue")
st.caption("Bubble size = units sold. Colour = category.")
st.plotly_chart(charts.margin_vs_revenue_scatter(product_df), use_container_width=True)

st.divider()

# ── Product table ─────────────────────────────────────────────────────────────
st.subheader("Full Product Table")
display = product_df[[
    "product_name", "category_l1", "category_l2", "brand", "price_tier",
    "units_sold", "net_revenue", "total_contribution_margin",
    "realized_margin_pct", "refund_rate_pct", "current_stock_status",
]].copy()
display["net_revenue"] = display["net_revenue"].map("${:,.2f}".format)
display["total_contribution_margin"] = display["total_contribution_margin"].map("${:,.2f}".format)
display["realized_margin_pct"] = display["realized_margin_pct"].apply(
    lambda v: f"{v:.1f}%" if v is not None and str(v) != "nan" else "—"
)
display["refund_rate_pct"] = display["refund_rate_pct"].apply(
    lambda v: f"{v:.1f}%" if v is not None and str(v) != "nan" else "—"
)
display.columns = [
    "Product", "Category", "Sub-category", "Brand", "Tier",
    "Units", "Net Revenue", "Contribution Margin", "Margin %", "Refund Rate %", "Stock",
]
st.dataframe(display, use_container_width=True, hide_index=True)
