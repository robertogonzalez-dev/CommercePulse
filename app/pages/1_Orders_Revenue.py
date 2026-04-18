"""Orders & Revenue deep-dive page."""

import streamlit as st

import app.db as db
from app.components import charts, filters, kpi_cards

st.set_page_config(page_title="Orders & Revenue — CommercePulse", page_icon="💰", layout="wide")

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
start_date, end_date = filters.render_date_filters()

# ── Page ──────────────────────────────────────────────────────────────────────
st.title("Orders & Revenue")
st.caption("Daily revenue performance, order volumes, and fulfilment metrics.")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading…"):
    kpis = db.get_kpi_summary(start_date, end_date)
    sales_df = db.get_sales_trend(start_date, end_date)

# ── KPIs ──────────────────────────────────────────────────────────────────────
st.subheader("Summary")
kpi_cards.revenue_kpis(kpis)
st.divider()
kpi_cards.orders_kpis(kpis)

st.divider()

if sales_df.empty:
    st.warning("No data for the selected date range.")
    st.stop()

# ── Revenue trend ─────────────────────────────────────────────────────────────
st.subheader("Revenue Trend")
granularity = st.radio(
    "View by",
    ["daily", "monthly", "quarterly"],
    horizontal=True,
    key="rev_granularity",
)
st.plotly_chart(charts.revenue_trend(sales_df, granularity), use_container_width=True)

st.divider()

# ── Orders trend + Refunds ────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Order Volume")
    st.plotly_chart(charts.orders_trend(sales_df), use_container_width=True)

with col2:
    st.subheader("Discount vs Refund Rate (%)")
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=sales_df["date"],
            y=sales_df["discount_rate_pct"],
            mode="lines",
            name="Discount Rate",
            line=dict(color="#ff7f0e", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=sales_df["date"],
            y=sales_df["refund_rate_pct"],
            mode="lines",
            name="Refund Rate",
            line=dict(color="#d62728", width=2),
        )
    )
    fig.update_layout(
        template="plotly_white",
        xaxis_title=None,
        yaxis_title="%",
        yaxis_ticksuffix="%",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Monthly breakdown table ───────────────────────────────────────────────────
st.subheader("Monthly Breakdown")
monthly = (
    sales_df.groupby("year_month", as_index=False)
    .agg(
        orders=("total_orders", "sum"),
        fulfilled=("fulfilled_orders", "sum"),
        gross=("gross_revenue", "sum"),
        net=("net_revenue", "sum"),
        aov=("avg_order_value", "mean"),
        refunds=("total_refunded", "sum"),
        refund_rate=("refund_rate_pct", "mean"),
    )
    .sort_values("year_month")
)
monthly.columns = [
    "Month",
    "Orders",
    "Fulfilled",
    "Gross Revenue",
    "Net Revenue",
    "Avg Order Value",
    "Refunds",
    "Refund Rate %",
]
for col in ["Gross Revenue", "Net Revenue", "Avg Order Value", "Refunds"]:
    monthly[col] = monthly[col].map("${:,.2f}".format)
monthly["Refund Rate %"] = monthly["Refund Rate %"].map("{:.1f}%".format)

st.dataframe(monthly, use_container_width=True, hide_index=True)
