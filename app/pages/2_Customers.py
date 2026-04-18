"""Customer analytics page — LTV, RFM, and segment analysis."""

import plotly.express as px
import streamlit as st

import app.db as db
from app.components import charts, filters, kpi_cards

st.set_page_config(page_title="Customers — CommercePulse", page_icon="👥", layout="wide")

st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
acq_channel = filters.render_acq_channel_filter()

st.title("Customers")
st.caption("Lifetime value, RFM scoring, and segment distribution.")

with st.spinner("Loading…"):
    customer_df = db.get_customer_ltv(channel=acq_channel)

if customer_df.empty:
    st.warning("No customer data for the selected filters.")
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────────
total = len(customer_df)
repeat = int(customer_df["is_repeat_customer"].sum()) if "is_repeat_customer" in customer_df.columns else 0
avg_clv = float(customer_df["historical_clv"].mean())
avg_aov = float(customer_df["avg_order_value"].mean())
top_seg = (
    customer_df["rfm_segment"].value_counts().idxmax()
    if "rfm_segment" in customer_df.columns and customer_df["rfm_segment"].notna().any()
    else "—"
)
kpi_cards.customer_kpis(total, repeat, avg_clv, avg_aov, top_seg)

st.divider()

# ── Segment + Acquisition ─────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Customer Segments")
    st.plotly_chart(charts.customer_segment_donut(customer_df), use_container_width=True)

with col2:
    st.subheader("RFM Segments")
    st.plotly_chart(charts.rfm_segment_donut(customer_df), use_container_width=True)

st.divider()

# ── Acquisition channel breakdown ─────────────────────────────────────────────
st.subheader("Customers by Acquisition Channel")

acq_df = (
    customer_df.groupby("acquisition_channel", as_index=False).agg(
        count=("customer_id", "count"),
        avg_clv=("historical_clv", "mean"),
    ).sort_values("count", ascending=False)
)
fig = px.bar(
    acq_df, x="acquisition_channel", y="count",
    color="avg_clv",
    color_continuous_scale="Blues",
    template="plotly_white",
    labels={"acquisition_channel": "Channel", "count": "Customers", "avg_clv": "Avg LTV ($)"},
    text_auto=True,
)
fig.update_layout(xaxis_title=None, margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── CLV Scatter ───────────────────────────────────────────────────────────────
st.subheader("LTV vs Recency")
st.caption("Each dot is a customer. Size indicates order count.")
plot_df = customer_df.dropna(subset=["days_since_last_order", "historical_clv"])
if not plot_df.empty:
    st.plotly_chart(charts.clv_scatter(plot_df), use_container_width=True)

st.divider()

# ── Top customers table ───────────────────────────────────────────────────────
st.subheader("Top Customers by Lifetime Value")
top = customer_df.head(20)[
    ["full_name", "acquisition_channel", "age_band", "city", "country",
     "customer_segment", "rfm_segment", "total_orders",
     "historical_clv", "predicted_clv_2yr", "days_since_last_order"]
].copy()
for col in ["historical_clv", "predicted_clv_2yr"]:
    top[col] = top[col].map("${:,.2f}".format)
st.dataframe(top, use_container_width=True, hide_index=True)
