"""Marketing & channel performance page — ROAS, spend efficiency, and funnel."""

import plotly.graph_objects as go
import streamlit as st

import app.db as db
from app.components import charts, filters

st.set_page_config(page_title="Marketing — CommercePulse", page_icon="📣", layout="wide")

st.sidebar.title("📊 CommercePulse")
st.sidebar.caption("E-commerce Analytics Platform")
st.sidebar.divider()
st.sidebar.subheader("Filters")
channel = filters.render_channel_filter()

start_date, end_date = filters.render_date_filters()

st.title("Marketing & Channels")
st.caption("Spend efficiency, ROAS, session funnel, and channel revenue contribution.")

with st.spinner("Loading…"):
    channel_df = db.get_channel_performance()
    refund_df = db.get_refund_analysis(
        start_ym=start_date.strftime("%Y-%m") if start_date else None,
        end_ym=end_date.strftime("%Y-%m") if end_date else None,
        channel=channel,
    )

if channel_df.empty:
    st.warning("No channel data available.")
    st.stop()

filtered = channel_df if channel is None else channel_df[channel_df["channel_name"] == channel]

# ── KPIs ──────────────────────────────────────────────────────────────────────
paid = filtered[filtered["is_paid"]]
total_spend = float(paid["total_spend"].sum())
avg_roas = float(paid["avg_roas"].mean()) if paid["avg_roas"].notna().any() else 0.0
avg_cpa = float(paid["avg_cpa"].mean()) if paid["avg_cpa"].notna().any() else 0.0
total_sessions = int(filtered["total_sessions"].sum())
avg_cvr = float(filtered["session_conversion_rate_pct"].mean()) if filtered["session_conversion_rate_pct"].notna().any() else 0.0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Spend", f"${total_spend:,.0f}")
col2.metric("Avg ROAS", f"{avg_roas:.2f}x")
col3.metric("Avg CPA", f"${avg_cpa:,.2f}")
col4.metric("Total Sessions", f"{total_sessions:,}")
col5.metric("Avg CVR", f"{avg_cvr:.1f}%")

st.divider()

# ── Revenue vs Spend + ROAS ───────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Revenue by Channel")
    st.plotly_chart(charts.revenue_by_channel(filtered), use_container_width=True)

with col_right:
    st.subheader("ROAS by Paid Channel")
    if not paid.empty:
        st.plotly_chart(charts.roas_by_channel(paid), use_container_width=True)
    else:
        st.info("No paid channel data available.")

st.divider()

# ── Sessions funnel ───────────────────────────────────────────────────────────
st.subheader("Sessions & Conversion Funnel")
col1, col2 = st.columns(2)

with col1:
    fig = go.Figure(go.Bar(
        x=filtered["channel_name"],
        y=filtered["total_sessions"],
        name="Sessions",
        marker_color="#636efa",
        text=filtered["total_sessions"],
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=filtered["channel_name"],
        y=filtered["session_conversions"],
        name="Conversions",
        marker_color="#00cc96",
    ))
    fig.update_layout(
        template="plotly_white",
        barmode="overlay",
        xaxis_title=None,
        yaxis_title="Count",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = go.Figure(go.Bar(
        x=filtered["channel_name"],
        y=filtered["bounce_rate_pct"],
        name="Bounce Rate %",
        marker_color="#ef553b",
        text=filtered["bounce_rate_pct"].apply(
            lambda v: f"{v:.1f}%" if v is not None else ""
        ),
        textposition="outside",
    ))
    fig2.update_layout(
        template="plotly_white",
        xaxis_title=None,
        yaxis_title="%",
        yaxis_ticksuffix="%",
        margin=dict(l=0, r=0, t=30, b=0),
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Channel detail table ──────────────────────────────────────────────────────
st.subheader("Channel Performance Detail")
show = filtered[[
    "channel_name", "channel_type", "is_paid", "total_orders",
    "gross_revenue", "net_revenue", "avg_order_value",
    "total_sessions", "bounce_rate_pct", "session_conversion_rate_pct",
    "total_spend", "avg_roas", "revenue_per_spend_dollar",
]].copy()
for c in ["gross_revenue", "net_revenue", "avg_order_value", "total_spend"]:
    show[c] = show[c].apply(lambda v: f"${v:,.2f}" if v is not None else "—")
for c in ["bounce_rate_pct", "session_conversion_rate_pct"]:
    show[c] = show[c].apply(lambda v: f"{v:.1f}%" if v is not None else "—")
for c in ["avg_roas", "revenue_per_spend_dollar"]:
    show[c] = show[c].apply(lambda v: f"{v:.2f}x" if v is not None else "—")
show.columns = [
    "Channel", "Type", "Paid?", "Orders", "Gross Revenue", "Net Revenue",
    "AOV", "Sessions", "Bounce %", "CVR %", "Spend", "ROAS", "Rev/Spend $",
]
st.dataframe(show, use_container_width=True, hide_index=True)
