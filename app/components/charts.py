"""Plotly chart factory functions for CommercePulse dashboards."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Shared theme ──────────────────────────────────────────────────────────────

_TEMPLATE = "plotly_white"
_PALETTE = px.colors.qualitative.Safe
_REVENUE_COLOR = "#1f77b4"
_ORDERS_COLOR = "#ff7f0e"
_ACCENT = "#2ca02c"
_DANGER = "#d62728"


def _clean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Drop rows where col is null/NaN."""
    return df.dropna(subset=[col])


# ── Time-series ───────────────────────────────────────────────────────────────

def revenue_trend(df: pd.DataFrame, granularity: str = "daily") -> go.Figure:
    """Line chart of gross vs net revenue over time."""
    group_col = {"daily": "date", "monthly": "year_month", "quarterly": "year_quarter"}.get(
        granularity, "date"
    )
    if granularity != "daily":
        df = (
            df.groupby(group_col, as_index=False)
            .agg(gross_revenue=("gross_revenue", "sum"), net_revenue=("net_revenue", "sum"))
        )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[group_col], y=df["gross_revenue"],
        mode="lines", name="Gross Revenue",
        line=dict(color=_REVENUE_COLOR, width=2),
    ))
    fig.add_trace(go.Scatter(
        x=df[group_col], y=df["net_revenue"],
        mode="lines", name="Net Revenue",
        line=dict(color=_ACCENT, width=2, dash="dot"),
        fill="tonexty", fillcolor="rgba(44,160,44,0.08)",
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_title=None,
        yaxis_title="Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0),
        hovermode="x unified",
    )
    return fig


def orders_trend(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=df["date"], y=df["total_orders"],
        name="Orders",
        marker_color=_ORDERS_COLOR,
        opacity=0.8,
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_title=None,
        yaxis_title="Orders",
        margin=dict(l=0, r=0, t=30, b=0),
        bargap=0.2,
    )
    return fig


# ── Bar charts ────────────────────────────────────────────────────────────────

def revenue_by_channel(df: pd.DataFrame) -> go.Figure:
    df = df.sort_values("gross_revenue", ascending=True)
    fig = go.Figure(go.Bar(
        x=df["gross_revenue"],
        y=df["channel_name"],
        orientation="h",
        marker_color=_REVENUE_COLOR,
        text=df["gross_revenue"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        yaxis_title=None,
        margin=dict(l=0, r=80, t=30, b=0),
    )
    return fig


def top_products_bar(df: pd.DataFrame, n: int = 10) -> go.Figure:
    df = df.head(n).sort_values("net_revenue", ascending=True)
    fig = go.Figure(go.Bar(
        x=df["net_revenue"],
        y=df["product_name"],
        orientation="h",
        marker_color=_ACCENT,
        text=df["net_revenue"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        yaxis_title=None,
        margin=dict(l=0, r=80, t=30, b=0),
    )
    return fig


def category_revenue_bar(df: pd.DataFrame) -> go.Figure:
    cat_df = (
        df.groupby("category_l1", as_index=False)["net_revenue"].sum()
        .sort_values("net_revenue", ascending=False)
    )
    fig = px.bar(
        cat_df, x="category_l1", y="net_revenue",
        color="category_l1", color_discrete_sequence=_PALETTE,
        template=_TEMPLATE,
        labels={"category_l1": "Category", "net_revenue": "Net Revenue ($)"},
    )
    fig.update_layout(
        showlegend=False, xaxis_title=None,
        yaxis_tickprefix="$", yaxis_tickformat=",.0f",
        margin=dict(l=0, r=0, t=30, b=0),
    )
    return fig


def roas_by_channel(df: pd.DataFrame) -> go.Figure:
    paid = df[df["is_paid"] == True].dropna(subset=["avg_roas"]).sort_values("avg_roas", ascending=False)
    fig = px.bar(
        paid, x="channel_name", y="avg_roas",
        color="avg_roas", color_continuous_scale="Blues",
        template=_TEMPLATE,
        labels={"channel_name": "Channel", "avg_roas": "Avg ROAS"},
        text_auto=".2f",
    )
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title=None,
        margin=dict(l=0, r=0, t=30, b=0),
    )
    return fig


# ── Pie / donut ───────────────────────────────────────────────────────────────

def customer_segment_donut(df: pd.DataFrame) -> go.Figure:
    seg = df.groupby("customer_segment", as_index=False).size().rename(columns={"size": "count"})
    fig = px.pie(
        seg, names="customer_segment", values="count",
        hole=0.5,
        color_discrete_sequence=_PALETTE,
        template=_TEMPLATE,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
    return fig


def rfm_segment_donut(df: pd.DataFrame) -> go.Figure:
    seg = df[df["rfm_segment"].notna()].groupby("rfm_segment", as_index=False).size().rename(
        columns={"size": "count"}
    )
    fig = px.pie(
        seg, names="rfm_segment", values="count",
        hole=0.5,
        color_discrete_sequence=_PALETTE,
        template=_TEMPLATE,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
    return fig


# ── Scatter ───────────────────────────────────────────────────────────────────

def clv_scatter(df: pd.DataFrame) -> go.Figure:
    plot_df = df.dropna(subset=["historical_clv", "days_since_last_order"])
    fig = px.scatter(
        plot_df,
        x="days_since_last_order",
        y="historical_clv",
        color="rfm_segment",
        hover_name="full_name",
        hover_data={"total_orders": True, "avg_order_value": ":.2f"},
        color_discrete_sequence=_PALETTE,
        template=_TEMPLATE,
        labels={
            "days_since_last_order": "Days Since Last Order",
            "historical_clv": "Lifetime Value ($)",
        },
        opacity=0.7,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))
    return fig


def margin_vs_revenue_scatter(df: pd.DataFrame) -> go.Figure:
    plot_df = df.dropna(subset=["realized_margin_pct", "net_revenue"])
    fig = px.scatter(
        plot_df,
        x="net_revenue",
        y="realized_margin_pct",
        color="category_l1",
        size="units_sold",
        hover_name="product_name",
        color_discrete_sequence=_PALETTE,
        template=_TEMPLATE,
        labels={
            "net_revenue": "Net Revenue ($)",
            "realized_margin_pct": "Margin %",
            "category_l1": "Category",
        },
        opacity=0.75,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))
    return fig


# ── Inventory ─────────────────────────────────────────────────────────────────

def inventory_risk_bar(df: pd.DataFrame) -> go.Figure:
    risk_order = ["critical", "high", "medium", "low"]
    color_map = {
        "critical": _DANGER,
        "high": "#ff7f0e",
        "medium": "#ffdd57",
        "low": _ACCENT,
    }
    counts = (
        df.groupby("risk_level", as_index=False).size()
        .rename(columns={"size": "count"})
    )
    counts["risk_level"] = pd.Categorical(counts["risk_level"], categories=risk_order, ordered=True)
    counts = counts.sort_values("risk_level")

    fig = px.bar(
        counts, x="risk_level", y="count",
        color="risk_level",
        color_discrete_map=color_map,
        template=_TEMPLATE,
        labels={"risk_level": "Risk Level", "count": "SKU Count"},
        text_auto=True,
    )
    fig.update_layout(showlegend=False, xaxis_title=None, margin=dict(l=0, r=0, t=30, b=0))
    return fig


def at_risk_revenue_by_category(df: pd.DataFrame) -> go.Figure:
    cat = (
        df[df["risk_level"].isin(["critical", "high"])]
        .groupby("category_l1", as_index=False)["at_risk_revenue"].sum()
        .sort_values("at_risk_revenue", ascending=True)
    )
    fig = go.Figure(go.Bar(
        x=cat["at_risk_revenue"], y=cat["category_l1"],
        orientation="h",
        marker_color=_DANGER,
        text=cat["at_risk_revenue"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        yaxis_title=None,
        margin=dict(l=0, r=80, t=30, b=0),
    )
    return fig


# ── Refunds ───────────────────────────────────────────────────────────────────

def refunds_by_reason(df: pd.DataFrame) -> go.Figure:
    reason = (
        df.groupby("refund_reason", as_index=False)["total_refunded"].sum()
        .sort_values("total_refunded", ascending=True)
    )
    fig = go.Figure(go.Bar(
        x=reason["total_refunded"],
        y=reason["refund_reason"],
        orientation="h",
        marker_color=_DANGER,
        text=reason["total_refunded"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
    ))
    fig.update_layout(
        template=_TEMPLATE,
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        yaxis_title=None,
        margin=dict(l=0, r=80, t=30, b=0),
    )
    return fig
