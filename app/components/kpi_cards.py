"""KPI card rendering helpers using st.metric."""

import streamlit as st


def _fmt_currency(value: float | None) -> str:
    if value is None:
        return "—"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.2f}"


def _fmt_number(value: float | None, decimals: int = 0) -> str:
    if value is None:
        return "—"
    return f"{value:,.{decimals}f}"


def _fmt_pct(value: float | None, decimals: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value:.{decimals}f}%"


def kpi_row(metrics: list[dict]) -> None:
    """
    Render a horizontal row of KPI cards.

    Each dict in metrics should have:
        label (str), value (str), delta (str | None), delta_color (str)
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics, strict=False):
        col.metric(
            label=m["label"],
            value=m["value"],
            delta=m.get("delta"),
            delta_color=m.get("delta_color", "normal"),
        )


def revenue_kpis(kpis: dict) -> None:
    kpi_row(
        [
            {"label": "Net Revenue", "value": _fmt_currency(kpis.get("total_net_revenue"))},
            {"label": "Gross Revenue", "value": _fmt_currency(kpis.get("total_gross_revenue"))},
            {"label": "Avg Order Value", "value": _fmt_currency(kpis.get("avg_order_value"))},
            {"label": "Total Discounts", "value": _fmt_currency(kpis.get("total_discounts"))},
            {"label": "Refunds", "value": _fmt_currency(kpis.get("total_refunded"))},
        ]
    )


def orders_kpis(kpis: dict) -> None:
    total = kpis.get("total_orders") or 1
    fulfilled = kpis.get("fulfilled_orders", 0)
    cancelled = kpis.get("cancelled_orders", 0)
    kpi_row(
        [
            {"label": "Total Orders", "value": _fmt_number(kpis.get("total_orders"))},
            {"label": "Fulfilled", "value": _fmt_number(fulfilled)},
            {
                "label": "Fulfillment Rate",
                "value": _fmt_pct(fulfilled / total * 100),
                "delta_color": "normal",
            },
            {"label": "Cancelled", "value": _fmt_number(cancelled)},
            {
                "label": "Refund Rate",
                "value": _fmt_pct(kpis.get("avg_refund_rate_pct")),
                "delta_color": "inverse",
            },
        ]
    )


def customer_kpis(
    total: int, repeat: int, avg_clv: float, avg_aov: float, top_segment: str
) -> None:
    repeat_rate = (repeat / total * 100) if total else 0
    kpi_row(
        [
            {"label": "Total Customers", "value": _fmt_number(total)},
            {"label": "Repeat Customers", "value": _fmt_number(repeat)},
            {"label": "Repeat Rate", "value": _fmt_pct(repeat_rate)},
            {"label": "Avg LTV", "value": _fmt_currency(avg_clv)},
            {"label": "Top RFM Segment", "value": top_segment or "—"},
        ]
    )


def inventory_kpis(df) -> None:
    if df.empty:
        st.info("No inventory data available.")
        return
    critical = int((df["risk_level"] == "critical").sum())
    out_of_stock = int((df["stock_status"] == "out_of_stock").sum())
    at_risk = float(df["at_risk_revenue"].sum())
    avg_cover = df["days_of_cover"].dropna().mean()
    kpi_row(
        [
            {"label": "Critical SKUs", "value": _fmt_number(critical), "delta_color": "inverse"},
            {"label": "Out of Stock", "value": _fmt_number(out_of_stock), "delta_color": "inverse"},
            {"label": "Revenue at Risk", "value": _fmt_currency(at_risk), "delta_color": "inverse"},
            {"label": "Avg Days of Cover", "value": _fmt_number(avg_cover, 1)},
            {"label": "Total SKUs", "value": _fmt_number(len(df))},
        ]
    )
