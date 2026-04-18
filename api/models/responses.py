import datetime
from typing import Any

from pydantic import BaseModel

# ── Health ────────────────────────────────────────────────────────────────────


class HealthResponse(BaseModel):
    status: str
    database: str
    warehouse_path: str


# ── KPIs ──────────────────────────────────────────────────────────────────────


class KPISummary(BaseModel):
    total_orders: int
    fulfilled_orders: int
    cancelled_orders: int
    total_gross_revenue: float
    total_net_revenue: float
    avg_order_value: float
    total_discounts: float
    total_refunded: float
    avg_refund_rate_pct: float
    total_customers: int
    repeat_customers: int
    top_channel: str | None = None
    date_range_start: datetime.date | None = None
    date_range_end: datetime.date | None = None


# ── Sales Trend ───────────────────────────────────────────────────────────────


class SalesTrendRow(BaseModel):
    date: datetime.date
    year: int
    year_month: str
    year_quarter: str
    total_orders: int
    fulfilled_orders: int
    cancelled_orders: int
    gross_revenue: float
    net_revenue: float
    total_discounts: float
    total_refunded: float
    avg_order_value: float | None = None
    unique_customers: int
    refund_rate_pct: float | None = None
    cancellation_rate_pct: float | None = None


# ── Products ──────────────────────────────────────────────────────────────────


class TopProductRow(BaseModel):
    product_id: str
    product_name: str
    category_l1: str
    category_l2: str | None = None
    brand: str | None = None
    sku: str | None = None
    price_tier: str
    units_sold: int
    gross_revenue: float
    net_revenue: float
    total_contribution_margin: float
    realized_margin_pct: float | None = None
    refund_rate_pct: float | None = None
    current_stock_status: str


# ── Customers ─────────────────────────────────────────────────────────────────


class CustomerLTVRow(BaseModel):
    customer_id: str
    full_name: str | None = None
    gender: str | None = None
    age_band: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    acquisition_channel: str | None = None
    customer_segment: str | None = None
    rfm_segment: str | None = None
    total_orders: int
    fulfilled_orders: int
    avg_order_value: float
    historical_clv: float
    predicted_clv_2yr: float
    days_since_last_order: int | None = None
    recency_score: int
    frequency_score: int
    monetary_score: int
    rfm_total_score: int


# ── Channels ──────────────────────────────────────────────────────────────────


class ChannelPerformanceRow(BaseModel):
    channel_name: str
    channel_type: str | None = None
    is_paid: bool
    total_orders: int
    unique_customers: int
    gross_revenue: float
    net_revenue: float
    avg_order_value: float | None = None
    total_sessions: int
    unique_visitors: int
    session_conversions: int
    bounces: int
    avg_session_duration_secs: float | None = None
    avg_pages_per_session: float | None = None
    bounce_rate_pct: float | None = None
    session_conversion_rate_pct: float | None = None
    total_spend: float
    total_impressions: int
    total_clicks: int
    revenue_attributed: float
    avg_roas: float | None = None
    avg_cpa: float | None = None
    revenue_per_spend_dollar: float | None = None


# ── Refunds ───────────────────────────────────────────────────────────────────


class RefundRow(BaseModel):
    year_month: str
    year: int
    refund_reason: str | None = None
    channel_name: str | None = None
    category_l1: str | None = None
    refund_count: int
    orders_refunded: int
    total_refunded: float
    avg_refund_amount: float | None = None
    refund_rate_pct: float | None = None
    revenue_impact_pct: float | None = None


# ── Inventory ─────────────────────────────────────────────────────────────────


class InventoryRiskRow(BaseModel):
    product_id: str
    product_name: str
    category_l1: str | None = None
    brand: str | None = None
    warehouse_id: str | None = None
    warehouse_name: str | None = None
    stock_status: str
    quantity_on_hand: int
    quantity_available: int
    reorder_level: int
    days_of_cover: float | None = None
    risk_level: str
    at_risk_revenue: float
    units_sold_last_90d: int
    avg_daily_units: float
    inventory_cost_value: float


# ── Pagination wrapper ────────────────────────────────────────────────────────


class PagedResponse(BaseModel):
    data: list[Any]
    total: int
    limit: int
    offset: int
    has_more: bool
