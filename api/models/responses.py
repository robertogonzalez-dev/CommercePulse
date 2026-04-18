import datetime
from typing import Any, Optional

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
    top_channel: Optional[str] = None
    date_range_start: Optional[datetime.date] = None
    date_range_end: Optional[datetime.date] = None


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
    avg_order_value: Optional[float] = None
    unique_customers: int
    refund_rate_pct: Optional[float] = None
    cancellation_rate_pct: Optional[float] = None


# ── Products ──────────────────────────────────────────────────────────────────

class TopProductRow(BaseModel):
    product_id: str
    product_name: str
    category_l1: str
    category_l2: Optional[str] = None
    brand: Optional[str] = None
    sku: Optional[str] = None
    price_tier: str
    units_sold: int
    gross_revenue: float
    net_revenue: float
    total_contribution_margin: float
    realized_margin_pct: Optional[float] = None
    refund_rate_pct: Optional[float] = None
    current_stock_status: str


# ── Customers ─────────────────────────────────────────────────────────────────

class CustomerLTVRow(BaseModel):
    customer_id: str
    full_name: Optional[str] = None
    gender: Optional[str] = None
    age_band: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    acquisition_channel: Optional[str] = None
    customer_segment: Optional[str] = None
    rfm_segment: Optional[str] = None
    total_orders: int
    fulfilled_orders: int
    avg_order_value: float
    historical_clv: float
    predicted_clv_2yr: float
    days_since_last_order: Optional[int] = None
    recency_score: int
    frequency_score: int
    monetary_score: int
    rfm_total_score: int


# ── Channels ──────────────────────────────────────────────────────────────────

class ChannelPerformanceRow(BaseModel):
    channel_name: str
    channel_type: Optional[str] = None
    is_paid: bool
    total_orders: int
    unique_customers: int
    gross_revenue: float
    net_revenue: float
    avg_order_value: Optional[float] = None
    total_sessions: int
    unique_visitors: int
    session_conversions: int
    bounces: int
    avg_session_duration_secs: Optional[float] = None
    avg_pages_per_session: Optional[float] = None
    bounce_rate_pct: Optional[float] = None
    session_conversion_rate_pct: Optional[float] = None
    total_spend: float
    total_impressions: int
    total_clicks: int
    revenue_attributed: float
    avg_roas: Optional[float] = None
    avg_cpa: Optional[float] = None
    revenue_per_spend_dollar: Optional[float] = None


# ── Refunds ───────────────────────────────────────────────────────────────────

class RefundRow(BaseModel):
    year_month: str
    year: int
    refund_reason: Optional[str] = None
    channel_name: Optional[str] = None
    category_l1: Optional[str] = None
    refund_count: int
    orders_refunded: int
    total_refunded: float
    avg_refund_amount: Optional[float] = None
    refund_rate_pct: Optional[float] = None
    revenue_impact_pct: Optional[float] = None


# ── Inventory ─────────────────────────────────────────────────────────────────

class InventoryRiskRow(BaseModel):
    product_id: str
    product_name: str
    category_l1: Optional[str] = None
    brand: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_name: Optional[str] = None
    stock_status: str
    quantity_on_hand: int
    quantity_available: int
    reorder_level: int
    days_of_cover: Optional[float] = None
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
