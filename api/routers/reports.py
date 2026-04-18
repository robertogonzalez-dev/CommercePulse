import duckdb
from fastapi import APIRouter, Depends

from api.db.connection import get_db_conn
from api.models.common import ReportFilters
from api.models.responses import (
    ChannelPerformanceRow,
    CustomerLTVRow,
    InventoryRiskRow,
    PagedResponse,
    RefundRow,
    SalesTrendRow,
    TopProductRow,
)
from api.services import (
    channel_service,
    customer_service,
    inventory_service,
    product_service,
    refund_service,
    sales_service,
)

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get(
    "/sales-trend",
    response_model=PagedResponse,
    summary="Daily sales trend with revenue, orders, and rates",
)
def sales_trend(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> PagedResponse:
    data = sales_service.get_sales_trend(conn, filters)
    total = sales_service.count_sales_trend(conn, filters)
    return PagedResponse(
        data=data,
        total=total,
        limit=filters.limit,
        offset=filters.offset,
        has_more=(filters.offset + filters.limit) < total,
    )


@router.get(
    "/top-products",
    response_model=PagedResponse,
    summary="Product performance ranked by net revenue",
)
def top_products(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> PagedResponse:
    data = product_service.get_top_products(conn, filters)
    total = product_service.count_products(conn, filters)
    return PagedResponse(
        data=data,
        total=total,
        limit=filters.limit,
        offset=filters.offset,
        has_more=(filters.offset + filters.limit) < total,
    )


@router.get(
    "/customer-ltv",
    response_model=PagedResponse,
    summary="Customer lifetime value with RFM scores and segments",
)
def customer_ltv(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> PagedResponse:
    data = customer_service.get_customer_ltv(conn, filters)
    total = customer_service.count_customers(conn, filters)
    return PagedResponse(
        data=data,
        total=total,
        limit=filters.limit,
        offset=filters.offset,
        has_more=(filters.offset + filters.limit) < total,
    )


@router.get(
    "/channel-performance",
    response_model=list[ChannelPerformanceRow],
    summary="Unified channel view: traffic, revenue, spend, and ROAS",
)
def channel_performance(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> list[dict]:
    return channel_service.get_channel_performance(conn, filters)


@router.get(
    "/refunds",
    response_model=PagedResponse,
    summary="Refund analysis by reason, channel, and category",
)
def refunds(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> PagedResponse:
    data = refund_service.get_refund_analysis(conn, filters)
    total = refund_service.count_refunds(conn, filters)
    return PagedResponse(
        data=data,
        total=total,
        limit=filters.limit,
        offset=filters.offset,
        has_more=(filters.offset + filters.limit) < total,
    )


@router.get(
    "/inventory-risk",
    response_model=PagedResponse,
    summary="Inventory risk matrix with days-of-cover and revenue exposure",
)
def inventory_risk(
    filters: ReportFilters = Depends(),
    conn: duckdb.DuckDBPyConnection = Depends(get_db_conn),
) -> PagedResponse:
    data = inventory_service.get_inventory_risk(conn, filters)
    total = inventory_service.count_inventory(conn, filters)
    return PagedResponse(
        data=data,
        total=total,
        limit=filters.limit,
        offset=filters.offset,
        has_more=(filters.offset + filters.limit) < total,
    )
