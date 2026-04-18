-- Daily and period-level sales aggregation.
-- Primary KPI surface: orders, revenue, AOV, discount rate, refund rate.

with orders as (
    select
        fo.order_key,
        fo.order_id,
        fo.order_date,
        fo.order_status,
        fo.is_fulfilled,
        fo.gross_revenue,
        fo.net_revenue,
        fo.gross_subtotal,
        fo.total_discounts,
        fo.shipping_cost,
        fo.amount_refunded,
        fo.item_count,
        fo.total_quantity,
        fo.has_refund,
        fo.customer_key,
        dd.year,
        dd.quarter_number,
        dd.year_quarter,
        dd.month_number,
        dd.month_name,
        dd.year_month,
        dd.week_of_year,
        dd.is_weekend
    from {{ ref('fact_orders') }} fo
    join {{ ref('dim_date') }} dd on fo.date_key = dd.date_key
),

daily as (
    select
        order_date                                                          as date,
        year,
        quarter_number,
        year_quarter,
        month_number,
        month_name,
        year_month,
        week_of_year,
        is_weekend,

        -- Volume
        count(distinct order_id)                                            as total_orders,
        count(distinct case when is_fulfilled then order_id end)            as fulfilled_orders,
        count(distinct case when order_status = 'cancelled' then order_id end)
                                                                            as cancelled_orders,
        count(distinct customer_key)                                        as unique_customers,
        sum(item_count)                                                     as total_items_sold,
        sum(total_quantity)                                                 as total_units_sold,

        -- Revenue
        sum(case when is_fulfilled then gross_revenue else 0 end)           as gross_revenue,
        sum(case when is_fulfilled then net_revenue else 0 end)             as net_revenue,
        sum(case when is_fulfilled then total_discounts else 0 end)         as total_discounts,
        sum(case when is_fulfilled then shipping_cost else 0 end)           as shipping_revenue,
        sum(case when is_fulfilled then amount_refunded else 0 end)         as total_refunded,

        -- Averages
        avg(case when is_fulfilled then gross_revenue end)                  as avg_order_value,
        avg(case when is_fulfilled then net_revenue end)                    as avg_net_order_value,

        -- Rate metrics
        {{ safe_divide(
            'count(distinct case when order_status = \'cancelled\' then order_id end)',
            'count(distinct order_id)'
        ) }} * 100                                                          as cancellation_rate_pct,

        {{ safe_divide(
            'sum(case when has_refund then 1 else 0 end)',
            'count(distinct case when is_fulfilled then order_id end)'
        ) }} * 100                                                          as refund_rate_pct,

        {{ safe_divide(
            'sum(case when is_fulfilled then total_discounts else 0 end)',
            'sum(case when is_fulfilled then gross_subtotal else 0 end)'
        ) }} * 100                                                          as discount_rate_pct

    from orders
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from daily
