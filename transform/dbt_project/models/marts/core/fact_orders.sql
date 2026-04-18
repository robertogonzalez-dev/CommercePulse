-- Grain: one row per order.
-- Central fact table. Joins financial rollup from int_order_financials with
-- dimension surrogate keys. Cancelled orders are retained with zero revenue
-- so funnel and cancellation analysis remains accurate.

with orders as (
    select * from {{ ref('stg_orders') }}
),

financials as (
    select * from {{ ref('int_order_financials') }}
),

dim_customer as (
    select customer_key, customer_id from {{ ref('dim_customer') }}
),

dim_date as (
    select date_key, date_day from {{ ref('dim_date') }}
),

dim_channel as (
    select channel_key, channel_name from {{ ref('dim_channel') }}
),

dim_region as (
    select region_key, city, state, country from {{ ref('dim_region') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}           as order_key,

        -- Natural key
        o.order_id,

        -- Foreign keys to dimensions
        dc.customer_key,
        dd.date_key,
        dch.channel_key,
        dr.region_key,

        -- Order attributes
        o.ordered_at,
        o.order_date,
        o.order_status,
        o.channel,
        o.promotion_code,
        o.is_late_delivery,
        o.is_fulfilled,
        o.estimated_delivery_date,
        o.actual_delivery_date,

        -- Item metrics (from int_order_financials)
        f.item_count,
        f.total_quantity,

        -- Financial metrics
        f.gross_subtotal,
        f.total_discounts,
        f.net_subtotal,
        f.shipping_cost,
        f.gross_revenue,
        f.net_revenue,
        f.amount_paid,
        f.amount_refunded,
        f.refund_count,
        f.has_refund,

        -- Payment metadata
        f.primary_payment_method,
        f.payment_gateway

    from orders o
    left join financials f   on o.order_id = f.order_id
    left join dim_customer dc on o.customer_id = dc.customer_id
    left join dim_date dd     on o.order_date = dd.date_day
    left join dim_channel dch on o.channel = dch.channel_name
    left join dim_region dr
        on  o.shipping_city    = dr.city
        and o.shipping_state   = dr.state
        and o.shipping_country = dr.country
)

select * from final
