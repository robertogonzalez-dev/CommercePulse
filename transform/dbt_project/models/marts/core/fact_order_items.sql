-- Grain: one row per order line item.
-- The most granular transactional fact. Enables product-level revenue,
-- discount, and margin analysis across any dimension.

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_date, customer_id, channel, order_status
    from {{ ref('stg_orders') }}
),

dim_product as (
    select product_key, product_id, cost_price from {{ ref('dim_product') }}
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

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id']) }}     as order_item_key,

        -- Natural keys
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        -- Foreign keys to dimensions
        dp.product_key,
        dc.customer_key,
        dd.date_key,
        dch.channel_key,

        -- Order context
        o.order_date,
        o.order_status,
        o.channel,

        -- Item metrics
        oi.quantity,
        oi.unit_price,
        oi.discount_amount,
        oi.discount_pct,
        oi.gross_line_total,
        oi.net_line_total,

        -- Margin (requires product cost from dim_product)
        cast(oi.quantity * dp.cost_price as decimal(12, 2))              as total_cost,
        cast(oi.net_line_total - (oi.quantity * dp.cost_price) as decimal(12, 2))
                                                                         as contribution_margin,
        case
            when oi.net_line_total > 0
                then round(
                    (oi.net_line_total - oi.quantity * dp.cost_price)
                    / oi.net_line_total * 100, 2
                )
            else null
        end                                                              as contribution_margin_pct

    from order_items oi
    join orders o         on oi.order_id = o.order_id
    left join dim_product dp  on oi.product_id = dp.product_id
    left join dim_customer dc on o.customer_id = dc.customer_id
    left join dim_date dd     on o.order_date = dd.date_day
    left join dim_channel dch on o.channel = dch.channel_name
)

select * from final
