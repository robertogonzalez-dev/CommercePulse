-- Grain: one row per refund record.
-- Enables refund rate, reason analysis, and net revenue impact measurement.

with refunds as (
    select * from {{ ref('stg_refunds') }}
),

orders as (
    select order_id, order_date, channel
    from {{ ref('stg_orders') }}
),

order_items as (
    select order_item_id, product_id
    from {{ ref('stg_order_items') }}
),

dim_customer as (
    select customer_key, customer_id from {{ ref('dim_customer') }}
),

dim_product as (
    select product_key, product_id from {{ ref('dim_product') }}
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
        {{ dbt_utils.generate_surrogate_key(['r.refund_id']) }}          as refund_key,

        -- Natural keys
        r.refund_id,
        r.order_id,
        r.order_item_id,

        -- Foreign keys to dimensions
        dc.customer_key,
        dp.product_key,
        dd.date_key,
        dch.channel_key,

        -- Refund attributes
        r.refund_date,
        r.refund_reason,
        r.refund_status,
        r.refund_method,
        r.is_completed,

        -- Financial
        r.refund_amount,
        case when r.is_completed then r.refund_amount else 0 end         as completed_refund_amount

    from refunds r
    left join orders o          on r.order_id = o.order_id
    left join order_items oi    on r.order_item_id = oi.order_item_id
    left join dim_customer dc   on r.customer_id = dc.customer_id
    left join dim_product dp    on oi.product_id = dp.product_id
    left join dim_date dd       on r.refund_date = dd.date_day
    left join dim_channel dch   on o.channel = dch.channel_name
)

select * from final
