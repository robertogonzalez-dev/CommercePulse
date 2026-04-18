-- Aggregates order-level financial metrics from items, payments, and refunds.
-- Used as a pre-joined, finance-complete record in fact_orders to avoid
-- repeating four-table join logic across multiple downstream consumers.

with orders as (
    select order_id, shipping_cost
    from {{ ref('stg_orders') }}
),

order_items_agg as (
    select
        order_id,
        count(distinct order_item_id)                               as item_count,
        sum(quantity)                                               as total_quantity,
        sum(gross_line_total)                                       as gross_subtotal,
        sum(discount_amount)                                        as total_discounts,
        sum(net_line_total)                                         as net_subtotal
    from {{ ref('stg_order_items') }}
    group by 1
),

payments_agg as (
    select
        order_id,
        sum(case when is_successful then amount else 0 end)         as amount_paid,
        max(case when is_successful then payment_method end)        as primary_payment_method,
        max(case when is_successful then gateway end)               as payment_gateway
    from {{ ref('stg_payments') }}
    group by 1
),

refunds_agg as (
    select
        order_id,
        sum(case when is_completed then refund_amount else 0 end)   as amount_refunded,
        count(case when is_completed then 1 end)                    as refund_count
    from {{ ref('stg_refunds') }}
    group by 1
),

final as (
    select
        o.order_id,
        coalesce(oia.item_count, 0)                                 as item_count,
        coalesce(oia.total_quantity, 0)                             as total_quantity,
        coalesce(oia.gross_subtotal, 0)                             as gross_subtotal,
        coalesce(oia.total_discounts, 0)                            as total_discounts,
        coalesce(oia.net_subtotal, 0)                               as net_subtotal,
        coalesce(o.shipping_cost, 0)                                as shipping_cost,
        -- Gross revenue = net item total + shipping (before any refunds)
        coalesce(oia.net_subtotal, 0) + coalesce(o.shipping_cost, 0)
                                                                    as gross_revenue,
        coalesce(pa.amount_paid, 0)                                 as amount_paid,
        coalesce(ra.amount_refunded, 0)                             as amount_refunded,
        coalesce(ra.refund_count, 0)                                as refund_count,
        -- Net revenue = gross revenue after refunds are applied
        coalesce(oia.net_subtotal, 0) + coalesce(o.shipping_cost, 0)
            - coalesce(ra.amount_refunded, 0)                       as net_revenue,
        ra.amount_refunded > 0                                      as has_refund,
        pa.primary_payment_method,
        pa.payment_gateway
    from orders o
    left join order_items_agg oia on o.order_id = oia.order_id
    left join payments_agg pa    on o.order_id = pa.order_id
    left join refunds_agg ra     on o.order_id = ra.order_id
)

select * from final
