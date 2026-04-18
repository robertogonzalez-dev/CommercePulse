-- Customer-level transactional summary built from staging tables.
-- Feeds dim_customer for segment classification and mart_customer_ltv for CLV analysis.

with orders as (
    select
        customer_id,
        order_id,
        ordered_at,
        order_date,
        order_status,
        channel,
        promotion_code
    from {{ ref('stg_orders') }}
),

financials as (
    select
        order_id,
        gross_revenue,
        net_revenue,
        amount_refunded,
        has_refund,
        total_discounts
    from {{ ref('int_order_financials') }}
),

joined as (
    select
        o.customer_id,
        o.order_id,
        o.ordered_at,
        o.order_date,
        o.order_status,
        o.channel,
        o.promotion_code,
        coalesce(f.gross_revenue, 0)    as gross_revenue,
        coalesce(f.net_revenue, 0)      as net_revenue,
        coalesce(f.amount_refunded, 0)  as amount_refunded,
        coalesce(f.has_refund, false)   as has_refund,
        coalesce(f.total_discounts, 0)  as total_discounts
    from orders o
    left join financials f on o.order_id = f.order_id
),

aggregated as (
    select
        customer_id,
        count(distinct order_id)                                        as total_orders,
        count(distinct case
            when order_status not in ('cancelled') then order_id
        end)                                                            as non_cancelled_orders,
        count(distinct case
            when order_status = 'delivered' then order_id
        end)                                                            as delivered_orders,
        sum(case
            when order_status not in ('cancelled') then gross_revenue else 0
        end)                                                            as total_gross_revenue,
        sum(case
            when order_status not in ('cancelled') then net_revenue else 0
        end)                                                            as total_net_revenue,
        sum(case
            when order_status not in ('cancelled') then amount_refunded else 0
        end)                                                            as total_refunded,
        sum(case
            when order_status not in ('cancelled') then total_discounts else 0
        end)                                                            as total_discounts,
        avg(case
            when order_status not in ('cancelled') then gross_revenue
        end)                                                            as avg_order_value,
        min(ordered_at)                                                 as first_ordered_at,
        max(ordered_at)                                                 as last_ordered_at,
        count(distinct case when has_refund then order_id end)          as orders_with_refunds,
        count(distinct channel)                                         as distinct_channels_used,
        count(distinct case when promotion_code is not null then order_id end)
                                                                        as orders_with_promo
    from joined
    group by 1
),

final as (
    select
        *,
        datediff('day', first_ordered_at, last_ordered_at)             as customer_lifespan_days,
        non_cancelled_orders > 1                                        as is_repeat_customer,
        case
            when total_orders = 0                   then 'no_purchase'
            when non_cancelled_orders = 1           then 'one_time'
            when non_cancelled_orders between 2 and 4 then 'repeat'
            else 'loyal'
        end                                                             as customer_segment,
        -- Simple 2-year predicted CLV based on historical avg and purchase frequency
        case
            when customer_lifespan_days > 0
                then round(
                    avg_order_value
                    * (cast(non_cancelled_orders as decimal)
                       / nullif(datediff('day', first_ordered_at, last_ordered_at), 0)
                       * 730),
                    2
                )
            else avg_order_value
        end                                                             as predicted_clv_2yr
    from aggregated
)

select * from final
