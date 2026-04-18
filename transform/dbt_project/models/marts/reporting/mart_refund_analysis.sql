-- Refund analysis mart.
-- Aggregates refunds by reason, channel, product, and time period to surface
-- quality and customer satisfaction signals.

with refunds as (
    select
        fr.refund_key,
        fr.refund_id,
        fr.order_id,
        fr.refund_date,
        fr.refund_reason,
        fr.refund_status,
        fr.refund_method,
        fr.is_completed,
        fr.completed_refund_amount,
        dd.year,
        dd.year_month,
        dd.year_quarter,
        dp.product_name,
        dp.category_l1,
        dp.category_l2,
        dp.brand,
        dc.customer_segment,
        dch.channel_name,
        dch.channel_type
    from {{ ref('fact_refunds') }} fr
    left join {{ ref('dim_date') }}     dd  on fr.date_key    = dd.date_key
    left join {{ ref('dim_product') }}  dp  on fr.product_key = dp.product_key
    left join {{ ref('dim_customer') }} dc  on fr.customer_key = dc.customer_key
    left join {{ ref('dim_channel') }}  dch on fr.channel_key  = dch.channel_key
),

orders_baseline as (
    select
        count(distinct order_id)                                        as total_orders,
        sum(case when is_fulfilled then net_revenue else 0 end)         as total_net_revenue
    from {{ ref('fact_orders') }}
),

refund_summary as (
    select
        year,
        year_month,
        year_quarter,
        refund_reason,
        channel_name,
        category_l1,
        count(distinct refund_id)                                       as refund_count,
        count(distinct order_id)                                        as orders_refunded,
        sum(completed_refund_amount)                                    as total_refunded,
        avg(completed_refund_amount)                                    as avg_refund_amount,
        count(distinct case when refund_method = 'original_payment' then refund_id end)
                                                                        as original_payment_refunds,
        count(distinct case when refund_method = 'store_credit' then refund_id end)
                                                                        as store_credit_refunds
    from refunds
    where is_completed
    group by 1, 2, 3, 4, 5, 6
),

final as (
    select
        rs.*,
        ob.total_orders,
        ob.total_net_revenue,
        {{ safe_divide('rs.orders_refunded', 'ob.total_orders') }} * 100
                                                                        as refund_rate_pct,
        {{ safe_divide('rs.total_refunded', 'ob.total_net_revenue') }} * 100
                                                                        as revenue_impact_pct
    from refund_summary rs
    cross join orders_baseline ob
)

select * from final
