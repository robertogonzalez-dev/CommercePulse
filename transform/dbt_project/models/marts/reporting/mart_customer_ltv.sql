-- Customer lifetime value mart.
-- One row per customer with historical CLV, predicted 2-year CLV, RFM scores,
-- and segment classification. Designed for retention and loyalty analysis.

with customers as (
    select
        dc.customer_key,
        dc.customer_id,
        dc.full_name,
        dc.email,
        dc.gender,
        dc.age_band,
        dc.city,
        dc.state,
        dc.country,
        dc.acquisition_channel,
        dc.registration_date,
        dc.is_active,
        dc.customer_segment,
        dc.is_repeat_customer
    from {{ ref('dim_customer') }} dc
),

order_metrics as (
    select
        customer_key,
        count(distinct order_id)                                                as total_orders,
        count(distinct case when is_fulfilled then order_id end)                as fulfilled_orders,
        sum(case when is_fulfilled then gross_revenue else 0 end)               as total_gross_revenue,
        sum(case when is_fulfilled then net_revenue else 0 end)                 as total_net_revenue,
        sum(case when is_fulfilled then amount_refunded else 0 end)             as total_refunded,
        sum(case when is_fulfilled then total_discounts else 0 end)             as total_discounts,
        avg(case when is_fulfilled then gross_revenue end)                      as avg_order_value,
        min(ordered_at)                                                         as first_ordered_at,
        max(ordered_at)                                                         as last_ordered_at,
        datediff('day', min(ordered_at), max(ordered_at))                       as lifespan_days,
        datediff('day', max(ordered_at), current_timestamp)                     as days_since_last_order
    from {{ ref('fact_orders') }}
    group by 1
),

rfm as (
    select
        customer_key,
        days_since_last_order                                                   as recency_days,
        fulfilled_orders                                                        as frequency,
        total_net_revenue                                                       as monetary,

        -- RFM score (1-5 each): higher = better
        ntile(5) over (order by days_since_last_order asc)                      as recency_score,
        ntile(5) over (order by fulfilled_orders desc)                          as frequency_score,
        ntile(5) over (order by total_net_revenue desc)                         as monetary_score
    from order_metrics
    where fulfilled_orders > 0
),

final as (
    select
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.email,
        c.gender,
        c.age_band,
        c.city,
        c.state,
        c.country,
        c.acquisition_channel,
        c.registration_date,
        c.is_active,
        c.customer_segment,
        c.is_repeat_customer,

        -- Order history
        coalesce(om.total_orders, 0)                                            as total_orders,
        coalesce(om.fulfilled_orders, 0)                                        as fulfilled_orders,
        om.first_ordered_at,
        om.last_ordered_at,
        om.lifespan_days,
        om.days_since_last_order,

        -- Revenue summary
        coalesce(om.total_gross_revenue, 0)                                     as total_gross_revenue,
        coalesce(om.total_net_revenue, 0)                                       as historical_clv,
        coalesce(om.total_refunded, 0)                                          as total_refunded,
        coalesce(om.total_discounts, 0)                                         as total_discounts,
        coalesce(om.avg_order_value, 0)                                         as avg_order_value,

        -- Predicted CLV (avg order value × annualised purchase rate × 2 years)
        case
            when coalesce(om.lifespan_days, 0) > 0
                then round(
                    coalesce(om.avg_order_value, 0)
                    * (cast(om.fulfilled_orders as decimal) / om.lifespan_days * 730),
                    2
                )
            else coalesce(om.avg_order_value, 0)
        end                                                                     as predicted_clv_2yr,

        -- RFM
        coalesce(rfm.recency_score, 0)                                          as recency_score,
        coalesce(rfm.frequency_score, 0)                                        as frequency_score,
        coalesce(rfm.monetary_score, 0)                                         as monetary_score,
        coalesce(rfm.recency_score, 0)
            + coalesce(rfm.frequency_score, 0)
            + coalesce(rfm.monetary_score, 0)                                   as rfm_total_score,

        -- RFM segment label
        case
            when coalesce(rfm.recency_score, 0) >= 4
                 and coalesce(rfm.frequency_score, 0) >= 4                      then 'Champion'
            when coalesce(rfm.recency_score, 0) >= 3
                 and coalesce(rfm.frequency_score, 0) >= 3                      then 'Loyal'
            when coalesce(rfm.recency_score, 0) >= 4
                 and coalesce(rfm.frequency_score, 0) < 2                       then 'New Customer'
            when coalesce(rfm.recency_score, 0) <= 2
                 and coalesce(rfm.frequency_score, 0) >= 3                      then 'At Risk'
            when coalesce(rfm.recency_score, 0) <= 2
                 and coalesce(rfm.frequency_score, 0) <= 2                      then 'Lost'
            when om.fulfilled_orders is null                                    then 'Never Purchased'
            else 'Needs Attention'
        end                                                                     as rfm_segment

    from customers c
    left join order_metrics om on c.customer_key = om.customer_key
    left join rfm              on c.customer_key = rfm.customer_key
)

select * from final
