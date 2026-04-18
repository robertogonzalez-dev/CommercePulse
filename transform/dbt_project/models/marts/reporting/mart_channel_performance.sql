-- Channel performance mart.
-- Joins sales, sessions, and marketing spend by channel to produce a unified
-- view of each channel's traffic, conversion, revenue, and ROI metrics.

with channel_orders as (
    select
        dch.channel_name,
        dch.channel_type,
        dch.is_paid,
        count(distinct fo.order_id)                                         as total_orders,
        count(distinct fo.customer_key)                                     as unique_customers,
        sum(case when fo.is_fulfilled then fo.gross_revenue else 0 end)     as gross_revenue,
        sum(case when fo.is_fulfilled then fo.net_revenue else 0 end)       as net_revenue,
        sum(case when fo.is_fulfilled then fo.total_discounts else 0 end)   as total_discounts,
        avg(case when fo.is_fulfilled then fo.gross_revenue end)            as avg_order_value
    from {{ ref('fact_orders') }} fo
    join {{ ref('dim_channel') }} dch on fo.channel_key = dch.channel_key
    group by 1, 2, 3
),

channel_sessions as (
    select
        dch.channel_name,
        count(distinct fs.session_id)                                       as total_sessions,
        count(distinct fs.customer_key)                                     as unique_visitors,
        sum(case when fs.is_converted then 1 else 0 end)                    as conversions,
        sum(case when fs.is_bounce then 1 else 0 end)                       as bounces,
        avg(fs.session_duration_seconds)                                    as avg_session_duration_secs,
        avg(fs.pages_viewed)                                                as avg_pages_per_session
    from {{ ref('fact_sessions') }} fs
    join {{ ref('dim_channel') }} dch on fs.channel_key = dch.channel_key
    group by 1
),

channel_spend as (
    select
        dch.channel_name,
        sum(fms.amount_spent)                                               as total_spend,
        sum(fms.impressions)                                                as total_impressions,
        sum(fms.clicks)                                                     as total_clicks,
        sum(fms.revenue_attributed)                                         as revenue_attributed,
        avg(fms.roas)                                                       as avg_roas,
        avg(fms.cost_per_conversion)                                        as avg_cpa
    from {{ ref('fact_marketing_spend') }} fms
    join {{ ref('dim_channel') }} dch on fms.channel_key = dch.channel_key
    group by 1
),

final as (
    select
        co.channel_name,
        co.channel_type,
        co.is_paid,

        -- Order metrics
        coalesce(co.total_orders, 0)                                        as total_orders,
        coalesce(co.unique_customers, 0)                                    as unique_customers,
        coalesce(co.gross_revenue, 0)                                       as gross_revenue,
        coalesce(co.net_revenue, 0)                                         as net_revenue,
        co.avg_order_value,

        -- Session / traffic metrics
        coalesce(cs.total_sessions, 0)                                      as total_sessions,
        coalesce(cs.unique_visitors, 0)                                     as unique_visitors,
        coalesce(cs.conversions, 0)                                         as session_conversions,
        coalesce(cs.bounces, 0)                                             as bounces,
        cs.avg_session_duration_secs,
        cs.avg_pages_per_session,
        {{ safe_divide('coalesce(cs.bounces, 0)', 'coalesce(cs.total_sessions, 0)') }} * 100
                                                                            as bounce_rate_pct,
        {{ safe_divide('coalesce(cs.conversions, 0)', 'coalesce(cs.total_sessions, 0)') }} * 100
                                                                            as session_conversion_rate_pct,

        -- Marketing spend (paid channels only)
        coalesce(sp.total_spend, 0)                                         as total_spend,
        coalesce(sp.total_impressions, 0)                                   as total_impressions,
        coalesce(sp.total_clicks, 0)                                        as total_clicks,
        coalesce(sp.revenue_attributed, 0)                                  as revenue_attributed,
        sp.avg_roas,
        sp.avg_cpa,

        -- Blended ROI: actual sales revenue / spend
        {{ safe_divide('coalesce(co.net_revenue, 0)', 'coalesce(sp.total_spend, 0)') }}
                                                                            as revenue_per_spend_dollar

    from channel_orders co
    left join channel_sessions cs on co.channel_name = cs.channel_name
    left join channel_spend sp    on co.channel_name = sp.channel_name
)

select * from final
