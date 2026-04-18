-- Grain: one row per campaign.
-- Links paid marketing spend to attributed revenue. Supports ROAS, CPA,
-- and channel efficiency analysis across the full campaign portfolio.

with marketing as (
    select * from {{ ref('stg_marketing_spend') }}
),

dim_channel as (
    select channel_key, channel_name from {{ ref('dim_channel') }}
),

dim_date_start as (
    select date_key, date_day from {{ ref('dim_date') }}
),

dim_date_end as (
    select date_key, date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['m.spend_id']) }}           as marketing_spend_key,

        -- Natural keys
        m.spend_id,
        m.campaign_id,

        -- Foreign keys to dimensions
        dch.channel_key,
        ds.date_key                                                      as start_date_key,
        de.date_key                                                      as end_date_key,

        -- Campaign attributes
        m.campaign_name,
        m.channel,
        m.campaign_start_date,
        m.campaign_end_date,
        m.campaign_duration_days,

        -- Budget metrics
        m.budget,
        m.amount_spent,
        m.budget_utilization_pct,

        -- Reach and engagement
        m.impressions,
        m.clicks,
        m.conversions,
        m.revenue_attributed,

        -- Efficiency KPIs (pre-computed in staging)
        m.click_through_rate,
        m.conversion_rate,
        m.cost_per_conversion,
        m.roas,

        -- Additional derived metrics
        {{ safe_divide('m.amount_spent', 'm.impressions') }} * 1000      as cpm,
        {{ safe_divide('m.amount_spent', 'm.clicks') }}                  as cost_per_click,
        m.revenue_attributed - m.amount_spent                            as net_attributed_revenue

    from marketing m
    left join dim_channel dch on m.channel = dch.channel_name
    left join dim_date_start ds on m.campaign_start_date = ds.date_day
    left join dim_date_end de   on m.campaign_end_date = de.date_day
)

select * from final
