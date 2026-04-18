-- Conformed channel dimension built from all channel-bearing source tables.
-- Ensures consistent channel labelling across orders, sessions, and marketing.

with order_channels as (
    select distinct channel as channel_name, 'orders' as channel_source
    from {{ ref('stg_orders') }}
    where channel is not null
),

session_channels as (
    select distinct channel as channel_name, 'web_sessions' as channel_source
    from {{ ref('stg_web_sessions') }}
    where channel is not null
),

marketing_channels as (
    select distinct channel as channel_name, 'marketing_spend' as channel_source
    from {{ ref('stg_marketing_spend') }}
    where channel is not null
),

all_channels as (
    select channel_name from order_channels
    union
    select channel_name from session_channels
    union
    select channel_name from marketing_channels
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['channel_name']) }}         as channel_key,
        channel_name,
        -- Classify into broad channel types for grouping in reports
        case
            when channel_name in ('organic_search', 'seo')              then 'organic'
            when channel_name in ('paid_search', 'ppc', 'google_ads')   then 'paid_search'
            when channel_name in ('paid_social', 'facebook', 'instagram',
                                  'tiktok', 'twitter')                  then 'paid_social'
            when channel_name in ('email', 'email_marketing')           then 'email'
            when channel_name in ('direct', 'direct_traffic')           then 'direct'
            when channel_name in ('referral', 'affiliate')              then 'referral'
            when channel_name in ('social', 'organic_social')           then 'organic_social'
            when channel_name in ('display', 'banner')                  then 'display'
            else 'other'
        end                                                              as channel_type,
        -- Paid vs owned/earned
        case
            when channel_name in ('paid_search', 'ppc', 'google_ads',
                                  'paid_social', 'facebook', 'instagram',
                                  'tiktok', 'display', 'banner')        then true
            else false
        end                                                              as is_paid

    from all_channels
)

select * from final
