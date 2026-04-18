-- Grain: one row per web session.
-- Powers conversion funnel, traffic source, and device performance analysis.

with sessions as (
    select * from {{ ref('stg_web_sessions') }}
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

dim_device as (
    select device_key, device_type from {{ ref('dim_device') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.session_id']) }}         as session_key,

        -- Natural keys
        s.session_id,
        s.order_id,

        -- Foreign keys to dimensions
        dc.customer_key,
        dd.date_key,
        dch.channel_key,
        dv.device_key,

        -- Session attributes
        s.session_started_at,
        s.session_ended_at,
        s.session_date,
        s.channel,
        s.device_type,
        s.landing_page,
        s.utm_source,
        s.utm_medium,
        s.campaign_id,

        -- Engagement metrics
        s.pages_viewed,
        s.products_viewed,
        s.cart_adds,
        s.checkout_started,
        s.session_duration_seconds,
        s.session_duration_minutes,

        -- Funnel flags
        s.is_bounce,
        s.checkout_started    as did_start_checkout,
        s.is_converted,

        -- Funnel step reached (for funnel visualisation)
        case
            when s.is_converted         then 4
            when s.checkout_started     then 3
            when s.cart_adds > 0        then 2
            when s.products_viewed > 0  then 1
            else 0
        end                                                              as funnel_step_reached

    from sessions s
    left join dim_customer dc on s.customer_id = dc.customer_id
    left join dim_date dd     on s.session_date = dd.date_day
    left join dim_channel dch on s.channel = dch.channel_name
    left join dim_device dv   on s.device_type = dv.device_type
)

select * from final
