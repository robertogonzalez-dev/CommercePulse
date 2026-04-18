with source as (
    select * from {{ source('bronze', 'raw_web_sessions') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by session_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        session_id,
        customer_id,
        cast(session_start as timestamp)                           as session_started_at,
        cast(session_end as timestamp)                             as session_ended_at,
        cast(session_start as date)                                as session_date,
        lower(trim(channel))                                       as channel,
        lower(trim(device_type))                                   as device_type,
        trim(landing_page)                                         as landing_page,
        cast(coalesce(pages_viewed, 0) as integer)                 as pages_viewed,
        cast(coalesce(products_viewed, 0) as integer)              as products_viewed,
        cast(coalesce(cart_adds, 0) as integer)                    as cart_adds,
        cast(coalesce(checkout_started, 0) as integer) > 0        as checkout_started,
        order_id,
        order_id is not null                                       as is_converted,
        trim(campaign_id)                                          as campaign_id,
        trim(utm_source)                                           as utm_source,
        lower(trim(utm_medium))                                    as utm_medium,
        cast(coalesce(session_duration_seconds, 0) as integer)     as session_duration_seconds,
        round(
            cast(coalesce(session_duration_seconds, 0) as decimal) / 60.0, 2
        )                                                          as session_duration_minutes,
        -- Engagement score: bounced sessions have 1 page view and no further interaction
        pages_viewed <= 1
            and coalesce(products_viewed, 0) = 0
            and coalesce(cart_adds, 0) = 0                         as is_bounce,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
