with source as (
    select * from {{ source('bronze', 'raw_marketing_spend') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by spend_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        spend_id,
        trim(campaign_id)                                          as campaign_id,
        trim(campaign_name)                                        as campaign_name,
        lower(trim(channel))                                       as channel,
        cast(start_date as date)                                   as campaign_start_date,
        cast(end_date as date)                                     as campaign_end_date,
        datediff('day', cast(start_date as date), cast(end_date as date)) + 1
                                                                   as campaign_duration_days,
        cast(budget as decimal(14, 2))                             as budget,
        cast(amount_spent as decimal(14, 2))                       as amount_spent,
        case
            when budget > 0
                then round(amount_spent / budget * 100, 2)
            else null
        end                                                        as budget_utilization_pct,
        cast(impressions as integer)                               as impressions,
        cast(clicks as integer)                                    as clicks,
        cast(conversions as integer)                               as conversions,
        cast(revenue_attributed as decimal(14, 2))                 as revenue_attributed,
        -- Campaign efficiency metrics
        {{ safe_divide('cast(clicks as decimal)', 'impressions') }} * 100
                                                                   as click_through_rate,
        {{ safe_divide('cast(conversions as decimal)', 'clicks') }} * 100
                                                                   as conversion_rate,
        {{ safe_divide('amount_spent', 'conversions') }}           as cost_per_conversion,
        {{ safe_divide('revenue_attributed', 'amount_spent') }}    as roas,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
