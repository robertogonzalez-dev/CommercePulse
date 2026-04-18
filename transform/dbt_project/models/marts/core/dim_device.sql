-- Device dimension derived from web session device types.

with devices as (
    select distinct
        lower(trim(device_type)) as device_type
    from {{ ref('stg_web_sessions') }}
    where device_type is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['device_type']) }}     as device_key,
        device_type,
        case
            when device_type = 'desktop'    then 'Desktop'
            when device_type = 'mobile'     then 'Mobile'
            when device_type = 'tablet'     then 'Tablet'
            else 'Unknown'
        end                                                         as device_label,
        device_type in ('mobile', 'tablet')                         as is_mobile

    from devices
)

select * from final
