with source as (
    select * from {{ source('bronze', 'raw_customers') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        customer_id,
        trim(first_name)                                            as first_name,
        trim(last_name)                                             as last_name,
        trim(first_name) || ' ' || trim(last_name)                 as full_name,
        lower(trim(email))                                         as email,
        trim(phone)                                                as phone,
        lower(trim(gender))                                        as gender,
        cast(date_of_birth as date)                                as date_of_birth,
        trim(city)                                                 as city,
        upper(trim(state))                                         as state,
        upper(trim(country))                                       as country,
        trim(zip_code)                                             as zip_code,
        lower(trim(acquisition_channel))                           as acquisition_channel,
        cast(registration_date as timestamp)                       as registered_at,
        cast(registration_date as date)                            as registration_date,
        cast(is_active as boolean)                                 as is_active,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
