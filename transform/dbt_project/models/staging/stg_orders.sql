with source as (
    select * from {{ source('bronze', 'raw_orders') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        order_id,
        customer_id,
        cast(order_date as timestamp)                              as ordered_at,
        cast(order_date as date)                                   as order_date,
        lower(trim(order_status))                                  as order_status,
        lower(trim(channel))                                       as channel,
        trim(shipping_address_city)                                as shipping_city,
        upper(trim(shipping_address_state))                        as shipping_state,
        upper(trim(shipping_address_country))                      as shipping_country,
        trim(shipping_address_zip)                                 as shipping_zip,
        upper(trim(promotion_code))                                as promotion_code,
        cast(coalesce(shipping_cost, 0) as decimal(10, 2))         as shipping_cost,
        cast(estimated_delivery_date as date)                      as estimated_delivery_date,
        cast(actual_delivery_date as date)                         as actual_delivery_date,
        case
            when actual_delivery_date is not null
                 and estimated_delivery_date is not null
                 and actual_delivery_date > estimated_delivery_date
            then true
            else false
        end                                                        as is_late_delivery,
        order_status not in ('cancelled', 'returned')              as is_fulfilled,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
