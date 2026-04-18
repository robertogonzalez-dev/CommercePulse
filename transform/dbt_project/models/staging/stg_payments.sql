with source as (
    select * from {{ source('bronze', 'raw_payments') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by payment_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        payment_id,
        order_id,
        cast(payment_date as timestamp)                            as paid_at,
        cast(payment_date as date)                                 as payment_date,
        lower(trim(payment_method))                                as payment_method,
        lower(trim(payment_status))                                as payment_status,
        cast(amount as decimal(12, 2))                             as amount,
        upper(trim(currency))                                      as currency,
        trim(transaction_id)                                       as transaction_id,
        lower(trim(gateway))                                       as gateway,
        payment_status = 'success'                                 as is_successful,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
