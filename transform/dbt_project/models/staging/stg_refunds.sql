with source as (
    select * from {{ source('bronze', 'raw_refunds') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by refund_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        refund_id,
        order_id,
        order_item_id,
        customer_id,
        cast(refund_date as date)                                  as refund_date,
        lower(trim(refund_reason))                                 as refund_reason,
        cast(refund_amount as decimal(12, 2))                      as refund_amount,
        lower(trim(refund_status))                                 as refund_status,
        lower(trim(refund_method))                                 as refund_method,
        refund_status = 'approved'                                 as is_completed,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
