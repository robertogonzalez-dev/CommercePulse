with source as (
    select * from {{ source('bronze', 'raw_order_items') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_item_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as integer)                                               as quantity,
        cast(unit_price as decimal(12, 2))                                      as unit_price,
        cast(coalesce(discount_amount, 0) as decimal(12, 2))                    as discount_amount,
        cast(line_total as decimal(12, 2))                                      as line_total,
        -- Derived: gross = quantity × unit_price before any discount
        cast(quantity * unit_price as decimal(12, 2))                           as gross_line_total,
        -- Derived: net = what the customer actually paid for this line
        cast(quantity * unit_price - coalesce(discount_amount, 0) as decimal(12, 2))
                                                                                as net_line_total,
        case
            when quantity * unit_price > 0
                then round(coalesce(discount_amount, 0) / (quantity * unit_price) * 100, 2)
            else 0
        end                                                                     as discount_pct,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
