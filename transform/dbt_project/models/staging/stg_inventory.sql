with source as (
    select * from {{ source('bronze', 'raw_inventory') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by inventory_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        inventory_id,
        product_id,
        trim(warehouse_id)                                         as warehouse_id,
        trim(warehouse_name)                                       as warehouse_name,
        cast(quantity_on_hand as integer)                          as quantity_on_hand,
        cast(quantity_reserved as integer)                         as quantity_reserved,
        cast(quantity_available as integer)                        as quantity_available,
        cast(reorder_level as integer)                             as reorder_level,
        cast(reorder_quantity as integer)                          as reorder_quantity,
        cast(last_updated as date)                                 as snapshot_date,
        quantity_available <= reorder_level                        as is_below_reorder,
        quantity_available = 0                                     as is_out_of_stock,
        -- Days of stock remaining is estimated externally; flag for risk analysis
        case
            when quantity_available = 0        then 'out_of_stock'
            when quantity_available <= reorder_level then 'low_stock'
            else 'in_stock'
        end                                                        as stock_status,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
