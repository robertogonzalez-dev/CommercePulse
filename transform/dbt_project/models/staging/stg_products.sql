with source as (
    select * from {{ source('bronze', 'raw_products') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by product_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

final as (
    select
        product_id,
        trim(product_name)                                         as product_name,
        trim(category_l1)                                          as category_l1,
        coalesce(trim(category_l2), 'Uncategorized')               as category_l2,
        trim(brand)                                                as brand,
        upper(trim(sku))                                           as sku,
        cast(cost_price as decimal(12, 2))                         as cost_price,
        cast(list_price as decimal(12, 2))                         as list_price,
        cast(list_price - cost_price as decimal(12, 2))            as gross_margin,
        case
            when list_price > 0
                then round((list_price - cost_price) / list_price * 100, 2)
            else null
        end                                                        as gross_margin_pct,
        cast(weight_kg as decimal(8, 3))                           as weight_kg,
        cast(is_active as boolean)                                 as is_active,
        cast(created_date as date)                                 as created_date,
        _ingested_at,
        _batch_id,
        _source_file,
        _row_hash
    from deduped
    where _row_num = 1
)

select * from final
