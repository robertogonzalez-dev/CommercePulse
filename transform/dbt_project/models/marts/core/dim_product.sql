with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}          as product_key,

        -- Natural key
        product_id,

        -- Descriptors
        product_name,
        category_l1,
        category_l2,
        category_l1 || ' > ' || category_l2                            as category_path,
        brand,
        sku,

        -- Pricing
        cost_price,
        list_price,
        gross_margin,
        gross_margin_pct,

        -- Physical
        weight_kg,

        -- Price tier classification for reporting segmentation
        case
            when list_price < 25                                        then 'budget'
            when list_price < 75                                        then 'mid_range'
            when list_price < 200                                       then 'premium'
            else 'luxury'
        end                                                             as price_tier,

        -- Lifecycle
        is_active,
        created_date

    from products
)

select * from final
