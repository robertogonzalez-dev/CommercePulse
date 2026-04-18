-- Grain: one row per product per warehouse per snapshot date.
-- Tracks inventory position over time. Enables stock-out risk analysis,
-- reorder flag monitoring, and warehouse-level availability reporting.

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

dim_product as (
    select product_key, product_id, cost_price, list_price
    from {{ ref('dim_product') }}
),

dim_date as (
    select date_key, date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key: product + warehouse + snapshot date
        {{ dbt_utils.generate_surrogate_key(['i.inventory_id']) }}       as inventory_snapshot_key,

        -- Natural keys
        i.inventory_id,
        i.product_id,
        i.warehouse_id,

        -- Foreign keys to dimensions
        dp.product_key,
        dd.date_key,

        -- Snapshot context
        i.warehouse_name,
        i.snapshot_date,

        -- Stock levels
        i.quantity_on_hand,
        i.quantity_reserved,
        i.quantity_available,
        i.reorder_level,
        i.reorder_quantity,

        -- Stock status flags
        i.is_below_reorder,
        i.is_out_of_stock,
        i.stock_status,

        -- Inventory value (at cost and at retail)
        cast(i.quantity_on_hand * dp.cost_price as decimal(14, 2))       as inventory_cost_value,
        cast(i.quantity_on_hand * dp.list_price as decimal(14, 2))       as inventory_retail_value,

        -- Potential lost revenue if out of stock (using average reorder qty as proxy)
        case
            when i.is_out_of_stock
                then cast(i.reorder_quantity * dp.list_price as decimal(14, 2))
            else 0
        end                                                              as at_risk_revenue

    from inventory i
    left join dim_product dp on i.product_id = dp.product_id
    left join dim_date dd    on i.snapshot_date = dd.date_day
)

select * from final
