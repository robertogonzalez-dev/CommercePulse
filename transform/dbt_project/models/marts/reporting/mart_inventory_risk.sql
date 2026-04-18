-- Inventory risk mart.
-- One row per product per warehouse. Flags products at risk of stock-out,
-- estimates days of cover, and quantifies the revenue exposure.

with inventory as (
    select * from {{ ref('fact_inventory_snapshots') }}
),

products as (
    select
        product_key,
        product_id,
        product_name,
        category_l1,
        brand,
        list_price,
        cost_price,
        is_active
    from {{ ref('dim_product') }}
),

-- Average daily units sold (last 90 days of orders)
sales_velocity as (
    select
        foi.product_key,
        count(distinct foi.order_id)                                        as orders_last_90d,
        sum(foi.quantity)                                                   as units_sold_last_90d,
        sum(foi.quantity) / 90.0                                            as avg_daily_units
    from {{ ref('fact_order_items') }} foi
    join {{ ref('fact_orders') }} fo on foi.order_id = fo.order_id
    where fo.order_date >= current_date - interval '90 days'
      and fo.order_status not in ('cancelled', 'returned')
    group by 1
),

final as (
    select
        inv.inventory_snapshot_key,
        inv.inventory_id,
        inv.warehouse_id,
        inv.warehouse_name,
        inv.snapshot_date,

        -- Product attributes
        p.product_id,
        p.product_name,
        p.category_l1,
        p.brand,
        p.is_active,
        p.list_price,
        p.cost_price,

        -- Stock levels
        inv.quantity_on_hand,
        inv.quantity_reserved,
        inv.quantity_available,
        inv.reorder_level,
        inv.reorder_quantity,
        inv.stock_status,

        -- Sales velocity
        coalesce(sv.units_sold_last_90d, 0)                                 as units_sold_last_90d,
        coalesce(sv.avg_daily_units, 0)                                     as avg_daily_units,

        -- Days of cover: how many days before stock runs out at current velocity
        case
            when coalesce(sv.avg_daily_units, 0) > 0
                then round(inv.quantity_available / sv.avg_daily_units, 1)
            else null
        end                                                                 as days_of_cover,

        -- Risk classification
        case
            when inv.is_out_of_stock                                        then 'critical'
            when inv.is_below_reorder
                 and coalesce(sv.avg_daily_units, 0) > 0
                 and (inv.quantity_available / sv.avg_daily_units) < 7      then 'high'
            when inv.is_below_reorder                                       then 'medium'
            else 'low'
        end                                                                 as risk_level,

        -- Revenue at risk
        inv.at_risk_revenue,
        cast(
            coalesce(inv.reorder_quantity, 0) * p.list_price as decimal(14, 2)
        )                                                                   as reorder_cost_at_retail,

        -- Valuation
        inv.inventory_cost_value,
        inv.inventory_retail_value

    from inventory inv
    join products p on inv.product_key = p.product_key
    left join sales_velocity sv on inv.product_key = sv.product_key
)

select * from final
order by
    case risk_level
        when 'critical' then 1
        when 'high'     then 2
        when 'medium'   then 3
        else 4
    end,
    at_risk_revenue desc
