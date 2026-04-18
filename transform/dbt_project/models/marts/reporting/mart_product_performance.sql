-- Product performance mart.
-- One row per product with sales volume, revenue, margin, and refund metrics.

with order_items as (
    select
        foi.product_key,
        foi.order_id,
        foi.quantity,
        foi.unit_price,
        foi.discount_amount,
        foi.gross_line_total,
        foi.net_line_total,
        foi.total_cost,
        foi.contribution_margin,
        foi.order_status
    from {{ ref('fact_order_items') }} foi
),

refunds as (
    select
        fr.product_key,
        sum(fr.completed_refund_amount)                         as total_refunded,
        count(distinct fr.refund_id)                            as refund_count
    from {{ ref('fact_refunds') }} fr
    group by 1
),

inventory as (
    select
        product_key,
        max(quantity_available)                                 as current_qty_available,
        max(quantity_on_hand)                                   as current_qty_on_hand,
        max(stock_status)                                       as current_stock_status,
        max(inventory_cost_value)                               as current_inventory_cost_value
    from {{ ref('fact_inventory_snapshots') }}
    group by 1
),

products as (
    select * from {{ ref('dim_product') }}
),

item_metrics as (
    select
        product_key,
        count(distinct order_id)                                as total_orders,
        sum(quantity)                                           as units_sold,
        sum(gross_line_total)                                   as gross_revenue,
        sum(net_line_total)                                     as net_revenue,
        sum(discount_amount)                                    as total_discounts,
        sum(total_cost)                                         as total_cogs,
        sum(contribution_margin)                                as total_contribution_margin,
        avg(unit_price)                                         as avg_selling_price,
        avg(contribution_margin)                                as avg_contribution_margin_per_item
    from order_items
    where order_status not in ('cancelled')
    group by 1
),

final as (
    select
        p.product_key,
        p.product_id,
        p.product_name,
        p.category_l1,
        p.category_l2,
        p.category_path,
        p.brand,
        p.sku,
        p.cost_price,
        p.list_price,
        p.gross_margin,
        p.gross_margin_pct,
        p.price_tier,
        p.is_active,

        -- Sales performance
        coalesce(im.total_orders, 0)                            as total_orders,
        coalesce(im.units_sold, 0)                              as units_sold,
        coalesce(im.gross_revenue, 0)                           as gross_revenue,
        coalesce(im.net_revenue, 0)                             as net_revenue,
        coalesce(im.total_discounts, 0)                         as total_discounts,
        coalesce(im.total_cogs, 0)                              as total_cogs,
        coalesce(im.total_contribution_margin, 0)               as total_contribution_margin,
        im.avg_selling_price,
        im.avg_contribution_margin_per_item,

        -- Refund metrics
        coalesce(r.total_refunded, 0)                           as total_refunded,
        coalesce(r.refund_count, 0)                             as refund_count,
        {{ safe_divide('coalesce(r.refund_count, 0)', 'coalesce(im.total_orders, 0)') }} * 100
                                                                as refund_rate_pct,

        -- Margin rate on actual sales
        {{ safe_divide(
            'coalesce(im.total_contribution_margin, 0)',
            'coalesce(im.net_revenue, 0)'
        ) }} * 100                                              as realized_margin_pct,

        -- Inventory position
        coalesce(inv.current_qty_available, 0)                  as current_qty_available,
        coalesce(inv.current_qty_on_hand, 0)                    as current_qty_on_hand,
        coalesce(inv.current_stock_status, 'unknown')           as current_stock_status,
        coalesce(inv.current_inventory_cost_value, 0)           as current_inventory_cost_value

    from products p
    left join item_metrics im on p.product_key = im.product_key
    left join refunds r        on p.product_key = r.product_key
    left join inventory inv    on p.product_key = inv.product_key
)

select * from final
