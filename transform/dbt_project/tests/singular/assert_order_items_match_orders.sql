-- Singular test: every order_item must reference a valid order in fact_orders.
-- A failure here means orphaned line items reached the Gold layer.

select oi.order_item_id
from {{ ref('fact_order_items') }} oi
left join {{ ref('fact_orders') }} fo on oi.order_id = fo.order_id
where fo.order_id is null
