-- Singular test: net revenue must not exceed gross revenue on any fulfilled order.
-- A failure indicates a data issue where refunds exceed the original order value.

select order_id
from {{ ref('fact_orders') }}
where is_fulfilled
  and net_revenue > gross_revenue
