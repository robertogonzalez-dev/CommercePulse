-- Singular test: every order must resolve to a date_key in dim_date.
-- A failure means the date spine range is too narrow and needs extending.

select fo.order_id
from {{ ref('fact_orders') }} fo
left join {{ ref('dim_date') }} dd on fo.date_key = dd.date_key
where dd.date_key is null
