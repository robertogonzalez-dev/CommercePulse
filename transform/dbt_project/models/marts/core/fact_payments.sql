-- Grain: one row per payment transaction.
-- Enables payment method mix analysis, gateway performance, and revenue reconciliation.

with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select order_id, customer_id, order_date, channel
    from {{ ref('stg_orders') }}
),

dim_customer as (
    select customer_key, customer_id from {{ ref('dim_customer') }}
),

dim_date as (
    select date_key, date_day from {{ ref('dim_date') }}
),

dim_channel as (
    select channel_key, channel_name from {{ ref('dim_channel') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }}         as payment_key,

        -- Natural keys
        p.payment_id,
        p.order_id,
        p.transaction_id,

        -- Foreign keys to dimensions
        dc.customer_key,
        dd.date_key,
        dch.channel_key,

        -- Payment attributes
        p.paid_at,
        p.payment_date,
        p.payment_method,
        p.payment_status,
        p.gateway,
        p.currency,
        p.is_successful,

        -- Financial
        p.amount,
        case when p.is_successful then p.amount else 0 end              as collected_amount,
        case when not p.is_successful then p.amount else 0 end          as failed_amount

    from payments p
    left join orders o    on p.order_id = o.order_id
    left join dim_customer dc on o.customer_id = dc.customer_id
    left join dim_date dd     on p.payment_date = dd.date_day
    left join dim_channel dch on o.channel = dch.channel_name
)

select * from final
