with customers as (
    select * from {{ ref('stg_customers') }}
),

metrics as (
    select * from {{ ref('int_customer_order_metrics') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }}        as customer_key,

        -- Natural key
        c.customer_id,

        -- Identity
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.gender,

        -- Demographics
        c.date_of_birth,
        datediff('year', c.date_of_birth, current_date)                 as age,
        case
            when datediff('year', c.date_of_birth, current_date) < 25   then '18-24'
            when datediff('year', c.date_of_birth, current_date) < 35   then '25-34'
            when datediff('year', c.date_of_birth, current_date) < 45   then '35-44'
            when datediff('year', c.date_of_birth, current_date) < 55   then '45-54'
            else '55+'
        end                                                              as age_band,

        -- Location
        c.city,
        c.state,
        c.country,
        c.zip_code,

        -- Acquisition
        c.acquisition_channel,
        c.registered_at,
        c.registration_date,
        c.is_active,

        -- Behavioural attributes (from int_customer_order_metrics)
        coalesce(m.total_orders, 0)                                      as total_orders,
        coalesce(m.non_cancelled_orders, 0)                              as non_cancelled_orders,
        coalesce(m.delivered_orders, 0)                                  as delivered_orders,
        m.first_ordered_at,
        m.last_ordered_at,
        m.customer_lifespan_days,
        coalesce(m.is_repeat_customer, false)                            as is_repeat_customer,
        coalesce(m.customer_segment, 'no_purchase')                      as customer_segment

    from customers c
    left join metrics m on c.customer_id = m.customer_id
)

select * from final
