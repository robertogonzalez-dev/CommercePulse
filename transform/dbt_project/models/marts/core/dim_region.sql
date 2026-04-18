-- Region dimension derived from unique shipping address combinations in orders.
-- Grain: one row per distinct city/state/country combination.

with shipping_addresses as (
    select distinct
        shipping_city   as city,
        shipping_state  as state,
        shipping_country as country
    from {{ ref('stg_orders') }}
    where shipping_city is not null
      and shipping_state is not null
      and shipping_country is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country']) }}    as region_key,
        city,
        state,
        country,
        -- US region groupings for domestic segmentation
        case
            when country = 'US' and state in ('CT', 'ME', 'MA', 'NH', 'RI', 'VT',
                                               'NY', 'NJ', 'PA')               then 'Northeast'
            when country = 'US' and state in ('IL', 'IN', 'MI', 'OH', 'WI',
                                               'IA', 'KS', 'MN', 'MO', 'NE',
                                               'ND', 'SD')                     then 'Midwest'
            when country = 'US' and state in ('AL', 'AR', 'DC', 'DE', 'FL', 'GA',
                                               'KY', 'LA', 'MD', 'MS', 'NC',
                                               'OK', 'SC', 'TN', 'TX', 'VA',
                                               'WV')                           then 'South'
            when country = 'US' and state in ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM',
                                               'UT', 'WY', 'AK', 'CA', 'HI',
                                               'OR', 'WA')                     then 'West'
            when country = 'US'                                                then 'Other US'
            else country
        end                                                                     as region_name,
        country = 'US'                                                          as is_domestic

    from shipping_addresses
)

select * from final
