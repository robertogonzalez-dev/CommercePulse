-- Date dimension covering the full analytical date range.
-- date_key is an integer YYYYMMDD suitable for use as a FK in fact tables.

with date_spine as (
    select
        unnest(
            generate_series(
                cast('{{ var("date_spine_start") }}' as date),
                cast('{{ var("date_spine_end") }}' as date),
                interval '1 day'
            )
        )::date as date_day
),

final as (
    select
        -- Surrogate key: YYYYMMDD integer
        cast(strftime(date_day, '%Y%m%d') as integer)                   as date_key,
        date_day,

        -- Calendar fields
        year(date_day)                                                  as year,
        quarter(date_day)                                               as quarter_number,
        'Q' || quarter(date_day)                                        as quarter_label,
        cast(year(date_day) as varchar)
            || '-Q' || cast(quarter(date_day) as varchar)               as year_quarter,
        month(date_day)                                                 as month_number,
        strftime(date_day, '%B')                                        as month_name,
        strftime(date_day, '%b')                                        as month_name_short,
        strftime(date_day, '%Y-%m')                                     as year_month,
        day(date_day)                                                   as day_of_month,
        dayofweek(date_day)                                             as day_of_week,       -- 0=Sun, 6=Sat
        strftime(date_day, '%A')                                        as day_name,
        strftime(date_day, '%a')                                        as day_name_short,
        dayofyear(date_day)                                             as day_of_year,
        weekofyear(date_day)                                            as week_of_year,

        -- Relative period flags
        date_day = current_date                                         as is_today,
        date_day < current_date                                         as is_past,
        date_day > current_date                                         as is_future,
        dayofweek(date_day) in (0, 6)                                   as is_weekend,
        dayofweek(date_day) not in (0, 6)                               as is_weekday,

        -- Period-end flags
        date_day = last_day(date_day)                                   as is_month_end,
        date_day = date_trunc('month', date_day)                        as is_month_start,
        -- Quarter end: last day of March, June, September, December
        month(date_day) in (3, 6, 9, 12)
            and date_day = last_day(date_day)                           as is_quarter_end,
        -- Year end
        month(date_day) = 12 and day(date_day) = 31                     as is_year_end

    from date_spine
)

select * from final
