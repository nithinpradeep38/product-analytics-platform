with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}
),
final as (
    select
        to_number(to_char(date_day, 'YYYYMMDD'))    as date_key,
        date_day                                     as full_date,
        dayofweek(date_day)                          as day_of_week,
        weekofyear(date_day)                         as week_number,
        month(date_day)                              as month,
        quarter(date_day)                            as quarter,
        year(date_day)                               as year,
        {{ is_weekend('dayofweek(date_day)') }}      as is_weekend
    from date_spine
)
select * from final