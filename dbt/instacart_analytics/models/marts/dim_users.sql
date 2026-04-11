with user_history as (
    select * from {{ ref('int_user_order_history') }}
),
final as (
    select
        user_id,
        first_order_date        as valid_from,
        null::date              as valid_to,
        true                    as is_current,
        first_order_date,
        latest_order_date,
        total_orders,
        avg_days_between_orders
    from user_history
)
select * from final