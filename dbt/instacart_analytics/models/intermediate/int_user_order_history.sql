with orders as (
    select * from {{ ref('stg_orders') }}
),
final as (
    select
        user_id,
        min(order_date)                 as first_order_date,
        max(order_date)                 as latest_order_date,
        count(order_id)                 as total_orders,
        avg(days_since_prior_order)     as avg_days_between_orders
    from orders
    group by user_id
)
select * from final