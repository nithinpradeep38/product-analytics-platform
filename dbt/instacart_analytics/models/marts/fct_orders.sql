with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),
final as (
    select
        order_id,
        user_id,
        to_number(to_char(order_date, 'YYYYMMDD')) as date_key,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        order_date,
        is_first_order,
        total_products_in_order,
        total_reordered_products,
        reorder_rate_in_order
    from orders_enriched
)
select * from final