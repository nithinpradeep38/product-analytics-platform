with orders as (
    select * from {{ ref('stg_orders') }}
),
order_products as (
    select * from {{ ref('stg_order_products') }}
),
order_aggregates as (
    select
        order_id,
        count(product_id)               as total_products_in_order,
        sum(case when is_reordered then 1 else 0 end) as total_reordered_products,
        total_reordered_products / nullif(total_products_in_order, 0) as reorder_rate_in_order
    from order_products
    group by order_id
),
final as (
    select
        o.*,
        oa.total_products_in_order,
        oa.total_reordered_products,
        oa.reorder_rate_in_order
    from orders o
    left join order_aggregates oa on o.order_id = oa.order_id
)
select * from final