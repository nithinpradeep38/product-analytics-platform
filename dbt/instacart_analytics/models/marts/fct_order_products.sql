with order_products as (
    select * from {{ ref('stg_order_products') }}
),
orders as (
    select order_id, order_date from {{ ref('fct_orders') }}
),
final as (
    select
        op.order_id,
        op.product_id,
        op.add_to_cart_order,
        op.is_reordered,
        o.order_date
    from order_products op
    left join orders o on op.order_id = o.order_id
)
select * from final