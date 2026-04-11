with order_products as (
    select * from {{ ref('stg_order_products') }}
)
select * from order_products