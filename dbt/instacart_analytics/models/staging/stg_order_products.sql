with source as (

    select * from {{ source('instacart', 'order_products') }}

),

renamed as (

    select
        order_id,
        product_id,
        add_to_cart_order,
        reordered::boolean                      as is_reordered,
        loaded_at

    from source

)

select * from renamed