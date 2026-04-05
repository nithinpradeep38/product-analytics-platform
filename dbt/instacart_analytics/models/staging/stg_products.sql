with source as (

    select * from {{ source('instacart', 'products') }}

),

renamed as (

    select
        product_id,
        product_name,
        aisle_id,
        department_id,
        loaded_at

    from source

)

select * from renamed