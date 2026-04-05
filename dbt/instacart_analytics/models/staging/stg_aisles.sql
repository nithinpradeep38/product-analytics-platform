with source as (

    select * from {{ source('instacart', 'aisles') }}

),

renamed as (

    select
        aisle_id,
        aisle,
        loaded_at

    from source

)

select * from renamed