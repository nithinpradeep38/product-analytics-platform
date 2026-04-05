with source as (

    select * from {{ source('instacart', 'departments') }}

),

renamed as (

    select
        department_id,
        department,
        loaded_at

    from source

)

select * from renamed