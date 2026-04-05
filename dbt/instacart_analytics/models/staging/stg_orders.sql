with source as (

    select * from {{ source('instacart', 'orders') }}

),

renamed as (

    select
        order_id,
        user_id,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        order_date::date                        as order_date,
        order_number = 1                        as is_first_order,
        loaded_at

    from source

)

select * from renamed