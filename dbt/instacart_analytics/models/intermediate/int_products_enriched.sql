with products as (
    select * from {{ ref('stg_products') }}
),
aisles as (
    select * from {{ ref('stg_aisles') }}
),
departments as (
    select * from {{ ref('stg_departments') }}
),
final as (
    select
        p.product_id,
        p.product_name,
        p.aisle_id,
        a.aisle,
        p.department_id,
        d.department
    from products p
    left join aisles a on p.aisle_id = a.aisle_id
    left join departments d on p.department_id = d.department_id
)
select * from final