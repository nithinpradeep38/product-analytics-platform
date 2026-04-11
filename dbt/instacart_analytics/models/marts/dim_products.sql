with products_enriched as (
    select * from {{ ref('int_products_enriched') }}
)
select * from products_enriched