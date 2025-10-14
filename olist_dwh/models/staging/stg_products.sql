{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'products') }}
),

renamed as (
    select
        product_id,
        product_category_name as product_category
        -- We can add other product details here later if needed
    from source
)

select * from renamed