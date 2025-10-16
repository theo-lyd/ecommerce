{{ config(
    materialized='table',
    tags=['recsys']
) }}

with products as (
    select * from {{ ref('stg_products') }}
),

-- This CTE adds the product category name in English
product_category_translation as (
    select * from {{ ref('stg_product_category_name_translation') }}
)

select
    p.product_id,
    t.product_category_name_english as product_category,
    p.product_photos_qty,
    -- We will add more features here later if needed
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from products p
left join product_category_translation t on p.product_category = t.product_category_name