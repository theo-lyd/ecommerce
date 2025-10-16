{{ config(
    materialized='table',
    tags=['recsys']
) }}

select distinct
    customer_unique_id,
    product_id
from {{ ref('fact_orders') }}