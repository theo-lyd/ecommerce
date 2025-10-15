{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'sellers') }}
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix as zip_code_prefix,
        seller_city as city,
        seller_state as state
    from source
)

select * from renamed