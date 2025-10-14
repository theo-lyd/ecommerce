-- models/staging/stg_customers.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'customers') }}
),
renamed as (
    select
    
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as zip_code_prefix,
        customer_city as city,
        customer_state as state

    from source
)

select * from renamed