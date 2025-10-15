{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'geolocation') }}
),

renamed as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        geolocation_lat as lat,
        geolocation_lng as lng,
        geolocation_city as city,
        geolocation_state as state
    from source
)

select * from renamed