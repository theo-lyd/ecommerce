{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'marketing_qualified_leads') }}
),

renamed as (
    select
        mql_id,
        cast(first_contact_date as date) as first_contact_date,
        landing_page_id,
        origin
    from source
)

select * from renamed