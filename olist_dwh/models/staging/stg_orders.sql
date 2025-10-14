-- models/staging/stg_orders.sql

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_status,

        -- Timestamps are correctly cast to timestamp type
        cast(order_purchase_timestamp as timestamp) as purchased_at,
        cast(order_approved_at as timestamp) as approved_at,
        cast(order_delivered_carrier_date as timestamp) as delivered_to_carrier_at,
        cast(order_delivered_customer_date as timestamp) as delivered_to_customer_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at,

        -- New columns for just the date part, as you suggested
        cast(order_purchase_timestamp as date) as purchase_date,
        cast(order_estimated_delivery_date as date) as estimated_delivery_date
    
    from source
)
select * from renamed