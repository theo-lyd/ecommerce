{{ config(
    materialized='table',
    tags=['mart']
) }}

with
orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    -- We aggregate this to the order level
    select
        order_id,
        seller_id, 
        count(order_item_id) as number_of_items,
        sum(price) as total_price,
        sum(freight_value) as total_freight_value
    from {{ ref('stg_order_items') }}
    group by 1, 2
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- We need to aggregate geolocation data to one lat/lon per zip code
geolocation as (
    select
        zip_code_prefix,
        avg(lat) as lat,
        avg(lng) as lng
    from {{ ref('stg_geolocation') }} 
    group by 1
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.purchased_at,
        i.seller_id,

        -- Labels / Target Variables
        (o.delivered_to_customer_at > o.estimated_delivery_at) as is_late,
        (cast(o.delivered_to_carrier_at as date) - cast(o.approved_at as date)) as seller_handling_days,
        (cast(o.delivered_to_customer_at as date) - cast(o.delivered_to_carrier_at as date)) as carrier_transit_days,

        -- Time-Based Features
        extract(dow from o.purchased_at) as purchase_day_of_week, -- 0=Sun, 1=Mon,...
        extract(month from o.purchased_at) as purchase_month,

        -- Geographic Distance Feature (Haversine formula in kilometers)
        -- This is a simplified formula and works well for this use case
        6371 * 2 * asin(sqrt(
            power(sin((radians(cg.lat) - radians(sg.lat)) / 2), 2) +
            cos(radians(sg.lat)) * cos(radians(cg.lat)) *
            power(sin((radians(cg.lng) - radians(sg.lng)) / 2), 2)
        )) as distance_km,

        -- Other relevant features
        i.number_of_items,
        i.total_price,
        i.total_freight_value,
        s.state as seller_state,
        c.state as customer_state

    from orders as o
    left join order_items as i on o.order_id = i.order_id
    left join sellers as s on i.seller_id = s.seller_id
    left join customers as c on o.customer_id = c.customer_id
    -- Join to get customer geolocation
    left join geolocation as cg on c.zip_code_prefix = cg.zip_code_prefix
    -- Join to get seller geolocation
    left join geolocation as sg on s.zip_code_prefix = sg.zip_code_prefix
    where o.delivered_to_customer_at is not null -- Only use completed orders
      and o.approved_at is not null
)

select * from final