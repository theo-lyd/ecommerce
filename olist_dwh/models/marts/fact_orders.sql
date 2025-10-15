-- models/marts/fact_orders.sql

{{ config(
    materialized='table',
    tags=['mart']
) }}

with
order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Aggregate payments to the order level to avoid a fan-out
order_payments_agg as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as max_payment_installments,
        -- Take the most frequent payment type for simplicity
        max(payment_type) as primary_payment_type
    from {{ ref('stg_order_payments') }}
    group by 1
),

final as (
    select
        -- Keys
        oi.order_item_id,
        oi.order_id,
        oi.seller_id,
        oi.product_id,
        o.customer_id,
        c.customer_unique_id,

        -- Timestamps & Status
        o.purchased_at,
        o.order_status,
        o.delivered_to_customer_at,

        -- Product Info
        p.product_category,

        -- Seller Info
        s.city as seller_city,
        s.state as seller_state,

        -- Customer Info
        c.city,
        c.state,

        -- Order Values
        oi.price,
        oi.freight_value,
        op.total_payment_value,
        op.max_payment_installments,
        op.primary_payment_type,

        -- Review Info
        r.review_score,
        r.review_comment_message,

        -- Calculated Fields (Feature Engineering)
        -- In PostgreSQL, subtracting timestamps gives an 'interval' type.
        -- We cast the result to a date and then subtract to get the number of days.
        (cast(o.delivered_to_customer_at as date) - cast(o.purchased_at as date)) as days_to_delivery,
        
        -- Flag for late deliveries
        case when o.delivered_to_customer_at > o.estimated_delivery_at then true else false end as is_late

    from order_items as oi
    left join orders as o on oi.order_id = o.order_id
    left join products as p on oi.product_id = p.product_id
    left join customers as c on o.customer_id = c.customer_id
    left join order_payments_agg as op on oi.order_id = op.order_id
    left join order_reviews as r on oi.order_id = r.order_id
    left join sellers as s on oi.seller_id = s.seller_id
)

select * from final