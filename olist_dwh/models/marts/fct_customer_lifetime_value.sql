{{ config(
    materialized='table',
    tags=['mart']
) }}

with fact_orders as (
    select * from {{ ref('fact_orders') }}
),

-- Define the snapshot date, which is our "prediction point in time"
-- Let's set it to 6 months before the last order in the dataset
snapshot_date as (
    select (max(purchased_at) - interval '6 month') as value from fact_orders
),

-- Get distinct order values before the snapshot date
customer_history as (
    select distinct
        customer_unique_id,
        order_id,
        purchased_at,
        total_payment_value
    from fact_orders
    where purchased_at < (select value from snapshot_date)
),

-- Calculate RFM features based on the customer's history
rfm_features as (
    select
        customer_unique_id,
        -- Recency: Days from the snapshot date to the customer's last purchase
        ((select value from snapshot_date)::date - max(purchased_at)::date) as recency,
        -- Frequency: Total number of orders
        count(distinct order_id) as frequency,
        -- Monetary: Total spend
        sum(total_payment_value) as monetary
    from customer_history
    group by 1
),

-- Calculate the label: future revenue in the 6 months AFTER the snapshot date
future_revenue as (
    select
        customer_unique_id,
        sum(total_payment_value) as future_6m_revenue
    from (
        -- Subquery to get distinct order-level values first
        select distinct order_id, customer_unique_id, total_payment_value, purchased_at from fact_orders
    ) as orders
    where orders.purchased_at >= (select value from snapshot_date)
      and orders.purchased_at < ((select value from snapshot_date) + interval '6 month') -- Use interval syntax
    group by 1
),

final as (
    select
        r.customer_unique_id,
        r.recency,
        r.frequency,
        r.monetary,
        coalesce(f.future_6m_revenue, 0) as future_6m_revenue
    from rfm_features as r
    left join future_revenue as f on r.customer_unique_id = f.customer_unique_id
)

select * from final