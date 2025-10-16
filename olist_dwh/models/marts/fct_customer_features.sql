{{ config(
    materialized='table',
    tags=['mart']
) }}

with fact_orders as (
    select * from {{ ref('fact_orders') }}
),

-- Define the same snapshot date as our previous CLV model for consistency
snapshot_date as (
    select (max(purchased_at) - interval '6 month') as value from fact_orders
),

-- Base table of customers and their orders BEFORE the snapshot date
customer_history as (
    select
        customer_unique_id,
        order_id,
        purchased_at,
        total_payment_value,
        product_category,
        review_score
    from fact_orders
    where purchased_at < (select value from snapshot_date)
),

-- Calculate historical RFM and Satisfaction features
customer_features as (
    select
        customer_unique_id,
        -- Recency
        ((select value from snapshot_date)::date - max(purchased_at)::date) as recency,
        -- Frequency
        count(distinct order_id) as frequency,
        -- Monetary
        sum(total_payment_value) as monetary,
        -- Satisfaction Features
        avg(review_score) as avg_review_score,
        -- Flag if the customer's most recent review was low
        max(case when rn_desc = 1 and review_score <= 2 then 1 else 0 end) as last_order_low_review_flag
    from (
        -- Use a window function to find the most recent order for each customer
        select *, row_number() over (partition by customer_unique_id order by purchased_at desc) as rn_desc
        from customer_history
    ) as ranked_orders
    group by 1
),

-- Calculate historical Product-based features
product_features as (
    select
        customer_unique_id,
        count(distinct product_category) as number_of_unique_product_categories,
        -- Find the most frequently purchased category for each customer
        max(case when rn_asc = 1 then product_category end) as most_frequent_product_category
    from (
        select
            customer_unique_id,
            product_category,
            row_number() over (partition by customer_unique_id order by count(*) desc, max(purchased_at) desc) as rn_asc
        from customer_history
        where product_category is not null
        group by 1, 2
    ) as category_ranks
    group by 1
),

-- Calculate the CLASSIFICATION LABEL: will the customer return in the next 6 months?
future_behavior as (
    select
        distinct customer_unique_id,
        1 as will_return_in_6m
    from fact_orders
    where purchased_at >= (select value from snapshot_date)
      and purchased_at < ((select value from snapshot_date) + interval '6 month')
),

-- Join all feature sets and the label together
final as (
    select
        cf.customer_unique_id,
        cf.recency,
        cf.frequency,
        cf.monetary,
        cf.avg_review_score,
        cf.last_order_low_review_flag,
        pf.number_of_unique_product_categories,
        pf.most_frequent_product_category,
        coalesce(fb.will_return_in_6m, 0) as will_return_in_6m
    from customer_features as cf
    left join product_features as pf on cf.customer_unique_id = pf.customer_unique_id
    left join future_behavior as fb on cf.customer_unique_id = fb.customer_unique_id
)

select * from final