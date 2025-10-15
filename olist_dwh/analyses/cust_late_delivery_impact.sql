-- How severely does a late delivery impact the review score?
-- This query compares the average review score for on-time vs. late deliveries.

select
    is_late,
    count(distinct order_id) as number_of_orders,
    avg(review_score) as average_review_score
from {{ ref('fact_orders') }}
where review_score is not null
group by 1
order by 1;