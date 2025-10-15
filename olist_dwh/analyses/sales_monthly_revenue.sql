-- What are the monthly revenue trends?
-- This query provides the total revenue for each month, ordered chronologically.

select
    date_trunc('month', purchased_at)::date as sales_month,
    sum(price) as monthly_revenue
from {{ ref('fact_orders') }}
group by 1
order by 1;