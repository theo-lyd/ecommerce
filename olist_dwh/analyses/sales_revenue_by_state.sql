-- Which states are our revenue hotspots? (Top 10)

select
    state,
    sum(total_payment_value) as total_revenue
from {{ ref('fact_orders') }}
group by 1
order by 2 desc
limit 10;