-- Customer Distribution by State
select
    state,
    count(distinct customer_unique_id) as number_of_customers
from {{ ref('fact_orders') }}
group by 1
order by 2 desc
limit 10;