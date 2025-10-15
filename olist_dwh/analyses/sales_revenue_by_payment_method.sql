-- How does revenue distribute across different payment methods?

select
    primary_payment_type,
    sum(total_payment_value) as total_revenue,
    count(distinct order_id) as number_of_orders
from {{ ref('fact_orders') }}
group by 1
order by 2 desc;