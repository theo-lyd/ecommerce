-- Who are the top 10 customers by total spending?

with customer_spending as (
    select
        customer_unique_id,
        sum(total_payment_value) as total_spent
    from (
        -- Get the unique value for each order first to avoid double-counting
        select distinct
            order_id,
            customer_unique_id,
            total_payment_value
        from {{ ref('fact_orders') }}
    ) as order_values
    group by 1
)

select
    customer_unique_id,
    total_spent
from customer_spending
order by 2 desc
limit 10;