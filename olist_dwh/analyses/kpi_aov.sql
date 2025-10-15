-- Average Order Value (AOV)
select
    avg(total_payment_value) as average_order_value
from (
    -- First, get the unique value for each order to avoid double-counting
    select distinct
        order_id,
        total_payment_value
    from {{ ref('fact_orders') }}
) as order_values;