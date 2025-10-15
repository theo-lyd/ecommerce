-- analyses/kpi_monthly_revenue.sql
select
    -- Use TO_CHAR for PostgreSQL to format the date
    to_char(purchased_at, 'YYYY-MM') as sales_month,

    -- Sum the price for each month
    sum(price) as monthly_revenue

-- Replace {{ ref('fact_orders') }} with analytics.fact_orders.
-- Click the "Run on active connection" button (a play icon with a database symbol) in the top-right corner of your editor.

from {{ ref('fact_orders') }}
group by 1
order by 1;