-- Query for Power BI Geographic Summary
-- Provides total revenue, order count, and customer count per city/state.

SELECT
    -- Location fields for Power BI Map visual
    c.state,
    c.city,

    -- Aggregated Metrics
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_unique_id) AS total_unique_customers,
    SUM(fo.total_payment_value) AS total_revenue

FROM {{ ref('fact_orders') }} AS fo
JOIN {{ ref('stg_orders') }} AS o ON fo.order_id = o.order_id
JOIN {{ ref('stg_customers') }} AS c ON o.customer_id = c.customer_id

GROUP BY
    c.state,
    c.city

ORDER BY
    total_revenue DESC;