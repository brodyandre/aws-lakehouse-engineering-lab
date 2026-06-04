SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    SUM(f.net_amount) AS revenue_net,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold
FROM gold.fct_sales AS f
JOIN gold.dim_customer AS c
    ON f.customer_key = c.customer_key
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city,
    c.state
ORDER BY
    revenue_net DESC,
    total_orders DESC
LIMIT 20;
