SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.net_amount) AS revenue_net,
    SUM(f.gross_amount) AS revenue_gross,
    SUM(f.discount_amount) AS total_discount,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold.fct_sales AS f
JOIN gold.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;
