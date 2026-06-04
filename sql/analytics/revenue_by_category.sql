SELECT
    p.category,
    SUM(f.net_amount) AS revenue_net,
    SUM(f.gross_amount) AS revenue_gross,
    SUM(f.discount_amount) AS total_discount,
    SUM(f.quantity) AS units_sold
FROM gold.fct_sales AS f
JOIN gold.dim_product AS p
    ON f.product_key = p.product_key
GROUP BY
    p.category
ORDER BY
    revenue_net DESC;
