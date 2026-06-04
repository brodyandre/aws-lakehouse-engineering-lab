CREATE OR REPLACE VIEW gold.dm_sales_overview AS
SELECT
    d.full_date,
    c.country,
    c.state,
    p.category,
    COALESCE(cam.channel, 'organic_or_unknown') AS channel,
    SUM(f.quantity) AS total_quantity,
    SUM(f.gross_amount) AS total_gross_amount,
    SUM(f.discount_amount) AS total_discount_amount,
    SUM(f.net_amount) AS total_net_amount,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold.fct_sales AS f
JOIN gold.dim_date AS d
    ON f.date_key = d.date_key
JOIN gold.dim_customer AS c
    ON f.customer_key = c.customer_key
JOIN gold.dim_product AS p
    ON f.product_key = p.product_key
LEFT JOIN gold.dim_campaign AS cam
    ON f.campaign_key = cam.campaign_key
GROUP BY
    d.full_date,
    c.country,
    c.state,
    p.category,
    COALESCE(cam.channel, 'organic_or_unknown');
