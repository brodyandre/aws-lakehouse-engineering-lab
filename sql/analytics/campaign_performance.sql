WITH sales_by_campaign AS (
    SELECT
        campaign_key,
        SUM(net_amount) AS revenue_net,
        SUM(gross_amount) AS revenue_gross,
        SUM(discount_amount) AS total_discount,
        COUNT(DISTINCT order_id) AS total_orders
    FROM gold.fct_sales
    WHERE campaign_key IS NOT NULL
    GROUP BY
        campaign_key
),
events_by_campaign AS (
    SELECT
        campaign_key,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM gold.fct_web_events
    WHERE campaign_key IS NOT NULL
    GROUP BY
        campaign_key
)
SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.budget,
    COALESCE(s.revenue_net, 0) AS revenue_net,
    COALESCE(s.revenue_gross, 0) AS revenue_gross,
    COALESCE(s.total_discount, 0) AS total_discount,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(e.total_events, 0) AS total_events,
    COALESCE(e.total_sessions, 0) AS total_sessions
FROM gold.dim_campaign AS c
LEFT JOIN sales_by_campaign AS s
    ON c.campaign_key = s.campaign_key
LEFT JOIN events_by_campaign AS e
    ON c.campaign_key = e.campaign_key
ORDER BY
    revenue_net DESC,
    total_events DESC;
