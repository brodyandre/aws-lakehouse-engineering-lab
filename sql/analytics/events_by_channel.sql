SELECT
    COALESCE(c.channel, 'organic_or_unknown') AS channel,
    w.event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT w.session_id) AS total_sessions,
    COUNT(DISTINCT w.customer_key) AS distinct_customers
FROM gold.fct_web_events AS w
LEFT JOIN gold.dim_campaign AS c
    ON w.campaign_key = c.campaign_key
GROUP BY
    COALESCE(c.channel, 'organic_or_unknown'),
    w.event_type
ORDER BY
    channel,
    total_events DESC;
