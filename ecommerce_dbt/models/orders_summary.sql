WITH base AS (
    SELECT
        user_id,
        event_type,
        product_id,
        price,
        event_time::date AS event_date
    FROM {{ source('ecommerce', 'user_events') }}
),

filtered AS (
    SELECT * FROM base
    WHERE event_type = 'purchase'
)

SELECT
    event_date,
    COUNT(*) AS total_orders,
    SUM(price) AS total_revenue
FROM filtered
GROUP BY event_date
ORDER BY event_date