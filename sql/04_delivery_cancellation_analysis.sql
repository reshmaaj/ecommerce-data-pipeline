-- 1. Late delivery percentage
-- Use DISTINCT orders because fact_order_items has multiple rows per order.

WITH orders AS (
    SELECT DISTINCT
        order_id,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM ecommerce_platform_gold.fact_order_items
    WHERE order_status = 'delivered'
)

SELECT
    COUNT(*) AS total_delivered_orders,
    SUM(
        CASE
            WHEN CAST(order_delivered_customer_date AS TIMESTAMP)
                 > CAST(order_estimated_delivery_date AS TIMESTAMP)
            THEN 1
            ELSE 0
        END
    ) AS late_orders,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN CAST(order_delivered_customer_date AS TIMESTAMP)
                     > CAST(order_estimated_delivery_date AS TIMESTAMP)
                THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS late_delivery_percentage
FROM orders;


-- 2. Average delivery time in days

WITH orders AS (
    SELECT DISTINCT
        order_id,
        order_purchase_timestamp,
        order_delivered_customer_date
    FROM ecommerce_platform_gold.fact_order_items
    WHERE order_status = 'delivered'
)

SELECT
    ROUND(
        AVG(
            date_diff(
                'hour',
                CAST(order_purchase_timestamp AS TIMESTAMP),
                CAST(order_delivered_customer_date AS TIMESTAMP)
            ) / 24.0
        ),
        2
    ) AS average_delivery_days
FROM orders;


-- 3. Monthly cancellation trend

WITH monthly_orders AS (
    SELECT DISTINCT
        order_id,
        order_status,
        date_trunc('month', order_purchase_date) AS month
    FROM ecommerce_platform_gold.fact_order_items
)

SELECT
    date_format(month, '%Y-%m') AS sales_month,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
    ROUND(
        100.0 * SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS cancellation_percentage
FROM monthly_orders
GROUP BY month
ORDER BY month;