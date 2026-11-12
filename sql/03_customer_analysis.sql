-- 1. Top 10 customers by total spending

SELECT
    customer_unique_id,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS total_spent
FROM ecommerce_platform_gold.fact_order_items
GROUP BY customer_unique_id
ORDER BY total_spent DESC
LIMIT 10;


-- 2. Repeat customer analysis

WITH customer_orders AS (
    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) AS total_orders
    FROM ecommerce_platform_gold.fact_order_items
    GROUP BY customer_unique_id
)

SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(
        100.0 * SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS repeat_customer_percentage
FROM customer_orders;


-- 3. Monthly repeat-customer trend

WITH customer_months AS (
    SELECT DISTINCT
        customer_unique_id,
        date_trunc('month', order_purchase_date) AS month
    FROM ecommerce_platform_gold.fact_order_items
),
ranked AS (
    SELECT
        customer_unique_id,
        month,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY month
        ) AS purchase_number
    FROM customer_months
)

SELECT
    date_format(month, '%Y-%m') AS sales_month,
    COUNT(*) AS customers,
    SUM(CASE WHEN purchase_number > 1 THEN 1 ELSE 0 END) AS repeat_customers
FROM ranked
GROUP BY month
ORDER BY month;


-- 4. Revenue by customer state

SELECT
    customer_state,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS total_revenue
FROM ecommerce_platform_gold.fact_order_items
GROUP BY customer_state
ORDER BY total_revenue DESC
LIMIT 10;