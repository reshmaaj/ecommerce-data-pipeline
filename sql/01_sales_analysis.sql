-- 1. Monthly sales and revenue

SELECT
    date_format(date_trunc('month', order_purchase_date), '%Y-%m') AS sales_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS monthly_revenue
FROM ecommerce_platform_gold.fact_order_items
GROUP BY date_trunc('month', order_purchase_date)
ORDER BY date_trunc('month', order_purchase_date);


-- 2. Monthly sales, revenue and average order value

WITH monthly_sales AS (
    SELECT
        date_trunc('month', order_purchase_date) AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(price) AS monthly_revenue
    FROM ecommerce_platform_gold.fact_order_items
    GROUP BY date_trunc('month', order_purchase_date)
)

SELECT
    date_format(month, '%Y-%m') AS sales_month,
    total_orders,
    ROUND(monthly_revenue, 2) AS monthly_revenue,
    ROUND(monthly_revenue / NULLIF(total_orders, 0), 2) AS average_order_value
FROM monthly_sales
ORDER BY month;


-- 3. Payment method analysis

SELECT
    payment_type,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(payment_value), 2) AS total_payment_value
FROM ecommerce_platform_gold.fact_order_payments
GROUP BY payment_type
ORDER BY total_payment_value DESC;