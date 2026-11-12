-- QUERY 1: MONTHLY SALES ANALYSIS

SELECT
    date_format(date_trunc('month', order_purchase_date), '%Y-%m') AS sales_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS monthly_revenue
FROM ecommerce_platform_gold.fact_order_items
GROUP BY date_trunc('month', order_purchase_date)
ORDER BY date_trunc('month', order_purchase_date);


-- QUERY 2: MONTHLY SALES AND AVERAGE ORDER VALUE

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
    ROUND(monthly_revenue / total_orders, 2) AS average_order_value
FROM monthly_sales
ORDER BY month;