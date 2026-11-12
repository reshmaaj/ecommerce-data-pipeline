-- 1. Top 10 product categories by revenue

-- First, check the actual column name in dim_products.
DESCRIBE ecommerce_platform_gold.dim_products;

-- Use the English category column confirmed by DESCRIBE
-- in place of the placeholder below.

SELECT
    p.product_category_name_english AS product_category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(f.price), 2) AS total_revenue
FROM ecommerce_platform_gold.fact_order_items f
JOIN ecommerce_platform_gold.dim_products p
    ON f.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;


-- 2. Top 10 sellers by revenue

SELECT
    f.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(f.price), 2) AS total_revenue
FROM ecommerce_platform_gold.fact_order_items f
JOIN ecommerce_platform_gold.dim_sellers s
    ON f.seller_id = s.seller_id
GROUP BY
    f.seller_id,
    s.seller_city,
    s.seller_state
ORDER BY total_revenue DESC
LIMIT 10;


-- 3. Total number of sellers

SELECT
    COUNT(*) AS total_sellers
FROM ecommerce_platform_gold.dim_sellers;