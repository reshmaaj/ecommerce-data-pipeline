-- 1. Overall order and revenue reconciliation

WITH orders AS (
    SELECT DISTINCT
        order_id,
        order_status
    FROM ecommerce_platform_gold.fact_order_items
),
sales AS (
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(price), 2) AS total_revenue
    FROM ecommerce_platform_gold.fact_order_items
),
order_summary AS (
    SELECT
        COUNT(*) AS orders_in_status_table,
        SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)
            AS delivered_orders,
        SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
            AS canceled_orders,
        ROUND(
            100.0 * SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
        ) AS cancellation_rate
    FROM orders
)

SELECT
    sales.total_orders,
    sales.total_revenue,
    order_summary.orders_in_status_table,
    order_summary.delivered_orders,
    order_summary.canceled_orders,
    order_summary.cancellation_rate
FROM sales
CROSS JOIN order_summary;


-- 2. Gold table validation against Silver

WITH checks AS (

    SELECT
        'fact_order_items row count' AS check_name,
        (SELECT COUNT(*) FROM ecommerce_platform_gold.fact_order_items) AS actual_value,
        (SELECT COUNT(*) FROM ecommerce_platform_silver.order_items) AS expected_value

    UNION ALL

    SELECT
        'fact_order_payments row count',
        (SELECT COUNT(*) FROM ecommerce_platform_gold.fact_order_payments),
        (SELECT COUNT(*) FROM ecommerce_platform_silver.order_payments)

    UNION ALL

    SELECT
        'dim_customers row count',
        (SELECT COUNT(*) FROM ecommerce_platform_gold.dim_customers),
        (SELECT COUNT(*) FROM ecommerce_platform_silver.customers)

    UNION ALL

    SELECT
        'dim_products row count',
        (SELECT COUNT(*) FROM ecommerce_platform_gold.dim_products),
        (SELECT COUNT(*) FROM ecommerce_platform_silver.products)

    UNION ALL

    SELECT
        'dim_sellers row count',
        (SELECT COUNT(*) FROM ecommerce_platform_gold.dim_sellers),
        (SELECT COUNT(*) FROM ecommerce_platform_silver.sellers)

    UNION ALL

    SELECT
        'Duplicate order items',
        (
            SELECT COUNT(*)
            FROM (
                SELECT order_id, order_item_id
                FROM ecommerce_platform_gold.fact_order_items
                GROUP BY order_id, order_item_id
                HAVING COUNT(*) > 1
            ) d
        ),
        0

    UNION ALL

    SELECT
        'Null order IDs in fact',
        (
            SELECT COUNT(*)
            FROM ecommerce_platform_gold.fact_order_items
            WHERE order_id IS NULL
        ),
        0

    UNION ALL

    SELECT
        'Unmatched products',
        (
            SELECT COUNT(*)
            FROM ecommerce_platform_gold.fact_order_items f
            LEFT JOIN ecommerce_platform_gold.dim_products p
                ON f.product_id = p.product_id
            WHERE p.product_id IS NULL
        ),
        0

    UNION ALL

    SELECT
        'Unmatched sellers',
        (
            SELECT COUNT(*)
            FROM ecommerce_platform_gold.fact_order_items f
            LEFT JOIN ecommerce_platform_gold.dim_sellers s
                ON f.seller_id = s.seller_id
            WHERE s.seller_id IS NULL
        ),
        0

    UNION ALL

    SELECT
        'Unmatched customers',
        (
            SELECT COUNT(*)
            FROM ecommerce_platform_gold.fact_order_items f
            LEFT JOIN ecommerce_platform_gold.dim_customers c
                ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        ),
        0
)

SELECT
    check_name,
    actual_value,
    expected_value,
    CASE
        WHEN actual_value = expected_value THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM checks
ORDER BY check_name;