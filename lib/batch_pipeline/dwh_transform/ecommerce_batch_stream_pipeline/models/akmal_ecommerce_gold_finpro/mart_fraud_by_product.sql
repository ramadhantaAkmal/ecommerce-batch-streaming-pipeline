{{ config(
    materialized = 'table',
    partition_by = {
        "field": "product_id",
        "data_type": "integer"
    },
    cluster_by = ["category", "product_id"],
    alias = 'mart_fraud_by_product'
) }}

WITH fact AS (
    SELECT
        f.order_id,
        f.product_id,
        f.amount_numeric,
        f.quantity,
        f.status,                  
        f.fraud_reasons,
        p.product_name,
        p.category
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }} f
    LEFT JOIN {{ source('akmal_ecommerce_silver_finpro','dim_products') }} p   
        ON f.product_id = p.product_id
)

SELECT
    product_id,
    product_name,
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN status = 'genuine' THEN order_id END) AS genuine_orders,
    COUNT(DISTINCT CASE WHEN status = 'frauds'   THEN order_id END) AS fraud_orders,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN status = 'frauds' THEN order_id END),
        COUNT(DISTINCT order_id)
    ) AS fraud_rate,
    SUM(CASE WHEN status = 'frauds' THEN amount_numeric ELSE 0 END) AS blocked_fraud_amount

FROM fact
GROUP BY ALL
HAVING fraud_orders > 0