{{ config(
    materialized = 'table',
    alias = 'mart_payment_method_analysis'
) }}

WITH fact AS (
    SELECT
        f.date_id,
        f.order_id,
        f.amount_numeric,
        f.status,
        f.payment_method,
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }} f
)
SELECT
    f.payment_method,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END) AS genuine_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'frauds'   THEN f.order_id END) AS fraud_orders,
    SUM(f.amount_numeric) AS gross_revenue,
    SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END) AS net_revenue,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'frauds' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS fraud_rate,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS genuine_rate

FROM fact f
GROUP BY payment_method