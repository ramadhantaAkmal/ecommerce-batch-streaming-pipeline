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
    -- Payment dimensions
    f.payment_method,

    -- Core counts
    COUNT(DISTINCT f.order_id) AS total_orders,

    COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END) AS genuine_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'FRAUD'   THEN f.order_id END) AS fraud_orders,

    -- Revenue & volume
    SUM(f.amount_numeric) AS gross_gmv,
    SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END) AS net_revenue,

    -- Key ratios
    SAFE_DIVIDE(
        SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END),
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END)
    ) AS aov_genuine,

    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'FRAUD' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS fraud_rate,

    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS genuine_rate

FROM fact f
GROUP BY payment_method