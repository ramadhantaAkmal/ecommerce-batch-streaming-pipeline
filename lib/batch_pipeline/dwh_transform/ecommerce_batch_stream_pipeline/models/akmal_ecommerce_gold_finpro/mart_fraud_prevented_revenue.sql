{{ config(
    materialized = 'table',
    partition_by = {
        "field": "order_month",
        "data_type": "date"
    },
    cluster_by = ["year", "month"],
    alias = 'mart_fraud_prevented_revenue'
) }}

WITH fraud_orders AS (
    SELECT
        f.order_id,
        f.amount_numeric,
        f.status,
        d.date      AS order_date,
        d.year,
        d.month,
        DATE_TRUNC(d.date, MONTH) AS order_month,
        f.country_code,
        f.payment_method
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }} f
    LEFT JOIN {{ source('akmal_ecommerce_silver_finpro','dim_date') }} d ON f.date_id = d.date_id
    WHERE f.status = 'frauds'
)

SELECT
    -- Time
    order_month,
    year,
    month,
    FORMAT_DATE('%Y-%m', order_month) AS year_month,

    -- Fraud prevention KPIs
    COUNT(DISTINCT order_id)                                          AS blocked_fraud_orders,
    
    SUM(amount_numeric)                                               AS total_saved_amount,
    AVG(amount_numeric)                                               AS avg_saved_per_fraud_order,
    ROUND(SUM(amount_numeric), 0)                                     AS monthly_fraud_prevented_revenue,

    -- COUNT(DISTINCT CASE WHEN amount_numeric > 5000000 THEN order_id END) 
    --     AS high_value_fraud_blocked,
    -- SUM(CASE WHEN country_code NOT IN ('ID') THEN amount_numeric ELSE 0 END)
    --     AS saved_from_international_fraud

FROM fraud_orders
GROUP BY ALL